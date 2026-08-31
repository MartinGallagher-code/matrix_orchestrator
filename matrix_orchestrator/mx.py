#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
"""mx -- run a request/response traffic matrix across a fleet of servers.

One file. One command per thing you want to do. Every host talks to every
other host in a request/response pattern: send a request of TX bytes, the
peer answers with a reply of RX bytes. Packets per second is the headline
number; Gbps comes along with it.

    mx gen --servers servers.txt --pps 20000   # 1. build matrix.csv
    mx start                                   # 2. deploy + run everywhere
    mx status                                  # 3. is it running?
    mx summarize                               # 4. pps / Gbps / loss / rtt
    mx stop                                    # 5. stop the agents
    mx clean                                   # 6. remove every trace

`mx run --for 60` does steps 2-5 in one shot. `mx hints` turns "I want
X" into the command that gets you X.

Everything the traffic needs lives in matrix.csv -- the host list, the
per-pair packet rates, the packet sizes and the port -- so no other
command needs those flags again. Edit the file and re-run `mx start` to
change the shape of a run.

The matrix is a grid CSV. Header row and first column carry host tokens
of the form  name[=addr[:port]]  (a bare IP works too). Cells are target
packets/sec from the row host to the column host; empty or 0 means no
flow, `max` means send unpaced. Lines beginning with '#' are the run
config and comments.

Traffic is UDP, deliberately: "a request of X bytes gets a reply of Y
bytes" is a statement about packets, and only a datagram protocol keeps
that promise on the wire. For TCP throughput sweeps use iperf_orchestrator.

Python 3.6+, standard library only, on the orchestrator and on every host.
"""

import argparse
import csv
import json
import multiprocessing
import os
import random
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Empty

VERSION = "1.7.0"

# ---------------------------------------------------------------------------
# Wire format
# ---------------------------------------------------------------------------
#
# Fixed 32-byte header, then zero padding out to the requested size.
#   magic | kind | flags | src index | packet len | reply len | seq | t_send
# `src index` is the sender's row number in the matrix, so accounting
# never depends on names or on reverse DNS. `t_send` is the client's own
# monotonic clock in microseconds and is echoed back untouched, which is
# what makes the round-trip time exact without any clock sync.
MAGIC = b"MXO1"
HDR = struct.Struct("!4sBBHIIQQ")
HDR_SIZE = HDR.size                 # 32
MAX_SIZE = 65507                    # largest UDP payload over IPv4
KIND_REQ, KIND_REP = 0, 1

# Per-packet overhead on the wire for UDP over Ethernet: preamble+SFD 8,
# MAC header 14, FCS 4, inter-frame gap 12, IPv4 20, UDP 8.
WIRE_OVERHEAD = 66

DEFAULT_PORT = 5300
DEFAULT_MATRIX = "matrix.csv"
DEFAULT_SERVERS = "servers.txt"
DEFAULT_REMOTE_DIR = "/var/tmp/mx"
DEFAULT_INTERVAL = 5.0

# `--workers auto` fans out one process per core, but stops here:
# a 64-core box forking 64 agents for a trickle of traffic helps
# nobody, and `--workers N` is there when it does.
MAX_AUTO_WORKERS = 8

# What one worker process sustains, in packets/sec of request+reply,
# measured on an ordinary x86 core. Used only to size advice, never to
# limit anything -- the real number is in every summary's `agent` column.
WORKER_PPS = 200000

# Sockets per peer. The ceiling is a sanity check, not a tuned limit:
# every stream is a socket on both ends, so N hosts x N peers x streams
# is what the kernel has to hold.
MAX_STREAMS = 256

REPORT_NAME = "report.csv"
LOG_NAME = "agent.log"
PID_NAME = "agent.pid"
MATRIX_NAME = "matrix.csv"          # name the matrix always takes on a host

ZEROS = b"\0" * MAX_SIZE

CONFIG_KEYS = ("tx_size", "rx_size", "port", "peers", "seed", "layers",
               "dwell", "fill")

_PADS = {}


def pad_for(size):
    """Zero padding that turns a header into a `size`-byte packet. Cached,
    because slicing a 64 KB buffer per packet would dominate the send loop."""
    pad = _PADS.get(size)
    if pad is None:
        pad = _PADS[size] = ZEROS[:size - HDR_SIZE]
    return pad


def _env(name, default):
    v = os.environ.get(name)
    return v if v else default


# ---------------------------------------------------------------------------
# Matrix
# ---------------------------------------------------------------------------

class Layering(object):
    """The rotation schedule of a layered matrix (`mx gen --peers K --dwell T`).

    The grid in the file is layer 0; the other layers exist only as this
    header, because every agent derives them from (hosts, peers, seed) --
    which is why a layered matrix deploys as one file however many layers
    it rotates through.
    """

    def __init__(self, peers, layers, dwell, seed, fill=False):
        self.peers = peers      # flows per host per layer (K)
        self.layers = layers    # ceil((N-1)/K), never chosen by hand
        self.dwell = dwell      # seconds each layer is held
        self.seed = seed
        # --equal-layers: the remainder layer is padded with repeats so
        # every layer carries exactly K -- "exactly once" per cycle
        # softens to "at least once" for the padded shifts.
        self.fill = fill

    def cycle(self):
        """Seconds for one full rotation -- every ordered pair once."""
        return self.layers * self.dwell


class Matrix(object):
    """Host list, endpoints, per-pair target rates and the run config."""

    def __init__(self, path, hosts, addrs, ports, rates, tx_size, rx_size, port,
                 layering=None):
        self.path = path
        self.hosts = hosts                  # ordered names; index == matrix row
        self.addrs = addrs                  # name -> address
        self.ports = ports                  # name -> port
        self.rates = rates                  # (src, dst) -> pps (inf == unpaced)
        self.tx_size = tx_size
        self.rx_size = rx_size
        self.port = port
        self.layering = layering            # None, or the rotation schedule
        self.index = {h: i for i, h in enumerate(hosts)}

    def peers_of(self, host):
        """[(peer, pps)] for every flow this host sends, in matrix order."""
        return [(d, self.rates[(host, d)]) for d in self.hosts
                if (host, d) in self.rates]

    def endpoint(self, host):
        return self.addrs[host], self.ports[host]


def parse_token(tok):
    """'name[=addr[:port]]' -> (name, addr, port_or_None).

    A bare address is its own name, so a plain list of IPs just works.
    """
    tok = tok.strip()
    bare = "=" not in tok
    if bare:
        name = addr = tok
    else:
        name, addr = tok.split("=", 1)
        name, addr = name.strip(), addr.strip()
    port = None
    if ":" in addr:
        addr, p = addr.rsplit(":", 1)
        try:
            port = int(p)
        except ValueError:
            die("bad port in host token %r" % tok)
        if bare:
            name = addr
    if not name or not addr:
        die("bad host token %r (want name[=addr[:port]])" % tok)
    return name, addr, port


def parse_pps(cell, where):
    """A matrix cell: '' / '0' -> no flow, 'max' -> unpaced, else pps."""
    cell = cell.strip().lower()
    if not cell:
        return 0.0
    if cell in ("max", "inf", "unpaced"):
        return float("inf")
    try:
        v = float(cell)
    except ValueError:
        die("%s: %r is not a packet rate (a number, empty, or 'max')" % (where, cell))
    if v < 0:
        die("%s: negative packet rate %r" % (where, cell))
    return v


def check_size(name, value):
    if value < HDR_SIZE or value > MAX_SIZE:
        die("%s must be between %d and %d bytes (got %d)"
            % (name, HDR_SIZE, MAX_SIZE, value))
    return value


def load_matrix(path):
    """Read a matrix CSV plus the '# key=value' run config above it."""
    if not os.path.isfile(path):
        die("matrix not found: %s  (make one with: mx gen --servers %s --pps 10000)"
            % (path, DEFAULT_SERVERS))
    cfg = {}
    data_lines = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                for tok in stripped.lstrip("#").split():
                    if "=" in tok:
                        k, v = tok.split("=", 1)
                        if k in CONFIG_KEYS:
                            cfg[k] = v
                continue
            data_lines.append(line)

    rows = [r for r in csv.reader(data_lines) if r and any(c.strip() for c in r)]
    if len(rows) < 2:
        die("%s: need a header row and at least one host row" % path)

    def _int(key, default):
        try:
            return int(cfg.get(key, default))
        except ValueError:
            die("%s: bad %s=%r in the config header" % (path, key, cfg[key]))

    port = _int("port", DEFAULT_PORT)
    tx_size = check_size("tx_size", _int("tx_size", 64))
    rx_size = check_size("rx_size", _int("rx_size", 64))

    hosts, addrs, ports = [], {}, {}
    for tok in rows[0][1:]:
        if not tok.strip():
            continue
        name, addr, p = parse_token(tok)
        if name in addrs:
            die("%s: duplicate host %r in the header" % (path, name))
        hosts.append(name)
        addrs[name] = addr
        ports[name] = p if p is not None else port
    if len(hosts) < 2:
        die("%s: need at least 2 hosts" % path)

    rates = {}
    for row in rows[1:]:
        src, _addr, _p = parse_token(row[0])
        if src not in addrs:
            die("%s: row host %r is not in the header row" % (path, src))
        for i, cell in enumerate(row[1:len(hosts) + 1]):
            dst = hosts[i]
            pps = parse_pps(cell, "%s: cell %s -> %s" % (path, src, dst))
            if pps > 0 and src != dst:
                rates[(src, dst)] = pps
    if not rates:
        die("%s: every cell is empty -- no flows to run" % path)

    layering = None
    if _int("layers", 1) > 1:
        n = len(hosts)
        layers = _int("layers", 1)
        peers = _int("peers", 0)
        seed = _int("seed", -1)
        try:
            dwell = float(cfg.get("dwell", 0))
        except ValueError:
            die("%s: bad dwell=%r in the config header" % (path, cfg["dwell"]))
        if not 1 <= peers <= n - 1 or seed < 0 or dwell <= 0:
            die("%s: layers=%d needs peers, seed and dwell in the header -- "
                "regenerate with `mx gen --peers K --dwell T`" % (path, layers))
        # The hard guarantee lives here: the layer count is a function of
        # (hosts, peers), never a choice, so a cycle always covers every
        # ordered pair exactly once. A header that disagrees was edited.
        want = -(-(n - 1) // peers)
        if layers != want:
            die("%s: layers=%d but %d hosts at %d peers per layer partition "
                "into %d layers -- the header was edited; regenerate with "
                "`mx gen`" % (path, layers, n, peers, want))
        layering = Layering(peers, layers, dwell, seed,
                            fill=bool(_int("fill", 0)))
    return Matrix(path, hosts, addrs, ports, rates, tx_size, rx_size, port,
                  layering)


def write_matrix(path, tokens, cell_for, tx_size, rx_size, port,
                 extra_comment=""):
    """Write a matrix CSV with the run config in its header comment."""
    names = [parse_token(t)[0] for t in tokens]
    out = sys.stdout if path == "-" else open(path, "w", newline="")
    try:
        out.write("# mx matrix v%s -- rows send, columns receive, cells are packets/sec\n"
                  % VERSION.split(".")[0])
        out.write("# tx_size=%d rx_size=%d port=%d\n" % (tx_size, rx_size, port))
        out.write("# every request of %d bytes is answered with a reply of %d bytes\n"
                  % (tx_size, rx_size))
        if extra_comment:
            out.write(extra_comment)
        w = csv.writer(out)
        w.writerow(["src\\dst"] + tokens)
        for si, stok in enumerate(tokens):
            w.writerow([stok] + [cell_for(si, di) for di in range(len(names))])
    finally:
        if out is not sys.stdout:
            out.close()


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def die(msg, code=2):
    """Exit with a usage-style status (2), as argparse does -- so a bad
    matrix is never mistaken for a run that merely found problems (1)."""
    sys.stderr.write("mx: %s\n" % msg)
    sys.stderr.flush()
    raise SystemExit(code)


def log(msg):
    sys.stdout.write("%s\n" % msg)
    sys.stdout.flush()


def fmt_pps(pps):
    if pps == float("inf"):
        return "max"
    if pps >= 1e6:
        return "%.3f Mpps" % (pps / 1e6)
    if pps >= 1e3:
        return "%.1f kpps" % (pps / 1e3)
    return "%.0f pps" % pps


def fmt_gbps(bps):
    if bps >= 1e9:
        return "%.2f Gbps" % (bps / 1e9)
    if bps >= 1e6:
        return "%.1f Mbps" % (bps / 1e6)
    return "%.0f kbps" % (bps / 1e3)


def fmt_secs(s):
    """'90' -> '1m30s', '7500' -> '2h05m': durations for humans."""
    s = int(round(s))
    if s < 60:
        return "%ds" % s
    if s < 3600:
        return "%dm%02ds" % (s // 60, s % 60)
    return "%dh%02dm" % (s // 3600, s % 3600 // 60)


def fmt_us(us):
    if us is None:
        return "-"
    if us >= 1e6:
        return "%.2fs" % (us / 1e6)
    if us >= 1000:
        return "%.1fms" % (us / 1000.0)
    return "%.0fus" % us


def pct(part, whole):
    return (part / whole * 100.0) if whole else 0.0


def wire_bps(pps, size):
    """Bits/sec on the wire, framing included -- what a NIC actually sees."""
    return pps * (size + WIRE_OVERHEAD) * 8.0


def payload_bps(pps, size):
    return pps * size * 8.0


def check_bind_matches_matrix(matrix, me, bind_ip, iface):
    """--bind pins this agent's listeners to one address; the matrix says
    where the peers will send. If those disagree, every request arrives
    at an address nobody is listening on, and the run reports 100% loss
    that has nothing to do with the network -- so refuse to start.

    `mx start --bind` avoids this by resolving the bind pattern on every
    host and retargeting the deployed matrix at those addresses (the same
    scheme iperf-orchestrator uses for its conn_ip); this check is the
    safety net for hand-run agents.
    """
    addr = matrix.addrs[me]
    try:
        resolved = socket.getaddrinfo(addr, None, socket.AF_INET,
                                      socket.SOCK_DGRAM)[0][4][0]
    except OSError:
        return       # can't resolve here; let the run speak for itself
    if resolved != bind_ip:
        die("--bind resolves to %s (%s), but the matrix tells peers to "
            "reach %r at %s. Requests would arrive at an address this "
            "agent is not listening on: 100%% loss, guaranteed, before "
            "the network is even involved. Either start via `mx start "
            "--bind ...` (which rewrites the deployed matrix to each "
            "host's %s address), rebuild servers.txt from the %s-side "
            "addresses, or drop --bind." % (bind_ip, iface, me, resolved,
                                            iface, iface))


def resolve_bind(spec):
    """--bind SPEC, iperf-orchestrator semantics: substring-match the spec
    against `ip -o -4 addr show`, so an interface name or an address both
    work. Returns (iface, ip)."""
    try:
        out = subprocess.check_output(["ip", "-o", "-4", "addr", "show"],
                                      stderr=subprocess.DEVNULL).decode()
    except (OSError, subprocess.CalledProcessError):
        die("--bind %r: `ip -o -4 addr show` failed; is iproute2 installed?" % spec)
    for line in out.splitlines():
        if spec in line:
            parts = line.split()
            if len(parts) >= 4:
                return parts[1], parts[3].split("/")[0]
    die("--bind %r: no interface matched" % spec)


# ---------------------------------------------------------------------------
# Round-trip time histogram
#
# Quarter-octave buckets from 1us: constant memory, no sorting, and
# percentiles land within ~20% of the true value -- plenty to tell 200us
# from 8ms, which is the question latency is being asked here.
# ---------------------------------------------------------------------------

RTT_OCTAVES = 32
RTT_NBUCKETS = RTT_OCTAVES * 4


def rtt_bucket(us):
    if us < 4:
        return max(0, int(us))
    octave = us.bit_length() - 1
    frac = (us >> (octave - 2)) & 3
    return min(RTT_NBUCKETS - 1, octave * 4 + frac)


def rtt_bucket_us(i):
    """Upper edge of bucket i, in microseconds."""
    octave, frac = divmod(i, 4)
    return (1 << octave) * (1.0 + (frac + 1) / 4.0)


def rtt_percentile(hist, q):
    total = sum(hist)
    if not total:
        return None
    want = total * q
    seen = 0
    for i, n in enumerate(hist):
        seen += n
        if seen >= want:
            return rtt_bucket_us(i)
    return rtt_bucket_us(len(hist) - 1)


# ---------------------------------------------------------------------------
# CPU sampling (Linux; silently unavailable elsewhere)
# ---------------------------------------------------------------------------

def read_cpu():
    """[(total, busy)] jiffies: index 0 is the whole box, then per core."""
    try:
        with open("/proc/stat") as f:
            lines = f.readlines()
    except OSError:
        return None
    out = []
    for line in lines:
        if not line.startswith("cpu"):
            break
        parts = line.split()
        vals = [int(v) for v in parts[1:11]]
        total = sum(vals)
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
        out.append((total, total - idle))
    return out or None


def cpu_delta(prev, cur):
    """(overall busy %, busiest single core %) between two read_cpu()s."""
    if not prev or not cur:
        return None, None
    def _pct(a, b):
        dt = b[0] - a[0]
        return (b[1] - a[1]) * 100.0 / dt if dt > 0 else 0.0
    overall = _pct(prev[0], cur[0])
    cores = [_pct(a, b) for a, b in zip(prev[1:], cur[1:])]
    return overall, (max(cores) if cores else overall)


try:
    _CLK_TCK = os.sysconf("SC_CLK_TCK") or 100
except (ValueError, OSError, AttributeError):
    _CLK_TCK = 100


def read_self_cpu():
    """This agent's own CPU time in seconds (user + system).

    The box-wide number does not answer the question that matters here:
    one agent is one Python process, so the GIL caps it near 100% of a
    single core no matter how many cores the box has. When this reads
    ~100% the agent is the bottleneck, not the fabric.
    """
    try:
        with open("/proc/self/stat") as f:
            fields = f.read().rsplit(") ", 1)[-1].split()
        # After the comm field: state is [0], so utime/stime are [11]/[12].
        return (int(fields[11]) + int(fields[12])) / float(_CLK_TCK)
    except (OSError, IndexError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Agent: worker processes
#
# Packet rate here is CPU work, and CPU work in Python means one process
# is one GIL. Threads do not merely fail to help -- measured on a 4-core
# box, the same send loop runs at 300k pps in one thread, 199k across two
# threads, and 57k across four, because the threads convoy on the GIL.
# The same work in separate processes scales nearly linearly: 1.09M pps
# across four.
#
# So the agent is P worker *processes*, and each worker is a single
# thread driving one event loop over all of its sockets. No locks, no
# thread hand-offs, nothing shared in the packet path. Workers split the
# send flows between them and each opens its own SO_REUSEPORT responder
# socket, so the kernel spreads inbound requests across them too; a peer's
# packets always hash to one worker, which keeps the per-peer counters
# consistent. Every interval each worker ships its finished numbers to
# the parent, which is the only process that writes the report.
# ---------------------------------------------------------------------------

MAX_DRAIN = 1024        # packets read from one socket per poll
MAX_SEND_BATCH = 512    # packets sent for one flow per poll


class Flow(object):
    """One client flow: this host -> one peer, request out, reply back.

    Owned start to finish by a single worker process, so the counters are
    plain integers with no synchronization anywhere.
    """

    def __init__(self, peer, addr, port, target_pps, stream=0, layer=None):
        self.peer = peer
        self.stream = stream
        self.layer = layer      # which layer this flow belongs to, if any
        # A drained flow stops sending but stays open one more interval so
        # replies still in flight are counted, not booked as loss against
        # the layer that just ended.
        self.draining = False
        # Counters are kept per stream but reported per peer, so the key
        # has to carry the stream and the label must not. It carries the
        # layer too: --equal-layers lets the padded layer share a pair
        # with layer 0, so at the cycle wrap a draining flow and a new
        # active flow to the SAME peer overlap for one interval -- with
        # one key they would read each other's delta baselines.
        self.key = "%s#%d@%s" % (peer, stream,
                                 "-" if layer is None else layer)
        self.addr = addr
        self.port = port
        self.target_pps = target_pps
        self.paced = target_pps != float("inf")
        # The bucket holds 5ms of traffic: enough to amortize the poll
        # cadence, too little for a stall to earn a catch-up burst.
        self.burst_cap = max(2.0, target_pps * 0.005) if self.paced else 0.0
        self.tokens = 0.0
        self.last = 0.0
        self.seq = 0
        self.sock = None
        self.dest = None
        self.tx_pkts = 0
        self.tx_bytes = 0
        self.rx_pkts = 0
        self.rx_bytes = 0
        self.errors = 0
        self.rtt_sum = 0
        self.rtt_n = 0
        self.rtt_max = 0
        self.rtt_hist = [0] * RTT_NBUCKETS

    def open(self, sndbuf, bind_ip):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if sndbuf:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
            except OSError:
                pass
        if bind_ip:
            sock.bind((bind_ip, 0))
        # Resolve once: a hostname in sendto() would mean a resolver call
        # per packet. Deliberately not connected -- a multi-homed peer
        # answers from whichever local address its route picks, and a
        # connected socket would discard those replies as coming from the
        # wrong peer. This socket carries one flow, so accepting whatever
        # arrives on it is right.
        self.dest = socket.getaddrinfo(self.addr, self.port, socket.AF_INET,
                                       socket.SOCK_DGRAM)[0][4]
        sock.setblocking(False)
        self.sock = sock
        return sock


class WorkerStats(object):
    """What one worker hands the parent at the end of an interval."""

    def __init__(self, wid, ts, flows, srv, cpu_pct):
        self.wid = wid
        self.ts = ts
        self.flows = flows      # list of per-flow dicts
        self.srv = srv          # list of per-peer server dicts
        self.cpu_pct = cpu_pct  # this worker's CPU, as a share of one core


class WorkerView(object):
    """The slice of the matrix a worker needs. The full Matrix carries the
    whole rates dict -- ~N^2 cells, tens of MB at 1000 hosts -- and every
    worker would inherit a pickled copy of it. This is the part they read.
    """

    def __init__(self, matrix, me):
        self.hosts = matrix.hosts
        self.my_index = matrix.index[me]
        self.my_port = matrix.ports[me]
        self.tx_size = matrix.tx_size
        self.rx_size = matrix.rx_size


def _worker(wid, view, me, cfg, flow_specs, stop, queue):
    """One worker process: send its share of the flows, answer requests
    on its own listening socket, and report every interval."""
    try:
        _worker_loop(wid, view, me, cfg, flow_specs, stop, queue)
    except KeyboardInterrupt:
        pass
    except Exception as exc:                      # noqa: BLE001
        sys.stderr.write("mx agent: worker %d died: %s\n" % (wid, exc))
        sys.stderr.flush()


def _worker_loop(wid, view, me, cfg, flow_specs, stop, queue):
    import selectors

    my_index = view.my_index
    tx_size, rx_size = view.tx_size, view.rx_size
    pad = pad_for(tx_size)
    pack, unpack = HDR.pack, HDR.unpack_from
    monotonic = time.monotonic

    sel = selectors.DefaultSelector()

    def build_flows(specs, layer):
        built = []
        for peer, addr, port, pps, stream in specs:
            fl = Flow(peer, addr, port, pps, stream, layer)
            try:
                sel.register(fl.open(cfg["sndbuf"], cfg["bind_ip"]),
                             selectors.EVENT_READ, fl)
            except OSError as exc:
                sys.stderr.write("mx agent: worker %d cannot reach %s (%s:%d): %s\n"
                                 % (wid, peer, addr, port, exc))
                sys.stderr.flush()
                continue
            fl.last = monotonic()
            built.append(fl)
        return built

    # Layered runs switch on the wall clock, each host on its own: the
    # responder answers whatever arrives whoever sends it, so no host
    # needs to agree with any other about when to switch -- skew only
    # costs a blurred boundary interval. The small bias absorbs a tick
    # that fires a few ms before the boundary it was aligned to.
    nlayers = cfg.get("nlayers", 1)
    dwell = cfg.get("dwell", 0.0)
    layered = nlayers > 1

    def wall_layer():
        return int((time.time() + 0.01) // dwell) % nlayers

    if layered:
        layer = wall_layer()
        flows = build_flows(flow_specs[layer], layer)
    else:
        layer = None
        flows = build_flows(flow_specs, None)
    draining = []      # (close_after, [flows]) -- oldest first

    # Responder. Every worker binds the same port with SO_REUSEPORT so the
    # kernel fans inbound requests across them.
    srv_counts = [[0, 0, 0, 0] for _ in range(len(view.hosts) + 1)]
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    reuseport = getattr(socket, "SO_REUSEPORT", None)
    if reuseport is not None:
        try:
            srv.setsockopt(socket.SOL_SOCKET, reuseport, 1)
        except OSError:
            pass
    try:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                       cfg["rcvbuf"] or 8 * 1024 * 1024)
    except OSError:
        pass
    try:
        srv.bind((cfg["bind_ip"], view.my_port))
        srv.setblocking(False)
        sel.register(srv, selectors.EVENT_READ, None)
    except OSError as exc:
        # Without SO_REUSEPORT only the first worker gets the port; the
        # rest stay senders, which still works, just with one responder.
        sys.stderr.write("mx agent: worker %d not listening on %s:%d (%s)\n"
                         % (wid, cfg["bind_ip"] or "0.0.0.0", view.my_port, exc))
        sys.stderr.flush()
        srv.close()
        srv = None

    pads = {}
    prev = {}
    prev_cpu = read_self_cpu()
    interval = cfg["interval"]

    def next_tick(now):
        """When to report next. Layered runs align ticks to wall-clock
        multiples of the interval: the dwell is a multiple of the interval,
        so every layer switch lands exactly on a tick and no report row
        ever straddles two layers. Ticks under half an interval away are
        skipped, so a report never covers a sliver."""
        if not layered:
            return now + interval
        wait = interval - (time.time() % interval)
        if wait < interval * 0.5:
            wait += interval
        return now + wait

    last_report = monotonic()
    next_report = next_tick(last_report)
    deadline = last_report + cfg["duration"] if cfg["duration"] > 0 else None
    parent = os.getppid()

    while not stop.is_set():
        now = monotonic()

        # ---- send whatever the token buckets allow ----
        next_due = now + 0.05
        for fl in flows:
            if fl.paced:
                fl.tokens = min(fl.tokens + (now - fl.last) * fl.target_pps,
                                fl.burst_cap)
                fl.last = now
                if fl.tokens < 1.0:
                    due = now + (1.0 - fl.tokens) / fl.target_pps
                    if due < next_due:
                        next_due = due
                    continue
                batch = int(fl.tokens)
                if batch > MAX_SEND_BATCH:
                    batch = MAX_SEND_BATCH
                fl.tokens -= batch
                next_due = now
            else:
                batch = MAX_SEND_BATCH
                next_due = now
            sendto, dest = fl.sock.sendto, fl.dest
            seq, sent, sent_b = fl.seq, 0, 0
            for _ in range(batch):
                seq += 1
                try:
                    sendto(pack(MAGIC, KIND_REQ, 0, my_index, tx_size, rx_size,
                                seq, int(monotonic() * 1e6)) + pad, dest)
                except OSError:
                    fl.errors += 1
                    break
                sent += 1
                sent_b += tx_size
            fl.seq = seq
            fl.tx_pkts += sent
            fl.tx_bytes += sent_b

        # ---- wait for replies and requests ----
        # select returns the moment anything is readable, so a reply is
        # timed the instant it lands; the timeout only bounds an idle wait.
        timeout = next_due - monotonic()
        if timeout < 0:
            timeout = 0
        elif timeout > 0.05:
            timeout = 0.05
        try:
            events = sel.select(timeout)
        except OSError:
            events = []

        for key, _mask in events:
            fl = key.data
            if fl is None:
                # Responder socket: answer every request with a reply of
                # the size the request asked for.
                recvfrom, sendto = key.fileobj.recvfrom, key.fileobj.sendto
                for _ in range(MAX_DRAIN):
                    try:
                        data, peer = recvfrom(MAX_SIZE)
                    except OSError:
                        break
                    if len(data) < HDR_SIZE:
                        continue
                    magic, kind, _f, src, _pl, rsize, sq, ts = unpack(data)
                    if magic != MAGIC or kind != KIND_REQ:
                        continue
                    row = srv_counts[src] if src < len(srv_counts) - 1 \
                        else srv_counts[-1]
                    row[0] += 1
                    row[1] += len(data)
                    if rsize < HDR_SIZE:
                        rsize = HDR_SIZE
                    elif rsize > MAX_SIZE:
                        rsize = MAX_SIZE
                    rpad = pads.get(rsize)
                    if rpad is None:
                        rpad = pads[rsize] = pad_for(rsize)
                    # t_send is echoed untouched, so the client measures
                    # its own round trip with no clock sync between us.
                    try:
                        sendto(pack(MAGIC, KIND_REP, 0, my_index, rsize, 0,
                                    sq, ts) + rpad, peer)
                    except OSError:
                        continue
                    row[2] += 1
                    row[3] += rsize
            else:
                recv = fl.sock.recv
                hist = fl.rtt_hist
                for _ in range(MAX_DRAIN):
                    try:
                        data = recv(MAX_SIZE)
                    except OSError:
                        break
                    if len(data) < HDR_SIZE:
                        continue
                    magic, kind, _f, _i, _pl, _rs, _sq, ts = unpack(data)
                    if magic != MAGIC or kind != KIND_REP:
                        continue
                    fl.rx_pkts += 1
                    fl.rx_bytes += len(data)
                    rtt = int(monotonic() * 1e6) - ts
                    if rtt >= 0:
                        fl.rtt_sum += rtt
                        fl.rtt_n += 1
                        if rtt > fl.rtt_max:
                            fl.rtt_max = rtt
                        hist[rtt_bucket(rtt)] += 1

        # ---- report ----
        now = monotonic()
        if now >= next_report:
            cur_cpu = read_self_cpu()
            elapsed = max(now - last_report, 1e-3)
            cpu_pct = None
            if cur_cpu is not None and prev_cpu is not None:
                cpu_pct = (cur_cpu - prev_cpu) / elapsed * 100.0
            prev_cpu = cur_cpu
            # Draining flows are collected too: their late replies belong
            # to the layer that just ended, and this is where they land.
            drains = [fl for _dl, fls in draining for fl in fls]
            stats = _collect(wid, flows + drains, srv_counts, view, me, prev,
                             elapsed, cpu_pct)
            try:
                queue.put(stats, block=False)
            except Exception:                     # noqa: BLE001 - full queue
                pass
            last_report = now
            next_report = next_tick(now)

            # Only after their tail was collected may drained flows close;
            # forgetting their deltas is what lets the same pair start
            # from zero when its layer comes around next cycle.
            while draining and draining[0][0] <= now:
                for fl in draining.pop(0)[1]:
                    try:
                        sel.unregister(fl.sock)
                    except (KeyError, ValueError):
                        pass
                    try:
                        fl.sock.close()
                    except OSError:
                        pass
                    _forget_flow(prev, fl.key)

            if layered:
                lj = wall_layer()
                if lj != layer:
                    for fl in flows:
                        fl.draining = True
                    if flows:
                        draining.append((now + interval * 0.5, flows))
                    flows = build_flows(flow_specs[lj], lj)
                    layer = lj

        if deadline is not None and now >= deadline:
            break
        # If the parent is gone, so is the reason to keep sending. This is
        # what keeps a SIGKILLed run from leaving traffic on the network.
        if os.getppid() != parent:
            break

    for fl in flows + [fl for _dl, fls in draining for fl in fls]:
        try:
            fl.sock.close()
        except OSError:
            pass
    if srv is not None:
        try:
            srv.close()
        except OSError:
            pass
    sel.close()


def _forget_flow(prev, key):
    """Drop a closed flow's delta baselines. When its pair comes around
    again next cycle the new flow's counters start at zero, and a stale
    baseline would turn that into a huge negative delta."""
    for k in ("tx", "txb", "rx", "rxb", "rs", "rn"):
        prev.pop((k, key), None)
    for i in range(RTT_NBUCKETS):
        prev.pop(("h", key, i), None)


def _collect(wid, flows, srv_counts, view, me, prev, elapsed, cpu_pct):
    """Turn this worker's raw counters into one interval's finished rates."""
    def delta(key, value):
        was = prev.get(key, 0)
        prev[key] = value
        return value - was

    frows = []
    for fl in flows:
        # Keyed per stream: two streams to the same peer are two sets of
        # counters, and sharing a key would make each delta the difference
        # between the other's total.
        d_tx = delta(("tx", fl.key), fl.tx_pkts)
        d_txb = delta(("txb", fl.key), fl.tx_bytes)
        d_rx = delta(("rx", fl.key), fl.rx_pkts)
        d_rxb = delta(("rxb", fl.key), fl.rx_bytes)
        d_sum = delta(("rs", fl.key), fl.rtt_sum)
        d_n = delta(("rn", fl.key), fl.rtt_n)
        hist = []
        for i, n in enumerate(fl.rtt_hist):
            hist.append(n - prev.get(("h", fl.key, i), 0))
            prev[("h", fl.key, i)] = n
        # A drained flow that caught no more replies has nothing left to
        # say; a row of zeroes would read as a flow that stalled.
        if fl.draining and not (d_tx or d_rx or d_n):
            continue
        frows.append({
            "peer": fl.peer,
            "layer": fl.layer,
            "drain": fl.draining,
            # A draining flow offers nothing, so it carries no target --
            # otherwise every switch interval would inflate the host's
            # target by a whole layer's worth.
            "target": None if not fl.paced or fl.draining else fl.target_pps,
            "pps": d_tx / elapsed,
            "mbps": d_txb * 8.0 / elapsed / 1e6,
            "rep_pps": d_rx / elapsed,
            "rep_mbps": d_rxb * 8.0 / elapsed / 1e6,
            "loss": (100.0 - pct(d_rx, d_tx)) if d_tx else None,
            "rtt_avg": (d_sum / d_n) if d_n else None,
            "rtt_p50": rtt_percentile(hist, 0.50) if d_n else None,
            "rtt_p99": rtt_percentile(hist, 0.99) if d_n else None,
            "rtt_max": fl.rtt_max,
            "hist": hist,
        })

    srows = []
    for i, peer in enumerate(view.hosts):
        if peer == me:
            continue
        row = srv_counts[i]
        d_req = delta(("sq", peer), row[0])
        d_reqb = delta(("sqb", peer), row[1])
        d_rep = delta(("sp", peer), row[2])
        d_repb = delta(("spb", peer), row[3])
        if not (d_req or d_rep):
            continue
        srows.append({
            "peer": peer,
            "pps": d_req / elapsed,
            "mbps": d_reqb * 8.0 / elapsed / 1e6,
            "rep_pps": d_rep / elapsed,
            "rep_mbps": d_repb * 8.0 / elapsed / 1e6,
        })
    return WorkerStats(wid, int(time.time()), frows, srows, cpu_pct)


# ---------------------------------------------------------------------------
# Agent: reporting (the parent process)
# ---------------------------------------------------------------------------

REPORT_FIELDS = ["ts", "host", "dir", "peer", "size", "rep_size", "target_pps",
                 "pps", "mbps", "rep_pps", "rep_mbps", "loss_pct",
                 "rtt_avg_us", "rtt_p50_us", "rtt_p99_us", "rtt_max_us",
                 "cpu_pct", "cpu_max_pct", "agent_cpu_pct", "workers", "layer"]


def _num(v, fmt="%.1f"):
    return "" if v is None else fmt % v


def _merge_flow(a, b):
    """Two workers' halves of one flow's interval. Rates add; the latency
    percentiles are recomputed from the combined histogram rather than
    averaged, which would be a percentile of percentiles."""
    hist = [x + y for x, y in zip(a["hist"], b["hist"])]
    n = sum(hist)
    pps = a["pps"] + b["pps"]
    rep_pps = a["rep_pps"] + b["rep_pps"]
    avgs = [(x["rtt_avg"], sum(x["hist"])) for x in (a, b)
            if x["rtt_avg"] is not None]
    weight = sum(w for _v, w in avgs)
    return {
        "peer": a["peer"],
        "layer": a.get("layer"),
        "drain": a.get("drain") or b.get("drain"),
        "target": None if a["target"] is None or b["target"] is None
                  else a["target"] + b["target"],
        "pps": pps,
        "mbps": a["mbps"] + b["mbps"],
        "rep_pps": rep_pps,
        "rep_mbps": a["rep_mbps"] + b["rep_mbps"],
        "loss": (100.0 - pct(rep_pps, pps)) if pps else None,
        "rtt_avg": (sum(v * w for v, w in avgs) / weight) if weight else None,
        "rtt_p50": rtt_percentile(hist, 0.50) if n else None,
        "rtt_p99": rtt_percentile(hist, 0.99) if n else None,
        "rtt_max": max(a["rtt_max"], b["rtt_max"]),
        "hist": hist,
    }


def _merge_srv(a, b):
    return {"peer": a["peer"],
            "pps": a["pps"] + b["pps"],
            "mbps": a["mbps"] + b["mbps"],
            "rep_pps": a["rep_pps"] + b["rep_pps"],
            "rep_mbps": a["rep_mbps"] + b["rep_mbps"]}


class Reporter(object):
    """The parent's side: drain what the workers report, write the CSV,
    and print the one line `mx status` shows."""

    def __init__(self, host, path, tx_size, rx_size, nworkers):
        self.host = host
        self.tx_size = tx_size
        self.rx_size = rx_size
        self.nworkers = nworkers
        fresh = not os.path.exists(path)
        self.fh = open(path, "a", newline="")
        self.csv = csv.writer(self.fh)
        if fresh:
            self.csv.writerow(REPORT_FIELDS)
            self.fh.flush()
        self.prev_cpu = read_cpu()
        self.last = time.monotonic()

    def tick(self, batches, complete=True):
        """Write one interval. `batches` is every WorkerStats collected
        since the last tick; `complete` says whether every live worker is
        represented.

        Rows are merged across workers before they are written, so the
        report keeps one row per peer per interval however many workers
        produced it. That merge is not cosmetic: SO_REUSEPORT rehashes
        when a worker's socket closes, so one peer's traffic can briefly
        land on two workers, and those two rows are two halves of one
        rate -- summed they are right, listed separately they read as a
        host that halved its throughput.
        """
        now = int(time.time())
        self.last = time.monotonic()
        cur_cpu = read_cpu()
        cpu, cpu_max = cpu_delta(self.prev_cpu, cur_cpu)
        self.prev_cpu = cur_cpu

        # Two batches from one worker are two overlapping measurements of
        # the same counters; only the newest can be counted.
        latest = {}
        for b in batches:
            if b.wid not in latest or b.ts >= latest[b.wid].ts:
                latest[b.wid] = b
        if not latest:
            return

        flows, srv, cpus = {}, {}, []
        for b in latest.values():
            if b.cpu_pct is not None:
                cpus.append(b.cpu_pct)
            for f in b.flows:
                # Keyed by (peer, layer, drain), not peer alone: at an
                # --equal-layers cycle wrap the old layer's drain and the
                # new layer's active flow are the SAME peer in the same
                # interval, and folding them into one row would blank the
                # active flow's traffic under the drain's flag. Two rows,
                # each under its own layer, is the correct accounting.
                fkey = (f["peer"], f.get("layer"), bool(f.get("drain")))
                cur = flows.get(fkey)
                flows[fkey] = f if cur is None else _merge_flow(cur, f)
            for s in b.srv:
                cur = srv.get(s["peer"])
                srv[s["peer"]] = s if cur is None else _merge_srv(cur, s)

        tx_pps = rx_pps = tx_bps = rx_bps = target = 0.0
        unpaced = 0
        cur_layer = None
        hist = [0] * RTT_NBUCKETS
        for fkey in sorted(flows, key=lambda t: (t[0], str(t[1]), t[2])):
            f = flows[fkey]
            peer = f["peer"]
            drain = f.get("drain")
            layer = f.get("layer")
            tx_pps += f["pps"]
            rx_pps += f["rep_pps"]
            tx_bps += f["mbps"] * 1e6
            rx_bps += f["rep_mbps"] * 1e6
            if not drain:
                if layer is not None:
                    cur_layer = layer
                if f["target"] is None:
                    unpaced += 1
                else:
                    target += f["target"]
            for i, n in enumerate(f["hist"]):
                hist[i] += n
            if drain:
                # The tail of a finished layer: replies (and their RTTs)
                # that landed after the switch. The send-side cells stay
                # blank -- a zero rate here is an artifact of the switch,
                # and averaged in it would halve every mean downstream.
                self.csv.writerow([
                    now, self.host, "tx", peer, self.tx_size, self.rx_size,
                    "", "", "", _num(f["rep_pps"]), _num(f["rep_mbps"], "%.3f"),
                    "", _num(f["rtt_avg"], "%.0f"), _num(f["rtt_p50"], "%.0f"),
                    _num(f["rtt_p99"], "%.0f"), _num(f["rtt_max"], "%.0f"),
                    "", "", "", "", _num(layer, "%d")])
                continue
            self.csv.writerow([
                now, self.host, "tx", peer, self.tx_size, self.rx_size,
                _num(f["target"]), _num(f["pps"]), _num(f["mbps"], "%.3f"),
                _num(f["rep_pps"]), _num(f["rep_mbps"], "%.3f"),
                _num(f["loss"], "%.3f"), _num(f["rtt_avg"], "%.0f"),
                _num(f["rtt_p50"], "%.0f"), _num(f["rtt_p99"], "%.0f"),
                _num(f["rtt_max"], "%.0f"), "", "", "", "",
                _num(layer, "%d")])

        srv_pps = 0.0
        for peer in sorted(srv):
            s = srv[peer]
            srv_pps += s["pps"]
            self.csv.writerow([
                now, self.host, "rx", peer, self.tx_size, self.rx_size,
                "", _num(s["pps"]), _num(s["mbps"], "%.3f"),
                _num(s["rep_pps"]), _num(s["rep_mbps"], "%.3f"),
                "", "", "", "", "", "", "", "", "", ""])

        # The busiest worker, not the average: one pegged worker is the
        # ceiling even when its siblings are idle, and averaging hides it.
        agent_cpu = max(cpus) if cpus else None

        # The host row and the status line both sum across workers, so
        # they only mean anything when every worker is in. A partial one
        # reads as a host that went quiet -- the per-peer rows above are
        # self-contained and stay either way. Say so in the log rather
        # than skipping silently: a host whose intervals are chronically
        # partial would otherwise never log at all, and `mx status`
        # (which tails this log) would show its startup banner forever.
        if not complete:
            self.fh.flush()
            log("ts=%d partial interval: %d of %d workers reported -- "
                "per-peer rows written, host totals skipped"
                % (now, len(latest), self.nworkers))
            return

        self.csv.writerow([
            now, self.host, "host", "*", self.tx_size, self.rx_size,
            "" if unpaced and not target else _num(target),
            _num(tx_pps), _num(tx_bps / 1e6, "%.3f"),
            _num(rx_pps), _num(rx_bps / 1e6, "%.3f"),
            _num(100.0 - pct(rx_pps, tx_pps), "%.3f") if tx_pps else "",
            "", _num(rtt_percentile(hist, 0.50), "%.0f"),
            _num(rtt_percentile(hist, 0.99), "%.0f"), "",
            _num(cpu), _num(cpu_max), _num(agent_cpu), self.nworkers,
            _num(cur_layer, "%d")])
        self.fh.flush()
        npeers = len(set(p for p, _l, drain in flows if not drain))

        log("ts=%d tx=%s/%s rx=%s serving=%s loss=%.2f%% rtt_p50=%s rtt_p99=%s "
            "cpu=%s workers=%d peers=%d"
            % (now, fmt_pps(tx_pps),
               "max" if unpaced and not target else fmt_pps(target),
               fmt_pps(rx_pps), fmt_pps(srv_pps),
               max(0.0, 100.0 - pct(rx_pps, tx_pps)) if tx_pps else 0.0,
               fmt_us(rtt_percentile(hist, 0.50)),
               fmt_us(rtt_percentile(hist, 0.99)),
               "%.0f%%(max %.0f%%)" % (cpu, cpu_max) if cpu is not None else "n/a",
               self.nworkers, npeers)
            + (" layer=%d" % cur_layer if cur_layer is not None else "")
            + (" busiest_worker=%.0f%% of a core" % agent_cpu
               if agent_cpu is not None else ""))


# ---------------------------------------------------------------------------
# Agent: the process that runs on every host
# ---------------------------------------------------------------------------

def resolve_self(matrix, override):
    """Which row of the matrix is this host? `mx start` always passes
    --host, so this only has to work for hand-run agents."""
    if override:
        if override not in matrix.index:
            die("--host %r is not in the matrix (hosts: %s)"
                % (override, ", ".join(matrix.hosts)))
        return override
    full = socket.gethostname()
    for cand in (full, full.split(".")[0]):
        if cand in matrix.index:
            return cand
    local = [h for h in matrix.hosts if _is_local_ipv4(matrix.addrs[h])]
    if len(local) == 1:
        return local[0]
    if len(local) > 1:
        die("several matrix hosts are local addresses (%s); pass --host"
            % ", ".join(local))
    die("this host (%s) is not in the matrix and none of its addresses are "
        "local; pass --host. Matrix hosts: %s" % (full, ", ".join(matrix.hosts)))


def _is_local_ipv4(addr):
    try:
        socket.inet_aton(addr)
    except OSError:
        return False
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind((addr, 0))
        return True
    except OSError:
        return False
    finally:
        s.close()


def resolve_workers(spec, nflows):
    """--workers N | auto. 'auto' is one per core, capped at MAX_AUTO_WORKERS
    so a big box does not fork 64 processes for a trickle of traffic."""
    cores = 1
    try:
        cores = multiprocessing.cpu_count()
    except NotImplementedError:
        pass
    if spec in (None, "", "auto"):
        n = min(max(1, cores), MAX_AUTO_WORKERS)
    else:
        try:
            n = int(spec)
        except ValueError:
            die("--workers wants a number or 'auto' (got %r)" % spec)
        if n < 1:
            die("--workers must be at least 1")
    # More workers than flows only adds responders, and even those cannot
    # help: inbound requests are spread by hashing the 4-tuple, so with F
    # flows arriving there are only F tuples to spread. Raise --streams to
    # raise this ceiling.
    n = max(1, min(n, max(1, nflows) + 1)) if nflows else n
    # Started, not refused -- somebody may be measuring the thrash on
    # purpose -- but said out loud: the classic mistake is one worker per
    # FLOW, and it costs a debugging session every time.
    if n > cores:
        log("workers: %d on a %d-core box. Each worker is a whole python "
            "process, and flows shard across however few there are, so "
            "workers beyond the cores add scheduling thrash, memory and "
            "partial intervals -- never packet rate. `--workers auto` "
            "sizes this sanely." % (n, cores))
    return n


def resolve_grace(interval, nworkers):
    """How long past a report tick the parent waits for straggling
    workers before writing the interval as partial.

    One second absorbs scheduler jitter, plus 10 ms per worker so a
    deliberately large-but-sane worker count gets a wider net. The hard
    ceiling is half the interval: a flush still open when the next
    round's reports arrive would trip the newest-batch-only rule and
    silently drop the older round's deltas -- a stale row is honest,
    lost data is not. Past this window a straggler is not jitter, it is
    a starved or dead worker, and `partial interval` in the log is the
    truthful report of that.
    """
    return min(interval * 0.5, 1.0 + 0.01 * nworkers)


def raise_fd_limit(nflows):
    """Every flow is a socket, so make sure this process may open enough.

    Raising the SOFT limit up to the hard limit needs no privilege and
    leaves no trace -- it lives and dies with this process, and workers
    inherit it across fork. The default soft limit of 1024 is exactly
    where a big mesh dies, so this removes the most common startup
    failure without touching the box.

    Only the HARD limit needs an administrator, and that case is a
    refusal with the fix spelled out, never something changed behind the
    operator's back.
    """
    # Flows + per-worker listener/report/queue plumbing + interpreter
    # headroom. Workers inherit the parent's limit, and no worker holds
    # more than the whole agent would.
    need = nflows + 64
    try:
        import resource
    except ImportError:
        return                      # non-POSIX; let the run speak for itself
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return
    if soft >= need:
        return
    if hard != resource.RLIM_INFINITY and hard < need:
        die("this run needs ~%d file descriptors (%d flows; each is a "
            "socket) but the hard limit is %d, which only an administrator "
            "can raise. Either raise it (limits.conf / systemd "
            "LimitNOFILE), lower --streams, use fewer peers (`mx gen "
            "--peers K`), or split the load across more hosts."
            % (need, nflows, hard))
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (need, hard))
        log("fd limit: soft %d -> %d (hard %s; raising the soft limit "
            "needs no privilege and ends with this process)"
            % (soft, need,
               "unlimited" if hard == resource.RLIM_INFINITY else hard))
    except (OSError, ValueError) as exc:
        # Refused despite the headroom (containers can do this): better
        # to start and let flow-open errors name the culprit than to
        # guess wrongly here.
        log("fd limit: could not raise soft limit %d -> %d (%s); "
            "continuing, but flows may fail to open" % (soft, need, exc))


def resolve_streams(spec):
    """--streams N: sockets per peer. More sockets means more 4-tuples,
    which is what lets workers, RSS queues and ECMP paths share the load
    for a mesh with few peers."""
    try:
        n = int(spec)
    except (TypeError, ValueError):
        die("--streams wants a number (got %r)" % spec)
    if n < 1:
        die("--streams must be at least 1")
    if n > MAX_STREAMS:
        die("--streams above %d is almost certainly a mistake (got %d)"
            % (MAX_STREAMS, n))
    return n


def cmd_agent(args):
    matrix = load_matrix(args.matrix)
    me = resolve_self(matrix, args.host)

    bind_ip = ""
    if args.bind:
        iface, bind_ip = resolve_bind(args.bind)
        log("bind: %r -> %s (%s)" % (args.bind, bind_ip, iface))
        check_bind_matches_matrix(matrix, me, bind_ip, iface)

    peers = matrix.peers_of(me)
    streams = resolve_streams(args.streams)
    lay = matrix.layering

    # Resolve every peer address ONCE, here in the parent, before any
    # worker forks. A layered agent rebuilds its flows at every switch,
    # and resolving per flow there would put the DNS resolver -- with
    # its own timeouts -- inside the workers' report schedule: a worker
    # blocked in a lookup misses its ticks, and a host whose workers
    # never all report in one interval never logs a stats line at all,
    # which reads in `mx status` as an agent that went quiet.
    addr_cache = {}

    def resolved(addr):
        ip = addr_cache.get(addr)
        if ip is None:
            try:
                ip = socket.getaddrinfo(addr, None, socket.AF_INET,
                                        socket.SOCK_DGRAM)[0][4][0]
            except OSError:
                # Keep the name: the flow's open() will fail with the
                # real resolver error, once, where it can be seen.
                ip = addr
            addr_cache[addr] = ip
        return ip

    # Each stream is its own socket, so it is its own 4-tuple: that is what
    # gives the kernel, the fabric's ECMP hash and our own workers more
    # than one thing to spread. The pair's rate is split across them, so
    # the offered load is identical however many streams carry it.
    if lay:
        # A layer switch is purely local -- the responder answers whatever
        # arrives -- but the accounting is only clean when switches land
        # on report ticks, which needs the dwell to be a whole number of
        # intervals. Warn rather than refuse: `mx start` refuses up
        # front, and a hand-run agent is better blurry than dead.
        ratio = lay.dwell / args.interval
        if lay.dwell < args.interval or abs(ratio - round(ratio)) > 0.01:
            log("mx agent: dwell %gs is not a whole multiple of the %gs "
                "interval -- layer switches will land mid-interval and "
                "blur the boundary rows" % (lay.dwell, args.interval))
        layer_specs = [[(p, resolved(a), pt, r, s) for p, a, pt, r, s in ls]
                       for ls in layered_flow_specs(matrix, me, streams)]
        maxflows = max(len(s) for s in layer_specs)
        nworkers = resolve_workers(args.workers, maxflows)
        # x2: at every switch the old layer's sockets stay open one
        # interval to catch in-flight replies while the new layer sends.
        raise_fd_limit(2 * maxflows)
    else:
        specs = []
        for peer, pps in peers:
            addr, port = matrix.endpoint(peer)
            per_stream = pps if pps == float("inf") else pps / streams
            for s in range(streams):
                specs.append((peer, resolved(addr), port, per_stream, s))
        nworkers = resolve_workers(args.workers, len(specs))
        # Before any worker forks, so they all inherit the raised limit.
        raise_fd_limit(len(specs))

    cfg = {"sndbuf": args.sndbuf, "rcvbuf": args.rcvbuf, "bind_ip": bind_ip,
           "interval": args.interval, "duration": args.duration,
           "nlayers": lay.layers if lay else 1,
           "dwell": lay.dwell if lay else 0.0}

    # Round-robin so that when the flow count does not divide evenly the
    # remainder is spread, not piled on worker 0. Striding by stream means
    # one peer's streams land on different workers, which is the point.
    # Layered shards are sharded layer by layer, so every layer spreads
    # across the workers the same way.
    if lay:
        shards = [[[] for _ in layer_specs] for _ in range(nworkers)]
        for j, lspecs in enumerate(layer_specs):
            for i, spec in enumerate(lspecs):
                shards[i % nworkers][j].append(spec)
    else:
        shards = [[] for _ in range(nworkers)]
        for i, spec in enumerate(specs):
            shards[i % nworkers].append(spec)

    view = WorkerView(matrix, me)
    stop = multiprocessing.Event()
    queue = multiprocessing.Queue()
    procs = []
    for wid in range(nworkers):
        p = multiprocessing.Process(target=_worker, name="mx-worker-%d" % wid,
                                    args=(wid, view, me, cfg, shards[wid],
                                          stop, queue))
        p.daemon = True
        p.start()
        procs.append(p)

    def on_stop(_sig, _frame):
        stop.set()

    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGINT, on_stop)

    os.makedirs(os.path.dirname(os.path.abspath(args.report)) or ".",
                exist_ok=True)
    reporter = Reporter(me, args.report, matrix.tx_size, matrix.rx_size,
                        nworkers)

    log("mx agent %s: host=%s port=%d peers=%d streams=%d flows=%d "
        "tx_size=%d rx_size=%d workers=%d report=%s"
        % (VERSION, me, matrix.ports[me], len(peers), streams,
           maxflows if lay else len(specs),
           matrix.tx_size, matrix.rx_size, nworkers, args.report))
    if lay:
        log("mx agent: layered -- %d layers of %d peers%s, dwell %gs, cycle "
            "%s; starting in layer %d"
            % (lay.layers, lay.peers, ", equal layers" if lay.fill else "",
               lay.dwell, fmt_secs(lay.cycle()),
               int((time.time() + 0.01) // lay.dwell) % lay.layers))

    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    interval = args.interval
    next_tick = time.monotonic() + interval
    # Wait a little past the tick for stragglers, rather than reporting a
    # window with one worker's numbers missing -- which would read as a
    # host that stopped serving.
    grace = resolve_grace(interval, nworkers)
    pending, seen = [], set()

    while not stop.is_set():
        try:
            pending.append(queue.get(timeout=0.2))
            seen.add(pending[-1].wid)
        except Empty:
            pass
        now = time.monotonic()
        live = sum(1 for p in procs if p.is_alive()) or len(procs)
        if now >= next_tick and (len(seen) >= live or now >= next_tick + grace):
            if pending:
                reporter.tick(pending, complete=len(seen) >= live)
                pending, seen = [], set()
            next_tick = now + interval if now > next_tick + interval \
                else next_tick + interval
        if deadline is not None and now >= deadline:
            break
        if not any(p.is_alive() for p in procs):
            break

    stop.set()
    for p in procs:
        p.join(timeout=5)
    for p in procs:
        if p.is_alive():
            p.terminate()
    # Whatever the workers managed to report on their way out. This one is
    # never treated as complete: the run is ending, workers stop at
    # slightly different moments, and a partial sum here would look like a
    # cliff in the last interval of every run.
    pending.extend(_drain(queue))
    if pending:
        reporter.tick(pending, complete=False)
    queue.close()
    log("mx agent: stopped")
    return 0


def _drain(q):
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except Empty:
            break
        except (OSError, ValueError):             # queue closed under us
            break
    return out


# ---------------------------------------------------------------------------
# Fleet: ssh fan-out
# ---------------------------------------------------------------------------

SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]


class Fleet(object):
    def __init__(self, matrix, args):
        self.matrix = matrix
        self.user = args.user
        self.jobs = max(1, args.jobs)
        self.dir = args.remote_dir
        self.python = args.python
        self.ssh = (args.ssh or "ssh").split()
        self.scp = (args.scp or "scp").split()
        self.dry_run = getattr(args, "dry_run", False)

    def target(self, host):
        addr = self.matrix.addrs[host]
        return "%s@%s" % (self.user, addr) if self.user else addr

    def rpath(self, name):
        return "%s/%s" % (self.dir, name)

    def sh(self, host, script, timeout=120):
        """Run a shell snippet on one host. Returns (rc, output)."""
        cmd = self.ssh + SSH_OPTS + [self.target(host), script]
        return self._run(cmd, timeout)

    def push(self, host, local_paths, remote_name=None, timeout=300):
        dest = "%s:%s" % (self.target(host), self.rpath(remote_name or ""))
        cmd = self.scp + SSH_OPTS + ["-q"] + list(local_paths) + [dest]
        return self._run(cmd, timeout)

    def pull(self, host, remote_name, local_path, timeout=300):
        src = "%s:%s" % (self.target(host), self.rpath(remote_name))
        cmd = self.scp + SSH_OPTS + ["-q", src, local_path]
        return self._run(cmd, timeout)

    def _run(self, cmd, timeout):
        if self.dry_run:
            return 0, "DRY-RUN %s" % " ".join(shlex.quote(c) for c in cmd)
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL,
                                 universal_newlines=True)
            out, _ = p.communicate(timeout=timeout)
            return p.returncode, (out or "").strip()
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()
            return 124, "timed out after %ss" % timeout
        except OSError as exc:
            return 127, str(exc)

    def each(self, fn, label=None, quiet=False):
        """Run fn(host) on every host, at most --jobs at a time.

        fn returns (rc, text). Output is printed host-prefixed in matrix
        order (not completion order, so runs are diffable), and the
        number of failures is returned.
        """
        hosts = self.matrix.hosts
        if label:
            log("[mx] %s on %d hosts" % (label, len(hosts)))
        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            results = list(pool.map(fn, hosts))
        width = max(len(h) for h in hosts)
        failed = []
        for host, (rc, text) in zip(hosts, results):
            if rc != 0:
                failed.append(host)
            if quiet and rc == 0:
                continue
            for line in (text or "").splitlines() or [""]:
                log("  %-*s  %s" % (width, host, line))
        if failed:
            log("[mx] FAILED on %d/%d hosts: %s"
                % (len(failed), len(hosts), " ".join(failed)))
        return len(failed)


def _agent_source():
    """This file, resolved through any symlink -- it is what gets copied
    to the hosts, so the fleet always runs exactly this version."""
    return os.path.realpath(os.path.abspath(__file__))


def _agent_flags(args):
    flags = ["--interval", "%g" % args.interval]
    if args.duration:
        flags += ["--duration", "%g" % args.duration]
    if args.workers and args.workers != "auto":
        flags += ["--workers", str(args.workers)]
    if getattr(args, "streams", 1) and args.streams != 1:
        flags += ["--streams", str(args.streams)]
    if args.bind:
        flags += ["--bind", args.bind]
    if args.sndbuf:
        flags += ["--sndbuf", str(args.sndbuf)]
    if args.rcvbuf:
        flags += ["--rcvbuf", str(args.rcvbuf)]
    return flags


# The pgrep pattern is bracketed so the very shell running it is not itself
# a match: that shell's command line contains the literal "[m]x.py agent",
# which the regex "mx.py agent" does not match.
PGREP = "pgrep -f '[m]x[.]py agent'"
PKILL = "pkill -%s -f '[m]x[.]py agent'"


def _kill_block(rdir):
    """Shell that stops the agent and leaves $status set. Falls through in
    every case -- `clean` appends the removal to it, so it must never
    exit early.

    The pid file is the primary handle; the pgrep/pkill fallback matches
    any mx agent on the box, so a host running two matrices out of
    different --remote-dirs at once should be stopped by pid (which it
    will be, unless the pid file was lost).
    """
    return """
d={d}
status=not-deployed
if [ -d "$d" ]; then
    status=not-running
    pid=""
    [ -f "$d/{pid}" ] && pid=$(cat "$d/{pid}" 2>/dev/null)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        status=stopped
    elif {pgrep} >/dev/null 2>&1; then
        {term} 2>/dev/null
        status=stopped
    fi
    if [ "$status" = stopped ]; then
        i=0
        while [ $i -lt 20 ]; do
            {pgrep} >/dev/null 2>&1 || break
            sleep 0.5
            i=$((i+1))
        done
        if {pgrep} >/dev/null 2>&1; then
            {kill9} 2>/dev/null
            status=killed
        fi
    fi
    rm -f "$d/{pid}"
fi
""".format(d=shlex.quote(rdir), pid=PID_NAME, pgrep=PGREP,
           term=PKILL % "TERM", kill9=PKILL % "KILL")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def read_server_list(path):
    if not os.path.isfile(path):
        die("server list not found: %s  (one host per line)" % path)
    hosts = []
    with open(path) as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                hosts.append(line)
    if len(hosts) < 2:
        die("%s: need at least 2 servers" % path)
    names = [parse_token(h)[0] for h in hosts]
    dupes = set(n for n in names if names.count(n) > 1)
    if dupes:
        die("%s: duplicate hosts: %s" % (path, ", ".join(sorted(dupes))))
    return hosts


def sparse_edges(n, k, seed):
    """The flow set for --peers: a k-regular random digraph as k rounds of
    a shifted permutation.

    Relabel the hosts by one random permutation p, then round r sends
    p[i] -> p[(i + shift_r) % n] with k distinct nonzero shifts. Every
    round is a permutation, so after k rounds every host has EXACTLY k
    flows out and k in -- the equal-load property is by construction, not
    statistical. Distinct shifts on one relabeling can never produce a
    duplicate edge or a self-loop, and the whole thing replays from the
    seed.

    Returns a set of (src_index, dst_index).
    """
    rng = random.Random(seed)
    relabel = list(range(n))
    rng.shuffle(relabel)
    return shift_edges(relabel, rng.sample(range(1, n), k))


def shift_edges(relabel, shifts):
    """The edges of the given shifts on one relabeling: position i sends
    to position (i + shift) % n, translated back to host indices."""
    n = len(relabel)
    edges = set()
    for shift in shifts:
        for i in range(n):
            edges.add((relabel[i], relabel[(i + shift) % n]))
    return edges


def layer_partition(n, k, seed, fill=False):
    """The layers of `--peers K --dwell T`: ONE permutation shared by every
    layer, and the n-1 nonzero shifts dealt out k at a time.

    The union of all n-1 shifts on one relabeling is the complete digraph,
    every ordered pair exactly once -- so disjoint shift sets make the
    layers edge-disjoint by construction, and a full rotation measures
    every pair exactly once. Independent random matrices per layer would
    instead collide by birthday and leave a tail of pairs never measured.

    The last layer keeps the remainder when k does not divide n-1: fewer
    peers for one dwell, never a pair measured twice or missed. With
    `fill` (--equal-layers) that layer is instead padded back up to k
    with shifts repeated from the front of the deal, so every layer
    carries exactly k and per-host load never dips -- at the price of
    the padded shifts being measured twice per cycle. The pad shifts
    come from other layers, so within any one layer the shifts are still
    distinct: every layer is exactly k-regular either way.

    Returns (relabel, [shift list per layer]).
    """
    rng = random.Random(seed)
    relabel = list(range(n))
    rng.shuffle(relabel)
    shifts = list(range(1, n))
    rng.shuffle(shifts)
    layers = [shifts[i:i + k] for i in range(0, n - 1, k)]
    if fill and len(layers) > 1 and len(layers[-1]) < k:
        layers[-1] = layers[-1] + shifts[:k - len(layers[-1])]
    return relabel, layers


def layered_flow_specs(matrix, me, streams):
    """Every layer's flow specs for this host, derived locally.

    sparse layers are a pure function of (hosts, peers, seed) and every
    host holds the identical host list, so each agent derives the whole
    schedule itself -- one matrix file deploys however many layers it
    rotates through, and there is no control channel to fail.
    """
    lay = matrix.layering
    n = len(matrix.hosts)
    relabel, shift_layers = layer_partition(n, lay.peers, lay.seed, lay.fill)
    pos = [0] * n
    for i, h in enumerate(relabel):
        pos[h] = i
    my_pos = pos[matrix.index[me]]
    # Per-pair pps is held constant across layers (so per-host load is
    # flat), and every populated cell of a layered matrix carries that
    # same one value -- any of this host's layer-0 cells is the rate.
    rate = matrix.peers_of(me)[0][1]
    specs = []
    for shifts in shift_layers:
        one = []
        for shift in shifts:
            peer = matrix.hosts[relabel[(my_pos + shift) % n]]
            addr, port = matrix.endpoint(peer)
            per_stream = rate if rate == float("inf") else rate / streams
            for s in range(streams):
                one.append((peer, addr, port, per_stream, s))
        specs.append(one)
    # Layer 0 must be exactly what the grid in the file says, or the
    # header and the body have drifted apart (a hand edit): refuse rather
    # than run a schedule half the fleet would disagree with.
    body = set(p for p, _r in matrix.peers_of(me))
    derived = set(s[0] for s in specs[0])
    if body != derived:
        die("%s: the grid does not match its layers header (edited by "
            "hand?) -- regenerate with `mx gen --peers %d --dwell %g "
            "--seed %d`" % (matrix.path, lay.peers, lay.dwell, lay.seed))
    return specs


def cmd_gen(args):
    tokens = read_server_list(args.servers)
    n = len(tokens)
    tx_size = check_size("--tx-size", args.tx_size)
    rx_size = check_size("--rx-size", args.rx_size)

    peers = n - 1
    edges = None
    seed = None
    nlayers = 1
    if args.dwell is not None and args.peers is None:
        die("--dwell rotates a --peers matrix through its layers; it needs "
            "--peers K to say how many flows each layer carries")
    if args.equal_layers and args.dwell is None:
        die("--equal-layers pads the rotation's remainder layer; it only "
            "means something with --peers K --dwell T")
    if args.peers is not None:
        if not 1 <= args.peers <= n - 1:
            die("--peers must be between 1 and %d for %d hosts (got %d)"
                % (n - 1, n, args.peers))
        peers = args.peers
        seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2 ** 32)
        if args.dwell is not None:
            if seed < 0:
                die("--seed must be non-negative for a layered matrix")
            if args.dwell < 1.0:
                die("--dwell below 1 second cannot be measured: the report "
                    "interval is the floor, and a layer needs a few clean "
                    "intervals to mean anything (3x the interval is a good "
                    "dwell; the default interval is %gs)" % DEFAULT_INTERVAL)
            # The layer count is derived, never chosen: that is the hard
            # guarantee that a full cycle covers every ordered pair
            # exactly once, with no pair repeated and none missed.
            nlayers = -(-(n - 1) // peers)
            if nlayers < 2:
                die("--peers %d already reaches all %d peers in one layer; "
                    "--dwell has nothing to rotate. Lower --peers, or drop "
                    "--dwell for a fixed sparse matrix." % (peers, n - 1))
            relabel, shift_layers = layer_partition(n, peers, seed,
                                                    args.equal_layers)
            edges = shift_edges(relabel, shift_layers[0])
        elif peers < n - 1:
            edges = sparse_edges(n, peers, seed)

    if args.pps is not None:
        cell = args.pps
        if cell <= 0:
            die("--pps must be greater than 0 (or 'max' for unpaced)")
    elif args.gbps is not None:
        if args.gbps <= 0:
            die("--gbps must be greater than 0")
        # Per-host egress budget in wire bits, split across its peers --
        # fewer peers means fatter flows, same per-host total.
        per_host_pps = args.gbps * 1e9 / ((tx_size + WIRE_OVERHEAD) * 8.0)
        cell = per_host_pps / peers
    else:
        cell = 10000.0

    if cell == float("inf"):
        text = "max"
    elif cell >= 100:
        text = "%.0f" % cell
    else:
        # Keep the precision that matters at low rates: rounding 0.4 pps
        # to "0" would silently delete the flow.
        text = "%.3f" % cell

    def cell_for(si, di):
        if si == di:
            return ""
        if edges is not None and (si, di) not in edges:
            return ""
        return text

    extra = ""
    if args.peers is not None:
        extra = ("# peers=%d seed=%d -- k-regular shuffle: every host sends to "
                 "and receives from exactly %d others\n" % (peers, seed, peers))
    if nlayers > 1:
        extra += ("# layers=%d dwell=%g%s -- disjoint-shift layers on that "
                  "one shuffle: the grid below\n"
                  "#   is layer 0; every agent derives layer "
                  "(walltime // dwell) %% layers itself,\n"
                  "#   and one full cycle measures every ordered pair %s\n"
                  % (nlayers, args.dwell,
                     " fill=1" if args.equal_layers else "",
                     "at least once (the remainder\n#   layer is padded "
                     "with repeats, so every layer carries exactly the "
                     "same load)" if args.equal_layers else "exactly once"))
    write_matrix(args.output, tokens, cell_for, tx_size, rx_size, args.port,
                 extra_comment=extra)
    if args.output == "-":
        return 0

    nflows = n * peers
    log("wrote %s" % args.output)
    log("  %d hosts, %d flows, %d bytes out -> %d bytes back"
        % (n, nflows, tx_size, rx_size))
    if edges is not None:
        log("  shape    : k-regular shuffle, %d peers per host (seed %d -- "
            "reuse --seed %d to replay)" % (peers, seed, seed))
    if nlayers > 1:
        last = (n - 1) - peers * (nlayers - 1)
        cycle = nlayers * args.dwell
        log("  layers   : %d, held %gs each -- every ordered pair measured "
            "%s per %s cycle"
            % (nlayers, args.dwell,
               "at least once" if args.equal_layers and last != peers
               else "exactly once", fmt_secs(cycle)))
        if last != peers and args.equal_layers:
            log("             every layer carries exactly %d peers per host "
                "(--equal-layers): the last\n"
                "             layer's %d leftover shifts are padded with %d "
                "repeats, so those %d pairs\n"
                "             per host are measured twice per cycle and the "
                "load never dips" % (peers, last, peers - last, peers - last))
        elif last != peers:
            log("             the last layer carries the remainder: %d "
                "peer%s per host instead of %d (use\n"
                "             --equal-layers to pad it with repeats and "
                "hold the load flat)"
                % (last, "" if last == 1 else "s", peers))
        elif args.equal_layers:
            log("             --equal-layers: %d divides %d evenly, so "
                "there is nothing to pad" % (peers, n - 1))
        log("             dwell must be a whole multiple of the agents' "
            "--interval (default %gs); `mx start` checks" % DEFAULT_INTERVAL)
    if cell == float("inf"):
        log("  rate: unpaced (send as fast as each host can)")
    else:
        eg = cell * peers
        log("  per pair : %s" % fmt_pps(cell))
        log("  per host : %s out, %s back in -- every host identical"
            % (fmt_pps(eg), fmt_pps(eg)))
        log("             %s out on the wire (%s payload)"
            % (fmt_gbps(wire_bps(eg, tx_size)), fmt_gbps(payload_bps(eg, tx_size))))
        log("             %s back on the wire (%s payload)"
            % (fmt_gbps(wire_bps(eg, rx_size)), fmt_gbps(payload_bps(eg, rx_size))))
        log("  fleet    : %s offered" % fmt_pps(cell * nflows))
    log("")
    log("next: mx check --nic-gbps 25     # will the NICs take it?")
    log("      mx start                   # deploy and run it")
    return 0


def cmd_check(args):
    m = load_matrix(args.matrix)
    egress_pps = dict((h, 0.0) for h in m.hosts)
    ingress_pps = dict((h, 0.0) for h in m.hosts)
    unpaced = 0
    for (s, d), pps in m.rates.items():
        if pps == float("inf"):
            unpaced += 1
            continue
        # A request out is a reply back, and vice versa: both directions
        # carry the same packet rate, only the sizes differ.
        egress_pps[s] += pps
        ingress_pps[d] += pps

    log("matrix %s: %d hosts, %d flows, %d bytes out -> %d bytes back, port %d"
        % (m.path, len(m.hosts), len(m.rates), m.tx_size, m.rx_size, m.port))
    if m.layering:
        lay = m.layering
        # The grid is layer 0, so every per-host figure below is one
        # layer's load -- which is the whole point: it never gets bigger,
        # the rotation only changes WHO carries it.
        log("  layered: %d layers of %d peers, dwell %gs -- the loads below "
            "are one layer's, held constant while the pairs rotate; every "
            "ordered pair is measured %s per %s cycle"
            % (lay.layers, lay.peers, lay.dwell,
               "at least once (equal layers)" if lay.fill else "once",
               fmt_secs(lay.cycle())))
    if unpaced:
        log("  %d flows are unpaced ('max'): offered load is whatever the "
            "senders manage, so the caps below cannot be checked" % unpaced)

    problems = 0
    rows = []
    for h in m.hosts:
        out_pps, in_pps = egress_pps[h], ingress_pps[h]
        # Out: requests this host sends + replies it owes its callers.
        out_bps = wire_bps(out_pps, m.tx_size) + wire_bps(in_pps, m.rx_size)
        in_bps = wire_bps(in_pps, m.tx_size) + wire_bps(out_pps, m.rx_size)
        rows.append((h, out_pps + in_pps, out_bps, in_bps))
    rows.sort(key=lambda r: -max(r[2], r[3]))

    log("")
    log("  %-24s %12s %14s %14s" % ("host", "packets/s", "out (wire)", "in (wire)"))
    for h, pps, out_bps, in_bps in rows[:args.top]:
        log("  %-24s %12s %14s %14s"
            % (h, fmt_pps(pps), fmt_gbps(out_bps), fmt_gbps(in_bps)))
    if len(rows) > args.top:
        log("  ... %d more (raise --top)" % (len(rows) - args.top))

    if args.nic_gbps or args.nic_mpps:
        log("")
        cap_bps = (args.nic_gbps or 0) * 1e9
        cap_pps = (args.nic_mpps or 0) * 1e6
        for h, pps, out_bps, in_bps in rows:
            if cap_bps and max(out_bps, in_bps) > cap_bps:
                log("  OVER CAPACITY %s: %s out / %s in > %s NIC"
                    % (h, fmt_gbps(out_bps), fmt_gbps(in_bps), fmt_gbps(cap_bps)))
                problems += 1
            if cap_pps and pps > cap_pps:
                log("  OVER CAPACITY %s: %s > %s NIC packet rate"
                    % (h, fmt_pps(pps), fmt_pps(cap_pps)))
                problems += 1

    total_pps = sum(v for v in egress_pps.values())
    log("")
    log("  fleet offered: %s requests + the same rate in replies"
        % fmt_pps(total_pps))
    if problems:
        log("  NOT admissible: %d host(s) over the caps you gave" % problems)
        log("  fix it: lower --pps in `mx gen`, or shrink --tx-size/--rx-size")
        return 1
    if args.nic_gbps or args.nic_mpps:
        log("  admissible against the caps you gave")
    else:
        log("  no caps given -- pass --nic-gbps and/or --nic-mpps to check them")
    log("")
    log("  note: one agent worker process sustains roughly 200k packets/sec of")
    log("        request+reply. `--workers auto` (the default) runs one per")
    log("        core, so a host's own ceiling is about 200k x cores.")
    return 0


def _probe_bind_ips(fleet, spec):
    """Resolve the --bind pattern ON EVERY HOST: same substring match
    against that host's own `ip -o -4 addr show`. Returns name -> IPv4.

    This is the load-bearing half of --bind, and the same scheme
    iperf-orchestrator uses (its PEER_BIND_IPS / conn_ip): the server
    list carries the login addresses, but pinning traffic to a NIC means
    *targeting* each peer's address on that NIC, not just binding the
    local sockets. Dies listing every host with no match, before a full
    mesh of doomed agents is launched.
    """
    script = ("ip -o -4 addr show 2>/dev/null | grep -F -m1 -- %s | "
              "awk '{print $4}' | cut -d/ -f1" % shlex.quote(spec))
    hosts = fleet.matrix.hosts
    with ThreadPoolExecutor(max_workers=fleet.jobs) as pool:
        results = list(pool.map(lambda h: fleet.sh(h, script, timeout=30),
                                hosts))
    ips, missing = {}, []
    for host, (rc, out) in zip(hosts, results):
        ip = out.strip().splitlines()[-1].strip() if out.strip() else ""
        if rc != 0 or not ip:
            missing.append(host)
        else:
            ips[host] = ip
    if missing:
        die("--bind %r matches no interface on %d host(s): %s -- fix the "
            "pattern (it is substring-matched against `ip -o -4 addr show` "
            "on each host) before starting a mesh that cannot work."
            % (spec, len(missing), " ".join(missing[:10])))
    return ips


def _write_retargeted_matrix(m, addr_of, path):
    """The deployed matrix, with every address replaced by that host's
    data-plane IP. Names, ports, rates and sizes are untouched, so the
    local matrix keeps the login addresses ssh needs."""
    tokens = ["%s=%s:%d" % (h, addr_of[h], m.ports[h]) for h in m.hosts]

    def cell(si, di):
        pps = m.rates.get((m.hosts[si], m.hosts[di]), 0)
        if not pps:
            return ""
        return "max" if pps == float("inf") else "%.3f" % pps

    # The layering header is load-bearing: it is the only place the other
    # layers exist, so dropping it here would silently freeze the fleet
    # in layer 0.
    extra = ""
    if m.layering:
        lay = m.layering
        extra = ("# peers=%d seed=%d layers=%d dwell=%g%s -- layered; the "
                 "grid is layer 0\n"
                 % (lay.peers, lay.seed, lay.layers, lay.dwell,
                    " fill=1" if lay.fill else ""))
    write_matrix(path, tokens, cell, m.tx_size, m.rx_size, m.port,
                 extra_comment=extra)


def cmd_start(args):
    m = load_matrix(args.matrix)
    if m.layering:
        # Refuse here, once, rather than deploying a fleet of agents that
        # would each warn about blurred boundaries. A tick-aligned dwell
        # is what keeps every report row inside a single layer.
        lay = m.layering
        ratio = lay.dwell / args.interval
        if lay.dwell < args.interval or abs(ratio - round(ratio)) > 0.01:
            die("layered matrix: dwell %gs must be a whole multiple of "
                "--interval (%gs) so layer switches land on report ticks. "
                "Try --interval %g."
                % (lay.dwell, args.interval,
                   lay.dwell / max(1, round(ratio))))
    fleet = Fleet(m, args)
    agent_src = _agent_source()
    flags = _agent_flags(args)

    if not args.no_deploy:
        # scp lands a file under its own basename, and the agent always
        # looks for matrix.csv -- so stage a copy under that name when the
        # local file is called something else. With --bind the staged copy
        # is retargeted at each host's data-plane IP, so the traffic rides
        # the bound NIC end to end while ssh keeps the login addresses.
        tmpdir = None
        matrix_src = args.matrix
        if args.bind and not fleet.dry_run:
            bind_ips = _probe_bind_ips(fleet, args.bind)
            tmpdir = tempfile.mkdtemp(prefix="mx-")
            matrix_src = os.path.join(tmpdir, MATRIX_NAME)
            _write_retargeted_matrix(m, bind_ips, matrix_src)
            changed = sum(1 for h in m.hosts if m.addrs[h] != bind_ips[h])
            log("[mx] --bind %r: matrix retargeted at each host's matching "
                "address (%d of %d changed)" % (args.bind, changed, len(m.hosts)))
        elif os.path.basename(matrix_src) != MATRIX_NAME:
            tmpdir = tempfile.mkdtemp(prefix="mx-")
            matrix_src = os.path.join(tmpdir, MATRIX_NAME)
            shutil.copyfile(args.matrix, matrix_src)

        def deploy(host):
            rc, out = fleet.sh(host, "mkdir -p %s" % shlex.quote(fleet.dir))
            if rc != 0:
                return rc, "mkdir failed: %s" % out
            rc, out = fleet.push(host, [agent_src, matrix_src])
            if rc != 0:
                return rc, "copy failed: %s" % out
            return 0, "deployed"

        try:
            if fleet.each(deploy, "deploying agent + matrix", quiet=True):
                return 1
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

    def start(host):
        # The agent is backgrounded as a *simple* command with all three
        # descriptors redirected: $! is then the agent's own pid, and
        # nothing is left holding ssh's channel open -- background an
        # `A && B` list instead and ssh hangs until the agent exits.
        cmd = ("nohup %s %s agent --matrix %s --host %s --report %s %s "
               "< /dev/null >> %s 2>&1 &"
               % (shlex.quote(fleet.python), os.path.basename(agent_src),
                  MATRIX_NAME, shlex.quote(host), REPORT_NAME,
                  " ".join(shlex.quote(f) for f in flags), LOG_NAME))
        script = """
d={d}
cd "$d" 2>/dev/null || {{ echo 'not deployed (drop --no-deploy)'; exit 1; }}
if [ -f {pid} ] && kill -0 "$(cat {pid} 2>/dev/null)" 2>/dev/null; then
    echo 'already running -- mx stop first'; exit 1
fi
rm -f {log} {report} {pid}
{cmd}
agent_pid=$!
echo "$agent_pid" > {pid}
sleep 0.5
if kill -0 "$agent_pid" 2>/dev/null; then
    echo started
else
    echo 'died on startup:'
    tail -n 5 {log} 2>/dev/null
    rm -f {pid}
    exit 1
fi
""".format(d=shlex.quote(fleet.dir), pid=PID_NAME, log=LOG_NAME,
           report=REPORT_NAME, cmd=cmd)
        return fleet.sh(host, script)

    failed = fleet.each(start, "starting agents", quiet=True)
    if failed:
        log("[mx] some hosts did not start; `mx logs` will tell you why")
        return 1
    log("[mx] running: %d hosts, %d flows, %d -> %d bytes, %s"
        % (len(m.hosts), len(m.rates), m.tx_size, m.rx_size,
           "unpaced" if any(v == float("inf") for v in m.rates.values())
           else fmt_pps(sum(m.rates.values()))))
    if m.layering:
        lay = m.layering
        log("[mx] layered: %d layers of %d peers, dwell %gs -- every "
            "ordered pair measured once per %s cycle; keep it running at "
            "least that long for full coverage"
            % (lay.layers, lay.peers, lay.dwell, fmt_secs(lay.cycle())))
    log("[mx] next: mx status      # one line per host")
    log("[mx]       mx summarize   # pps / Gbps / loss / rtt")
    log("[mx]       mx stop        # when you are done")
    return 0


def cmd_status(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    script = """
d={d}
[ -d "$d" ] || {{ echo NOT-DEPLOYED; exit 0; }}
cd "$d"
if [ -f {pid} ] && kill -0 "$(cat {pid} 2>/dev/null)" 2>/dev/null; then
    tail -n 1 {log} 2>/dev/null || echo 'running (no report yet)'
else
    echo NOT-RUNNING
fi
""".format(d=shlex.quote(fleet.dir), pid=PID_NAME, log=LOG_NAME)

    def one(host):
        return fleet.sh(host, script, timeout=30)

    while True:
        if args.watch:
            sys.stdout.write("\033[2J\033[H")
            log("mx status -- %s (every %gs, ctrl-c to stop)"
                % (time.strftime("%H:%M:%S"), args.watch))
        fleet.each(one)
        if not args.watch:
            break
        try:
            time.sleep(args.watch)
        except KeyboardInterrupt:
            break
    return 0


def cmd_stop(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    script = _kill_block(fleet.dir) + 'echo "$status"\n'
    failed = fleet.each(lambda h: fleet.sh(h, script), "stopping agents")
    log("[mx] reports and logs are still on the hosts:")
    log("[mx]   mx summarize   # collect + analyze")
    log("[mx]   mx logs        # collect agent logs")
    log("[mx]   mx clean       # delete every trace")
    return 1 if failed else 0


def cmd_logs(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    os.makedirs(args.output, exist_ok=True)

    def one(host):
        dest = os.path.join(args.output, "%s.log" % host)
        rc, out = fleet.pull(host, LOG_NAME, dest)
        if rc != 0:
            return rc, "no log: %s" % out
        return 0, "-> %s" % dest

    failed = fleet.each(one, "collecting logs into %s/" % args.output)
    return 1 if failed else 0


def cmd_clean(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    if not args.yes and not args.dry_run:
        log("[mx] this stops every agent and deletes %s on %d hosts."
            % (fleet.dir, len(m.hosts)))
        log("[mx] collect anything you want first (mx summarize, mx logs).")
        try:
            reply = input("[mx] type 'yes' to continue: ")
        except EOFError:
            reply = ""
        if reply.strip().lower() != "yes":
            log("[mx] nothing done")
            return 1
    script = _kill_block(fleet.dir) + """
rm -rf "$d"
if [ -e "$d" ]; then echo "LEFTOVER: $d still exists"; exit 1; fi
if {pgrep} >/dev/null 2>&1; then echo 'LEFTOVER: an agent is still running'; exit 1; fi
echo "clean (was: $status)"
""".format(pgrep=PGREP)
    failed = fleet.each(lambda h: fleet.sh(h, script), "removing every trace")
    if failed:
        return 1
    log("[mx] nothing of mx remains on the fleet "
        "(no packages, no sysctls, no unit files -- there never were any)")
    return 0


def cmd_collect(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    os.makedirs(args.reports, exist_ok=True)

    def one(host):
        dest = os.path.join(args.reports, "%s.csv" % host)
        rc, out = fleet.pull(host, REPORT_NAME, dest)
        if rc != 0:
            return rc, "no report yet: %s" % out
        return 0, "-> %s" % dest

    failed = fleet.each(one, "collecting reports into %s/" % args.reports,
                        quiet=True)
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------

class Agg(object):
    """Mean and max of report columns over a window of samples.

    Counts are per column, not per row: a flow whose replies dried up for
    an interval writes no rtt at all, and averaging those blanks in as
    zeroes would flatter the latency.
    """

    def __init__(self):
        self.sums = {}
        self.counts = {}
        self.maxes = {}

    def add(self, row, keys):
        for k in keys:
            v = row.get(k, "")
            if v in ("", None):
                continue
            try:
                fv = float(v)
            except ValueError:
                continue
            self.sums[k] = self.sums.get(k, 0.0) + fv
            self.counts[k] = self.counts.get(k, 0) + 1
            if fv > self.maxes.get(k, float("-inf")):
                self.maxes[k] = fv

    def mean(self, key):
        n = self.counts.get(key, 0)
        return self.sums[key] / n if n else 0.0

    def peak(self, key):
        v = self.maxes.get(key)
        return v if v is not None else 0.0


# The report columns each aggregate needs. Named once, because
# `summarize` and `export` have to read the same run the same way.
TX_KEYS = ["target_pps", "pps", "mbps", "rep_pps", "rep_mbps",
           "rtt_avg_us", "rtt_p50_us", "rtt_p99_us", "rtt_max_us"]
HOST_KEYS = ["target_pps", "pps", "mbps", "rep_pps", "rep_mbps",
             "cpu_pct", "cpu_max_pct", "agent_cpu_pct", "workers"]


def _aggregate(recent):
    """Sort a window of report rows into the three aggregates every
    analysis needs: the client side per pair, the server side per pair,
    and the host totals. Blank cells are skipped by `Agg`, never counted
    as zero."""
    tx = {}       # (host, peer) -> Agg   (client side: what we sent/got back)
    srv = {}      # (host, peer) -> Agg   (server side: what actually arrived)
    hostagg = {}  # host -> Agg
    for r in recent:
        d = r.get("dir")
        if d == "tx":
            tx.setdefault((r["host"], r["peer"]), Agg()).add(r, TX_KEYS)
        elif d == "rx":
            srv.setdefault((r["host"], r["peer"]), Agg()).add(r, ["pps", "rep_pps"])
        elif d == "host":
            hostagg.setdefault(r["host"], Agg()).add(r, HOST_KEYS)
    return tx, srv, hostagg


def _per_host(tx, srv, hostagg, nticks, layered):
    """Per-host rates over the window, aggregated the way the fleet totals
    are: from the per-pair rows in a full mesh, and from the host rows in a
    layered run, where most pairs are idle most of the time and summing
    per-pair means would count every layer as if it had run the whole
    window.

    Latency is always the worst peer's, never a mean of percentiles.
    """
    stats = {}

    def entry(host):
        h = stats.get(host)
        if h is None:
            h = stats[host] = {"sent": 0.0, "back": 0.0, "target": 0.0,
                               "served": 0.0, "served_rep": 0.0,
                               "rtt_avg": 0.0, "rtt_p50": 0.0, "rtt_p99": 0.0,
                               "rtt_max": 0.0, "peers": 0, "peer_set": set(),
                               # Did anything report on this host's receive
                               # side? Without a single rx row, "0 served" is
                               # not a measurement of nothing -- it is nothing
                               # measured, and must not be exported as a zero.
                               "served_seen": False, "rtt_avg_n": 0}
        return h

    for (host, peer), a in tx.items():
        h = entry(host)
        h["peers"] += 1
        h["peer_set"].add(peer)
        avg = a.mean("rtt_avg_us")
        if avg > 0:
            h["rtt_avg"] += avg
            h["rtt_avg_n"] += 1
        if not layered:
            h["sent"] += a.mean("pps")
            h["back"] += a.mean("rep_pps")
            h["target"] += a.mean("target_pps")
        h["rtt_p50"] = max(h["rtt_p50"], a.mean("rtt_p50_us"))
        h["rtt_p99"] = max(h["rtt_p99"], a.mean("rtt_p99_us"))
        h["rtt_max"] = max(h["rtt_max"], a.peak("rtt_max_us"))
    if layered:
        for host, a in hostagg.items():
            h = entry(host)
            h["sent"] = a.mean("pps")
            h["back"] = a.mean("rep_pps")
            h["target"] = a.mean("target_pps")
    for (host, _peer), a in srv.items():
        h = entry(host)
        h["served_seen"] = True
        if layered:
            # The server rows are per-interval counts of what arrived; in a
            # layered run only the intervals the host itself reported can
            # turn them back into a rate.
            if not nticks.get(host):
                continue
            h["served"] += a.sums.get("pps", 0.0) / nticks[host]
            h["served_rep"] += a.sums.get("rep_pps", 0.0) / nticks[host]
        else:
            h["served"] += a.mean("pps")
            h["served_rep"] += a.mean("rep_pps")
    for h in stats.values():
        if h["rtt_avg_n"]:
            h["rtt_avg"] /= h["rtt_avg_n"]
    return stats


def _forward_delivery(stats, srv, nticks, layered):
    """host -> the packet rate of its requests that its peers actually
    counted arriving, or None when that cannot be known.

    This is the receiver-side truth a sender cannot see, and it is what
    separates a host whose requests are being dropped on the way out from
    one whose replies are lost coming back. It needs every peer's rx row
    for this host: with even one missing, the total would be short and the
    forward loss it implies would be invented rather than measured.
    """
    forward = {}
    for host, h in stats.items():
        arrived = 0.0
        for peer in h["peer_set"]:
            agg = srv.get((peer, host))
            if agg is None:
                arrived = None
                break
            if layered:
                # Normalised by the *receiving* host's interval count, the
                # same way the fleet total is.
                if not nticks.get(peer):
                    arrived = None
                    break
                arrived += agg.sums.get("pps", 0.0) / nticks[peer]
            else:
                arrived += agg.mean("pps")
        forward[host] = arrived if h["peer_set"] else None
    return forward


def _report_sizes(rows, tx_size, rx_size):
    """The packet sizes the run actually used.

    They come from the reports, not from the matrix: the matrix may have
    been re-generated since the run, and the bytes that were on the wire
    are the ones worth converting to Gbps.
    """
    for r in rows:
        if r.get("size") and r.get("rep_size"):
            try:
                return int(r["size"]), int(r["rep_size"])
            except ValueError:
                pass
            break
    return tx_size, rx_size


def _read_reports(paths):
    rows = []
    for p in paths:
        try:
            with open(p, newline="") as f:
                for r in csv.DictReader(f):
                    if r.get("ts"):
                        rows.append(r)
        except OSError as exc:
            log("[mx] cannot read %s: %s" % (p, exc))
    return rows


def cmd_summarize(args):
    m = load_matrix(args.matrix)
    if not args.no_collect:
        cmd_collect(args)
    paths = sorted(os.path.join(args.reports, f)
                   for f in os.listdir(args.reports)
                   if f.endswith(".csv")) if os.path.isdir(args.reports) else []
    if not paths:
        die("no reports in %s/ -- is the run started? (mx status)" % args.reports)

    rows = _read_reports(paths)
    if not rows:
        die("reports are empty -- give the agents an interval or two, then retry")
    latest = max(int(float(r["ts"])) for r in rows)
    cutoff = latest - args.window
    recent = [r for r in rows if int(float(r["ts"])) >= cutoff]

    tx_size, rx_size = _report_sizes(recent, m.tx_size, m.rx_size)

    tx, srv, hostagg = _aggregate(recent)
    if not tx:
        die("no client rows in the last %ds -- are the agents still running?"
            % args.window)

    layered = m.layering is not None
    # How many complete intervals each host reported in the window: the
    # divisor that turns a sparse pile of per-pair rows back into a
    # time-averaged rate.
    nticks = {h: a.counts.get("pps", 0) for h, a in hostagg.items()}

    if layered and hostagg:
        # In a layered run most pairs are idle for most of the window, and
        # a pair's Agg only averages its ACTIVE intervals -- summing pair
        # means would count every layer as if it ran the whole time. The
        # host rows are instantaneous sums over whatever was active, so
        # their time-average is the truth.
        tx_pps = sum(a.mean("pps") for a in hostagg.values())
        rep_pps = sum(a.mean("rep_pps") for a in hostagg.values())
        target = sum(a.mean("target_pps") for a in hostagg.values())
        tx_bps = sum(a.mean("mbps") for a in hostagg.values()) * 1e6
        rep_bps = sum(a.mean("rep_mbps") for a in hostagg.values()) * 1e6
        fwd_pps = sum(a.sums.get("pps", 0.0) / nticks[h]
                      for (h, _p), a in srv.items() if nticks.get(h))
    else:
        tx_pps = sum(a.mean("pps") for a in tx.values())
        rep_pps = sum(a.mean("rep_pps") for a in tx.values())
        target = sum(a.mean("target_pps") for a in tx.values())
        tx_bps = sum(a.mean("mbps") for a in tx.values()) * 1e6
        rep_bps = sum(a.mean("rep_mbps") for a in tx.values()) * 1e6
        # Requests that actually landed, counted by the hosts that received
        # them: the ground truth the senders cannot see.
        fwd_pps = sum(a.mean("pps") for a in srv.values())

    # avg is the mean across flows; p50/p99 are the worst flow's, because
    # a fleet-wide percentile of percentiles would hide the sick pair.
    rtt_n = [a for a in tx.values() if a.mean("rtt_avg_us") > 0]
    rtt_avg = sum(a.mean("rtt_avg_us") for a in rtt_n) / len(rtt_n) if rtt_n else 0
    rtt_p50 = max([a.mean("rtt_p50_us") for a in tx.values()] or [0])
    rtt_p99 = max([a.mean("rtt_p99_us") for a in tx.values()] or [0])
    rtt_max = max([a.peak("rtt_max_us") for a in tx.values()] or [0])

    log("")
    log("mx summarize -- last %ds, %d hosts, %d flows, %d bytes out -> %d bytes back"
        % (args.window, len(m.hosts), len(tx), tx_size, rx_size))
    if layered:
        log("layered -- %d layers of %d peers rotating every %gs; rates are "
            "time averages over the window"
            % (m.layering.layers, m.layering.peers, m.layering.dwell))
    log("")
    log("  REQUESTS  %14s   %12s wire   %12s payload"
        % (fmt_pps(tx_pps), fmt_gbps(wire_bps(tx_pps, tx_size)), fmt_gbps(tx_bps)))
    log("  DELIVERED %14s   %s of what was sent"
        % (fmt_pps(fwd_pps), "%.2f%%" % pct(fwd_pps, tx_pps) if tx_pps else "-"))
    log("  REPLIES   %14s   %12s wire   %12s payload"
        % (fmt_pps(rep_pps), fmt_gbps(wire_bps(rep_pps, rx_size)), fmt_gbps(rep_bps)))
    if target:
        log("  TARGET    %14s   %.1f%% achieved" % (fmt_pps(target), pct(tx_pps, target)))
    total_wire = wire_bps(tx_pps, tx_size) + wire_bps(rep_pps, rx_size)
    log("  TOTAL     %14s   %12s wire, both directions"
        % (fmt_pps(tx_pps + rep_pps), fmt_gbps(total_wire)))
    loss = max(0.0, 100.0 - pct(rep_pps, tx_pps)) if tx_pps else 0.0
    fwd_loss = max(0.0, 100.0 - pct(fwd_pps, tx_pps)) if tx_pps else 0.0
    log("  LOSS      %13.2f%%   round trip (%.2f%% forward, %.2f%% on the way back)"
        % (loss, fwd_loss, max(0.0, loss - fwd_loss)))
    log("  RTT       avg %s over all flows; worst flow p50 %s  p99 %s  max %s"
        % (fmt_us(rtt_avg), fmt_us(rtt_p50), fmt_us(rtt_p99), fmt_us(rtt_max)))

    # Per host, worst delivery first.
    stats = _per_host(tx, srv, hostagg, nticks, layered)

    log("")
    # "agent" is the agent process's own CPU as a share of ONE core --
    # near 100% means the agent is the ceiling, whatever the box shows.
    # "egress" is everything the host puts on the wire: its own requests
    # plus the replies it owes the hosts that call it.
    log("  %-20s %11s %11s %11s %10s %8s %9s %6s %7s %6s"
        % ("host", "sent", "back", "serving", "egress", "loss", "rtt p99",
           "cpu", "1 core", "agent"))
    ranked = sorted(stats.items(),
                    key=lambda kv: (pct(kv[1]["back"], kv[1]["sent"])
                                    if kv[1]["sent"] else 100.0))
    for host, h in ranked[:args.top_hosts]:
        ha = hostagg.get(host)
        egress = wire_bps(h["sent"], tx_size) + wire_bps(h["served_rep"],
                                                         rx_size)
        log("  %-20s %11s %11s %11s %10s %7.2f%% %9s %5s %6s %5s"
            % (host, fmt_pps(h["sent"]), fmt_pps(h["back"]),
               fmt_pps(h["served"]), fmt_gbps(egress),
               max(0.0, 100.0 - pct(h["back"], h["sent"])) if h["sent"] else 0.0,
               fmt_us(h["rtt_p99"]),
               "%.0f%%" % ha.mean("cpu_pct") if ha else "-",
               "%.0f%%" % ha.mean("cpu_max_pct") if ha else "-",
               "%.0f%%" % ha.mean("agent_cpu_pct") if ha else "-"))
    if len(ranked) > args.top_hosts:
        log("  ... %d more hosts (raise --top-hosts)" % (len(ranked) - args.top_hosts))

    # Worst flows: split loss into the outbound and return legs, which is
    # what tells a sick receiver apart from a sick sender.
    flows = []
    for (host, peer), a in tx.items():
        sent = a.mean("pps")
        if sent <= 0:
            continue
        if layered:
            # Ratio of totals, not of per-row means: the replies still in
            # flight at a layer switch land in a drain row with nothing
            # sent against them, and only the totals put the two halves
            # of that boundary back together.
            tot_tx = a.sums.get("pps", 0.0)
            frac = pct(a.sums.get("rep_pps", 0.0), tot_tx) if tot_tx else 0.0
            back = sent * frac / 100.0
            arrived = None
            if (peer, host) in srv and tot_tx:
                arrived = sent * (srv[(peer, host)].sums.get("pps", 0.0)
                                  / tot_tx)
        else:
            back = a.mean("rep_pps")
            frac = pct(back, sent)
            arrived = srv[(peer, host)].mean("pps") if (peer, host) in srv \
                else None
        flows.append((frac, host, peer, sent, back, arrived,
                      a.mean("target_pps"), a.mean("rtt_p99_us")))
    flows.sort()
    bad = [f for f in flows if f[0] < 99.5]
    if bad:
        log("")
        log("  WORST FLOWS (of %d; delivery = replies back / requests sent)" % len(flows))
        for frac, host, peer, sent, back, arrived, tgt, p99 in bad[:args.top]:
            extra = ""
            if arrived is not None and sent > 0:
                extra = "  forward %.1f%%" % pct(arrived, sent)
            log("    %s -> %s: %s sent, %s back (%.1f%%)%s  p99 %s"
                % (host, peer, fmt_pps(sent), fmt_pps(back), frac, extra, fmt_us(p99)))
        if len(bad) > args.top:
            log("    ... %d more lossy flows (raise --top)" % (len(bad) - args.top))
    else:
        log("")
        log("  every flow is delivering >= 99.5% of its replies")

    coverage = None
    if layered:
        lay = m.layering
        # Coverage is cumulative over the WHOLE report history, not the
        # --window: the window says how the run is going, coverage says
        # how far around the rotation it has come.
        counts = {}
        for r in rows:
            if r.get("dir") != "tx" or not r.get("pps"):
                continue
            try:
                if float(r["pps"]) <= 0:
                    continue
            except ValueError:
                continue
            k = (r["host"], r["peer"])
            counts[k] = counts.get(k, 0) + 1
        total = len(m.hosts) * (len(m.hosts) - 1)
        measured = len(counts)
        coverage = (measured, total, lay.cycle())
        log("")
        log("  COVERAGE  %d of %d ordered pairs measured so far (%.1f%%)"
            % (measured, total, pct(measured, total)))
        log("            %d layers x %gs dwell = one full cycle every %s"
            % (lay.layers, lay.dwell, fmt_secs(lay.cycle())))
        if measured < total:
            missing = []
            for s in m.hosts:
                for d in m.hosts:
                    if s != d and (s, d) not in counts:
                        missing.append("%s->%s" % (s, d))
                    if len(missing) >= 4:
                        break
                if len(missing) >= 4:
                    break
            rest = total - measured - len(missing)
            log("            still unmeasured: %s%s"
                % (", ".join(missing),
                   " (+%d more)" % rest if rest > 0 else ""))
        if args.grid:
            os.makedirs(args.grid, exist_ok=True)
            path = os.path.join(args.grid, "coverage_grid.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["src\\dst"] + m.hosts)
                for s in m.hosts:
                    w.writerow([s] + ["" if s == d else
                                      (counts.get((s, d), "") or "")
                                      for d in m.hosts])
            log("")
            log("  wrote %s (cells: intervals measured; empty = never yet)"
                % path)

        # What the window saw, layer by layer. `sent` averages over the
        # intervals the layer was actually active; delivery is the ratio
        # of totals, so drain-row tails count for their own layer.
        per_layer = {}
        for r in recent:
            if r.get("dir") != "tx" or r.get("layer") in (None, ""):
                continue
            try:
                lj = int(float(r["layer"]))
            except ValueError:
                continue
            tss, pairs, agg = per_layer.setdefault(lj, (set(), set(), Agg()))
            if r.get("pps"):
                tss.add(r["ts"])
                pairs.add((r["host"], r["peer"]))
            agg.add(r, ["pps", "rep_pps", "rtt_p99_us"])
        if per_layer:
            log("")
            log("  LAYERS in the window: %d of %d" % (len(per_layer), lay.layers))
            log("  %7s %8s %12s %10s %10s"
                % ("layer", "pairs", "sent", "delivery", "worst p99"))
            for shown, lj in enumerate(sorted(per_layer)):
                if shown >= 10 and len(per_layer) > 11:
                    log("  ... %d more layers" % (len(per_layer) - shown))
                    break
                tss, pairs, agg = per_layer[lj]
                tot = agg.sums.get("pps", 0.0)
                sent = tot / max(1, len(tss))
                dlv = pct(agg.sums.get("rep_pps", 0.0), tot) if tot else 0.0
                log("  %7d %8d %12s %9.2f%% %10s"
                    % (lj, len(pairs), fmt_pps(sent), dlv,
                       fmt_us(agg.peak("rtt_p99_us"))))

    if args.grid:
        _write_grids(args.grid, m, tx, srv)

    # How many peers each host is a client for -- the cap on how many
    # workers it can ever put to work.
    flowcount = dict((host, h["peers"]) for host, h in stats.items())
    _summary_hints(rx_size, hostagg, flowcount, target, tx_pps, rep_pps,
                   fwd_pps, rtt_p50, rtt_p99, coverage)
    return 0


def _write_grids(gdir, m, tx, srv):
    os.makedirs(gdir, exist_ok=True)
    hosts = m.hosts
    grids = {
        "pps_grid.csv": lambda s, d: tx[(s, d)].mean("pps") if (s, d) in tx else None,
        "delivered_grid.csv": lambda s, d: (srv[(d, s)].mean("pps")
                                            if (d, s) in srv else None),
        "loss_grid.csv": lambda s, d: (max(0.0, 100.0 - pct(tx[(s, d)].mean("rep_pps"),
                                                            tx[(s, d)].mean("pps")))
                                       if (s, d) in tx and tx[(s, d)].mean("pps") else None),
        "rtt_p99_grid.csv": lambda s, d: (tx[(s, d)].mean("rtt_p99_us")
                                          if (s, d) in tx else None),
    }
    for name, cell in grids.items():
        with open(os.path.join(gdir, name), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["src\\dst"] + hosts)
            for s in hosts:
                row = [s]
                for d in hosts:
                    v = None if s == d else cell(s, d)
                    row.append("" if v is None else "%.1f" % v)
                w.writerow(row)
    log("")
    log("  wrote %s/{%s}" % (gdir, ",".join(sorted(grids))))


def _summary_hints(rx_size, hostagg, flowcount, target, tx_pps, rep_pps,
                   fwd_pps, rtt_p50, rtt_p99, coverage=None):
    """Turn what the numbers say into what to do next."""
    hints = []
    if coverage:
        measured, total, cycle = coverage
        if measured < total:
            hints.append("the rotation has measured %d of %d ordered pairs "
                         "(%.1f%%). A full cycle takes %s -- keep the run "
                         "alive at least that long (mx run --for %d) and "
                         "every pair gets its turn."
                         % (measured, total, pct(measured, total),
                            fmt_secs(cycle), int(cycle + cycle // 10)))
    hot = [(h, a.mean("cpu_max_pct")) for h, a in hostagg.items()
           if a.mean("cpu_max_pct") >= 85.0]
    # Each worker is one Python process, so the GIL caps it near 100% of a
    # single core. The busiest worker, not the box-wide figure, is what
    # says "you are measuring the tool, not the network".
    busy_agents = [(h, a.mean("agent_cpu_pct")) for h, a in hostagg.items()
                   if a.mean("agent_cpu_pct") >= 75.0]
    short = target and tx_pps < target * 0.95
    fwd_loss = max(0.0, 100.0 - pct(fwd_pps, tx_pps)) if tx_pps else 0.0
    ret_loss = max(0.0, pct(fwd_pps, tx_pps) - pct(rep_pps, tx_pps)) if tx_pps else 0.0

    if busy_agents:
        worst = max(busy_agents, key=lambda kv: kv[1])
        agg = hostagg[worst[0]]
        workers = int(agg.mean("workers") or 1)
        peers = flowcount.get(worst[0], 0)
        lead = ("sending %.0f%% of target, and " % pct(tx_pps, target)
                if short else "")
        hints.append("%sthe busiest agent worker on %s is at %.0f%% of a core: "
                     "that worker, not the fabric, is the ceiling. One worker "
                     "is one python process and one GIL, so it stops near 100%% "
                     "of a single core (~%s of request+reply)."
                     % (lead, worst[0], worst[1], fmt_pps(WORKER_PPS)))
        # Workers cannot outnumber flows: inbound requests are spread by
        # hashing the 4-tuple, so a mesh with few peers has few tuples to
        # spread however many cores the box has. --streams is the lever.
        if workers <= peers + 1:
            hints.append("%s is running %d worker(s) for %d peer(s), which is "
                         "the most it can use: workers are capped by the number "
                         "of flows, because each pair is one socket and one "
                         "4-tuple. Add `mx start --streams 8` -- it splits each "
                         "pair's rate across 8 sockets, so the load spreads over "
                         "8x the workers, NIC receive queues and ECMP paths. The "
                         "offered rate does not change."
                         % (worst[0], workers, peers))
        else:
            hints.append("raise `mx start --workers N` while %s has spare cores; "
                         "past that, add hosts or lower --pps." % worst[0])
    elif short and hot:
        hints.append("sending %.0f%% of target and %d host(s) have a core at "
                     ">=85%% (%s): something on those boxes is CPU bound, and "
                     "it is not the network. Lower --pps or spread the load "
                     "over more hosts."
                     % (pct(tx_pps, target), len(hot),
                        ", ".join(h for h, _ in hot[:3])))
    elif short:
        hints.append("sending only %.0f%% of the target rate with CPU to "
                     "spare: a flow may have failed to open (check `mx logs`), "
                     "or the run was too short for the first interval's "
                     "startup cost to average out -- try a longer `--for`."
                     % pct(tx_pps, target))
    if fwd_loss > 1.0 and not (hot or busy_agents):
        hints.append("%.1f%% of requests never arrive and nothing is CPU bound: "
                     "that is the fabric dropping. Re-run with a lower --pps to "
                     "find the knee, or `mx summarize --grid g` and look for a "
                     "dark row (sick sender) or column (sick receiver)." % fwd_loss)
    elif fwd_loss > 1.0:
        hints.append("%.1f%% of requests never arrive while CPU is pegged: the "
                     "receiver is dropping them in software before the fabric "
                     "gets the blame. Try `mx start --workers 4` (or more) and "
                     "`--rcvbuf 16777216`." % fwd_loss)
    if ret_loss > 1.0:
        hints.append("%.1f%% of the loss is on the reply path only. With "
                     "rx_size=%d that direction carries %s -- if that is more "
                     "than the forward direction, the return leg is the "
                     "bottleneck; shrink --rx-size."
                     % (ret_loss, rx_size,
                        fmt_gbps(wire_bps(rep_pps, rx_size))))
    if rtt_p50 and rtt_p99 > rtt_p50 * 5 and rtt_p99 > 1000:
        hints.append("p99 (%s) is far above p50 (%s): queues are building "
                     "somewhere. Lower the rate until the tail collapses -- that "
                     "point is the fabric's usable packet rate."
                     % (fmt_us(rtt_p99), fmt_us(rtt_p50)))
    if not hints:
        if target and tx_pps >= target * 0.98 and pct(rep_pps, tx_pps) >= 99.5:
            hints.append("everything is landing at the target rate. Raise --pps "
                         "(or use `mx gen --pps max`) and re-run until loss or "
                         "the p99 tail appears -- that is the fabric's limit.")
        else:
            hints.append("nothing looks wrong. `mx hints` lists the usual goals "
                         "and the command for each.")
    log("")
    log("  WHAT TO DO NEXT")
    for h in hints:
        _wrap("    * ", h)


def _wrap(prefix, text, width=78):
    for line in textwrap.wrap(text, width=width, initial_indent=prefix,
                              subsequent_indent=" " * len(prefix)):
        log(line)


# ---------------------------------------------------------------------------
# export: a run as an overlay for the datacenter layout viewer
# ---------------------------------------------------------------------------
#
# The viewer (github.com/MartinGallagher-code/datacenter_visualization) draws
# a floor from a `.dc` layout and colours it from a results file: one sample
# per line,
#
#     <test>  <target>  <value>  [key=value ...]
#
# tab separated, append-only, with `!test` lines carrying each overlay's
# units and palette direction. `mx export >> results.tsv` after every run is
# the whole integration; `--json` writes the same samples as NDJSON for a
# pipeline rather than a person.
#
# The numbers are the ones `mx summarize` prints, computed here, by the
# report's own rules: a blank cell is "not measured" and never zero, a
# layered run's rates come from the host rows because most pairs are idle
# most of the window, loss is a ratio of totals rather than a mean of
# ratios, and latency is the worst peer's rather than a percentile of
# percentiles. That is why this is an export and not somebody else's
# importer -- a tool reading reports/*.csv from outside cannot know any of
# it, and every one of those rules is the difference between a number and a
# flattering number.

# (test, `!test` metadata). One sample per host per window, from the
# aggregates `mx summarize` prints.
EXPORT_HOST_TESTS = [
    ("pps", 'unit=pps higher=good decimals=0 short=PPS label="Requests sent"'),
    ("rep_pps", 'unit=pps higher=good decimals=0 short=REP label="Replies received"'),
    ("served_pps", 'unit=pps higher=good decimals=0 short=SRV label="Requests arriving here"'),
    ("request_gbps", 'unit=Gb/s higher=good decimals=2 short=REQ label="Requests on the wire"'),
    ("egress_gbps", 'unit=Gb/s higher=good decimals=2 short=EGR label="Egress on the wire"'),
    # Only with --nic-gbps. The reading a relative overlay cannot give: when
    # the whole fleet is at half its NICs' rate, every host sits at 100% of a
    # median that is itself wrong, and only an absolute scale shows it.
    ("line_util", 'unit=% higher=good min=0 max=100 agg=median decimals=0 short=UTIL label="Egress vs NIC line rate"'),
    # Each host against the fleet's own median, on a diverging ramp pinned at
    # 0-200%: 100% is "normal for this fabric", and no one has to know what
    # good looks like on this hardware. It is the only relative reading an
    # unpaced run has, since `max` leaves nothing to be achieved against.
    #
    # agg=median, and the choice carries the meaning. Every host in a mesh
    # talks to the sick host, so `min` reddens the whole floor and hides it;
    # a host that is itself slow has all of its flows slow, so its median
    # drops while a healthy host with one bad peer stays near 100%. That is
    # "I am slow" against "I have a slow peer".
    ("rel_median", 'unit=% higher=good palette=rdbu min=0 max=200 agg=median decimals=0 short=REL label="Packet rate vs fleet median"'),
    ("loss", 'unit=% higher=bad decimals=2 short=LOSS label="Round-trip loss"'),
    ("forward_loss", 'unit=% higher=bad decimals=2 short=FWD label="Requests lost on the way out"'),
    ("return_loss", 'unit=% higher=bad decimals=2 short=RET label="Replies lost on the way back"'),
    # Diverging around 100% for the same reason as rel_median: on target is
    # the midpoint, not the top of a ramp.
    ("achieved", 'unit=% higher=good palette=rdbu min=0 max=200 agg=median decimals=0 short=ACHV label="Rate achieved vs target"'),
    # agg=min: a rack has come round only when its slowest host has.
    ("coverage", 'unit=% higher=good min=0 max=100 agg=min decimals=0 short=COV label="Peers measured so far"'),
    ("rtt_avg", 'unit=us higher=bad decimals=0 short=AVG label="RTT mean over this host\'s flows"'),
    ("rtt_p50", 'unit=us higher=bad decimals=0 short=P50 label="RTT p50, worst peer"'),
    # Already the worst peer's; agg=max keeps it the worst peer's when a rack
    # is collapsed, rather than averaging the sick pair away.
    ("rtt_p99", 'unit=us higher=bad agg=max decimals=0 short=P99 label="RTT p99, worst peer"'),
    ("rtt_max", 'unit=us higher=bad agg=max decimals=0 short=MAX label="RTT max, worst peer"'),
    # Percentages pinned to the scale they are really on: auto-fitting makes
    # a 30% peak look alarming for no reason but being the highest.
    ("cpu", 'unit=% higher=bad min=0 max=100 decimals=0 short=CPU label="Host CPU"'),
    ("cpu_core", 'unit=% higher=bad min=0 max=100 agg=max decimals=0 short=CORE label="Busiest core"'),
    # Not pinned: this one is a share of ONE core and a multi-core box can
    # report above 100%. agg=max -- one pegged worker is the ceiling for its
    # whole rack, and a mean would bury it.
    ("agent_cpu", 'unit=% higher=bad agg=max decimals=0 short=ACPU label="Busiest mx worker, share of a core"'),
    ("peers", 'higher=good agg=min decimals=0 short=PEER label="Flows this host sends"'),
    ("workers", 'higher=good agg=min decimals=0 short=WRK label="Agent worker processes"'),
    ("intervals", 'higher=good agg=min decimals=0 short=IVL label="Report intervals in the window"'),
    ("state", 'agg=last short=STATE label="Did this host report?"'),
]

# One sample per flow, under their own names so that `max` on a peer
# overlay reads as "the worst peer of this host" -- and so that per-peer
# samples can never be averaged in with the per-host ones.
EXPORT_PEER_TESTS = [
    ("peer_pps", 'unit=pps higher=good agg=mean decimals=0 short=PPS label="Requests sent to one peer"'),
    ("peer_loss", 'unit=% higher=bad agg=max decimals=2 short=LOSS label="Round-trip loss, worst peer"'),
    ("peer_rtt_p99", 'unit=us higher=bad agg=max decimals=0 short=P99 label="RTT p99, worst peer"'),
]

# --raw: the columns a `dir=host` row carries as they stand, one sample per
# reporting interval. The overlays that have no per-interval meaning are
# still written once per host alongside them, so --raw is a superset of the
# default rather than a different set.
EXPORT_RAW_COLUMNS = [
    ("pps", "pps"),
    ("rep_pps", "rep_pps"),
    ("loss", "loss_pct"),
    ("rtt_p50", "rtt_p50_us"),
    ("rtt_p99", "rtt_p99_us"),
    ("cpu", "cpu_pct"),
    ("cpu_core", "cpu_max_pct"),
    ("agent_cpu", "agent_cpu_pct"),
]

EXPORT_META = dict(EXPORT_HOST_TESTS + EXPORT_PEER_TESTS)


# A results line is whitespace separated, and a double quote is how a value
# with a space in it is written -- so neither can appear inside a field.
BAD_IN_FIELD = ' \t\n\r"'


def _fmt_num(value):
    """A number for a results file: four significant digits, fixed
    notation. `%g` would write a megapacket rate as 1.235e+06, which is
    correct and unreadable in a file people grep."""
    text = "%.4g" % value
    if "e" in text or "E" in text:
        text = "%.0f" % value
    return text


def _report_number(cell):
    """A report cell as a float, or None when it is blank or unparseable.

    Blank means "not measured" everywhere in a report, and importing it as
    zero is the one mistake that quietly flatters every number downstream.
    """
    if cell is None:
        return None
    text = str(cell).strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


def _load_names(path):
    """`--names`: mx host name -> the name the layout knows it by.

    One mapping per line, `mxname target`, separated by whitespace or '='.
    Blank lines and '#' comments are ignored, and a host the file does not
    mention keeps its matrix name -- so the file only has to carry the
    exceptions.
    """
    names = {}
    try:
        with open(path) as f:
            for lineno, raw in enumerate(f, 1):
                line = raw.split("#", 1)[0].strip()
                if not line:
                    continue
                parts = line.replace("=", " ").split()
                if len(parts) != 2:
                    die("%s line %d: want `mxname target`, got %r"
                        % (path, lineno, line))
                names[parts[0]] = parts[1]
    except OSError as exc:
        die("cannot read %s: %s" % (path, exc))
    return names


class Overlay(object):
    """The results file being built: `!test` declarations, then samples.

    A test is declared the first time something is filed under it, so an
    overlay never appears in the file with no data behind it.
    """

    def __init__(self, test_prefix="mx_", target_prefix="", names=None,
                 run=None, meta_table=None):
        self.meta_table = EXPORT_META if meta_table is None else meta_table
        self.test_prefix = test_prefix
        self.target_prefix = target_prefix
        self.names = names or {}
        self.run = run
        self.meta = []          # [(test, "key=value ...")]
        self.samples = []       # [(test, target, value, [key=value ...])]
        self._declared = set()

    def target(self, host):
        """The layout's name for an mx host: mapped by --names, then
        prefixed by --target-prefix. The viewer resolves a bare name, a
        path, or any unique tail of a path, so `wr01r01u07`, `R01/u07` and
        `DH1/A/R01/u07` all land on the same node."""
        name = self.target_prefix + self.names.get(host, host)
        if not name or any(c in name for c in BAD_IN_FIELD):
            die("target %r: a results file is whitespace separated and quote "
                "aware, so a target can hold neither (host %r)" % (name, host))
        return name

    def add(self, test, host, value, extras=()):
        """File one sample. `None` is dropped: not measured is not zero."""
        if value is None:
            return
        if not isinstance(value, str):
            if value != value or value in (float("inf"), float("-inf")):
                return
            value = _fmt_num(value)
        name = self.test_prefix + test
        if test not in self._declared:
            self._declared.add(test)
            meta = self.meta_table.get(test)
            if meta:
                self.meta.append((name, meta))
        extras = list(extras)
        if self.run:
            extras.append("run=%s" % self.run)
        self.samples.append((name, self.target(host), value, extras))

    def tsv_lines(self, with_meta=True):
        out = []
        if with_meta:
            for test, meta in self.meta:
                out.append("!test\t%s\t%s" % (test, meta))
        for test, target, value, extras in self.samples:
            out.append("\t".join([test, target, value] + extras))
        return out

    def json_lines(self, with_meta=True):
        """NDJSON: one object per line, so concatenating two runs is still a
        valid file -- which a top-level `[ ... ]` array would not be."""
        out = []
        if with_meta:
            for test, meta in self.meta:
                entry = {"!test": test}
                entry.update(_meta_pairs(meta))
                out.append(json.dumps(entry, sort_keys=True))
        for test, target, value, extras in self.samples:
            number = _report_number(value)
            entry = {"test": test, "target": target,
                     "value": value if number is None else number}
            if extras:
                entry["meta"] = _meta_pairs(" ".join(extras))
            out.append(json.dumps(entry, sort_keys=True))
        return out


def _meta_pairs(text):
    """`unit=us higher=bad label="RTT p50"` -> a dict, quotes removed."""
    pairs = {}
    for token in shlex.split(text):
        if "=" in token:
            key, value = token.split("=", 1)
            pairs[key] = value
    return pairs


def _median(values):
    ordered = sorted(values)
    n = len(ordered)
    if not n:
        return None
    mid = n // 2
    return ordered[mid] if n % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0


def _peer_coverage(rows, m):
    """host -> % of the peers it should measure that it has measured yet.

    Only a layered run needs this: its rotation reaches each peer once per
    cycle, so a floor plan drawn before a full cycle has elapsed is showing
    a partial mesh, and this is the overlay that says which hosts are still
    short. Counted over the whole report history rather than the window --
    coverage is cumulative, unlike every rate here.
    """
    seen = {}
    for r in rows:
        if r.get("dir") != "tx" or not r.get("pps"):
            continue
        try:
            if float(r["pps"]) <= 0:
                continue
        except ValueError:
            continue
        seen.setdefault(r["host"], set()).add(r["peer"])
    total = len(m.hosts) - 1
    if total < 1:
        return {}
    return dict((host, pct(len(peers), total)) for host, peers in seen.items())


def cmd_export(args):
    m = load_matrix(args.matrix) if os.path.isfile(args.matrix) else None
    if m is None:
        if not args.no_collect:
            die("matrix not found: %s -- `mx export --no-collect` exports the "
                "reports already in %s/ without it" % (args.matrix, args.reports))
        # Everything but the roll call survives without it: a host that never
        # reported is only knowable from the list of hosts there should be.
        sys.stderr.write("[mx] export: no %s, so hosts that never reported "
                         "cannot be listed\n" % args.matrix)

    stdout = args.output in ("-", "")
    if not args.no_collect:
        # `mx collect` narrates to stdout, which is where the results file
        # itself goes when there is no -o. Nothing but samples may land there.
        saved = sys.stdout
        if stdout:
            sys.stdout = sys.stderr
        try:
            cmd_collect(args)
        finally:
            sys.stdout = saved

    paths = sorted(os.path.join(args.reports, f)
                   for f in os.listdir(args.reports)
                   if f.endswith(".csv")) if os.path.isdir(args.reports) else []
    if not paths:
        die("no reports in %s/ -- is the run started? (mx status)" % args.reports)
    rows = _read_reports(paths)
    if not rows:
        die("reports are empty -- give the agents an interval or two, then retry")

    latest = max(int(float(r["ts"])) for r in rows)
    if args.window > 0:
        recent = [r for r in rows if int(float(r["ts"])) >= latest - args.window]
    else:
        recent = rows          # --window 0: the whole history
    if not recent:
        die("no rows in the last %ds -- widen --window, or --window 0 for "
            "everything the reports hold" % args.window)

    # The matrix is the authority on whether a run is layered; without one,
    # the layer column says so just as well.
    layered = (m.layering is not None) if m else any(r.get("layer") for r in recent)
    tx, srv, hostagg = _aggregate(recent)
    if not tx and not hostagg:
        die("nothing measured in the window -- are the agents still running? "
            "(mx status)")
    nticks = {h: a.counts.get("pps", 0) for h, a in hostagg.items()}
    tx_size, rx_size = _report_sizes(
        recent, m.tx_size if m else 64, m.rx_size if m else 64)
    stats = _per_host(tx, srv, hostagg, nticks, layered)

    if args.run and any(c in args.run for c in BAD_IN_FIELD):
        die("--run %r: the label is written onto every sample line, so it "
            "can hold no whitespace or quotes" % args.run)

    meta_table = dict(EXPORT_META)
    if args.nic_gbps:
        # A known line rate turns the throughput overlays from relative
        # shading into absolute readings: without it the viewer fits the ramp
        # to whatever the run produced, so a floor uniformly at half speed
        # still paints comfortably green.
        for test, top in (("request_gbps", args.nic_gbps),
                          ("egress_gbps", args.nic_gbps)):
            meta_table[test] = "%s min=0 max=%g" % (meta_table[test], top)

    out = Overlay(meta_table=meta_table,
                  test_prefix=args.test_prefix,
                  target_prefix=args.target_prefix,
                  names=_load_names(args.names) if args.names else None,
                  run=args.run)

    forward = _forward_delivery(stats, srv, nticks, layered)
    coverage = _peer_coverage(rows, m) if (layered and m) else {}
    # The fleet's own median rate, which is what makes a host's number
    # readable without knowing the hardware. One host is not a fleet, and a
    # median of one would paint it a confident 100%.
    rates = [h["sent"] for h in stats.values() if h["sent"] > 0]
    fleet_median = _median(rates) if len(rates) > 1 else None

    if args.raw:
        for r in recent:
            if r.get("dir") != "host":
                continue
            extras = ["ts=%s" % r["ts"]]
            for test, column in EXPORT_RAW_COLUMNS:
                out.add(test, r["host"], _report_number(r.get(column)), extras)
            target_pps = _report_number(r.get("target_pps"))
            sent = _report_number(r.get("pps"))
            if target_pps and sent is not None:
                out.add("achieved", r["host"], pct(sent, target_pps), extras)

    for host in sorted(stats):
        h = stats[host]
        agg = hostagg.get(host)
        if not args.raw:
            out.add("pps", host, h["sent"])
            out.add("rep_pps", host, h["back"])
            if h["sent"] > 0:
                out.add("loss", host, max(0.0, 100.0 - pct(h["back"], h["sent"])))
            if h["target"] > 0:
                out.add("achieved", host, pct(h["sent"], h["target"]))
            for test in ("rtt_avg", "rtt_p50", "rtt_p99", "rtt_max"):
                if h[test] > 0:
                    out.add(test, host, h[test])
            if agg:
                for test, column in (("cpu", "cpu_pct"),
                                     ("cpu_core", "cpu_max_pct"),
                                     ("agent_cpu", "agent_cpu_pct")):
                    if agg.counts.get(column):
                        out.add(test, host, agg.mean(column))

        # Derived from more than one row, so they have no per-interval
        # meaning and are written once per host in either mode.
        if h["sent"] > 0:
            # What this host's own requests put on the wire, framing
            # included: known from its own rows alone.
            out.add("request_gbps", host, wire_bps(h["sent"], tx_size) / 1e9)
        if fleet_median:
            out.add("rel_median", host, pct(h["sent"], fleet_median))
        if h["served_seen"]:
            out.add("served_pps", host, h["served"])
            # Everything the host puts on the wire: its requests plus the
            # replies it owes the hosts that call it. The number a NIC has
            # to carry -- and only a number at all once the reply half has
            # been measured, which is what served_seen says.
            egress = (wire_bps(h["sent"], tx_size)
                      + wire_bps(h["served_rep"], rx_size)) / 1e9
            out.add("egress_gbps", host, egress)
            if args.nic_gbps:
                # Utilisation is of everything the NIC carries, so it waits
                # on the reply half like egress does rather than reporting a
                # comfortable half-truth from the request side alone.
                out.add("line_util", host, pct(egress, args.nic_gbps))
        arrived = forward.get(host)
        if arrived is not None and h["sent"] > 0:
            # The split that tells a sick sender from a sick receiver:
            # what never arrived, and what arrived but never came back.
            fwd = max(0.0, 100.0 - pct(arrived, h["sent"]))
            out.add("forward_loss", host, fwd)
            out.add("return_loss", host,
                    max(0.0, pct(arrived, h["sent"]) - pct(h["back"], h["sent"])))
        if host in coverage:
            out.add("coverage", host, coverage[host])
        out.add("peers", host, float(h["peers"]))
        out.add("intervals", host, float(nticks.get(host, 0)))
        if agg and agg.counts.get("workers"):
            out.add("workers", host, agg.mean("workers"))

    # Which hosts were heard from, in either mode -- and how a host failed
    # to be, which is not one thing. A host that reported earlier in the run
    # and nothing inside the window went quiet while the run was going; a
    # host with no row anywhere never started. Those need different people.
    for host in sorted(stats):
        out.add("state", host, "REPORTING")
    ever = set(r["host"] for r in rows if r.get("host"))
    silent = sorted(h for h in ever if h not in stats)
    for host in silent:
        out.add("state", host, "SILENT")

    if args.peers:
        pair_layer = {}
        for r in recent:
            if r.get("dir") == "tx" and r.get("layer"):
                pair_layer.setdefault((r["host"], r["peer"]), r["layer"])
        for (host, peer) in sorted(tx):
            a = tx[(host, peer)]
            extras = ["peer=%s" % out.target(peer)]
            layer = pair_layer.get((host, peer))
            if layer:
                extras.append("layer=%s" % layer)
            sent = a.mean("pps")
            out.add("peer_pps", host, sent if sent > 0 else None, extras)
            if layered:
                # A ratio of totals, not of per-row means: the replies still
                # in flight at a layer switch land in a drain row with
                # nothing sent against them, and only the totals put the two
                # halves of that boundary back together.
                total = a.sums.get("pps", 0.0)
                delivered = pct(a.sums.get("rep_pps", 0.0), total) if total else None
            else:
                delivered = pct(a.mean("rep_pps"), sent) if sent > 0 else None
            if delivered is not None:
                out.add("peer_loss", host, max(0.0, 100.0 - delivered), extras)
            p99 = a.mean("rtt_p99_us")
            out.add("peer_rtt_p99", host, p99 if p99 > 0 else None, extras)

    # Hosts the matrix knows about that said nothing in the window: the one
    # thing a results file can show that a report cannot, because a host
    # with no report has no row to carry it.
    missing = []
    if m:
        for host in m.hosts:
            if host not in stats and host not in ever:
                out.add("state", host, "NO-DATA")
                missing.append(host)

    if not out.samples:
        die("nothing to export from %s/ -- no measured rows in the window"
            % args.reports, code=1)

    # What a reader needs to know a month later, when this file holds
    # thirty runs: what the numbers are of, and what was missing from them.
    stamp = lambda t: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
    earliest = min(int(float(r["ts"])) for r in recent)
    header = [
        "# mx %s export -- %d host(s), %s, %d sample(s)"
        % (VERSION, len(stats),
           "whole history" if args.window <= 0 else "last %ds" % args.window,
           len(out.samples)),
        "# run shape: %d bytes out -> %d bytes back%s"
        % (tx_size, rx_size,
           ", %d layers x %gs dwell" % (m.layering.layers, m.layering.dwell)
           if (m and m.layering) else ""),
        "# window covers %s .. %s" % (stamp(earliest), stamp(latest)),
    ]
    if args.nic_gbps:
        header.append("# NIC line rate %g Gb/s: mx_line_util included"
                      % args.nic_gbps)
    if silent:
        header.append("# %d host(s) went quiet before the window: %s"
                      % (len(silent), " ".join(silent[:8])))
    if missing:
        header.append("# %d host(s) in the matrix never reported at all: %s"
                      % (len(missing), " ".join(missing[:8])))
    lines = (header +
             (out.json_lines(not args.no_meta) if args.json
              else out.tsv_lines(not args.no_meta)))

    if stdout:
        for line in lines:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
    else:
        with open(args.output, "a" if args.append else "w") as f:
            for line in lines:
                f.write(line + "\n")

    tests = sorted(set(t for t, _target, _v, _e in out.samples))
    sys.stderr.write("[mx] export: %d sample(s) over %d overlay(s) on %d "
                     "host(s) -> %s\n"
                     % (len(out.samples), len(tests), len(stats),
                        "stdout" if stdout else args.output))
    # An overlay is left out when its number was not measured, so say which
    # and why: a sparse overlay is this working, not a bug to hunt.
    no_rx = [h for h in sorted(stats) if not stats[h]["served_seen"]]
    if no_rx:
        sys.stderr.write("[mx] export: %d host(s) have no rx rows, so "
                         "mx_served_pps and mx_egress_gbps are omitted for "
                         "them rather than exported as zero: %s\n"
                         % (len(no_rx), " ".join(no_rx[:8])))
    no_split = [h for h in sorted(stats)
                if forward.get(h) is None and stats[h]["sent"] > 0]
    if no_split:
        sys.stderr.write("[mx] export: %d host(s) have a peer that reported "
                         "nothing, so the forward/return loss split is "
                         "omitted rather than counted short: %s\n"
                         % (len(no_split), " ".join(no_split[:8])))
    if silent:
        sys.stderr.write("[mx] export: %d host(s) reported earlier but not in "
                         "the window (mx_state SILENT): %s\n"
                         % (len(silent), " ".join(silent[:8])))
    if missing:
        sys.stderr.write("[mx] export: %d host(s) in the matrix never reported "
                         "(mx_state NO-DATA): %s\n"
                         % (len(missing), " ".join(missing[:8])))
    return 0


# ---------------------------------------------------------------------------
# run: the whole thing, one command
# ---------------------------------------------------------------------------

def cmd_run(args):
    rc = cmd_start(args)
    if rc != 0:
        return rc
    wait = args.duration if args.duration > 0 else args.for_seconds
    log("")
    log("[mx] running for %gs -- ctrl-c stops the agents and summarizes early"
        % wait)
    m = load_matrix(args.matrix)
    if m.layering and wait < m.layering.cycle():
        log("[mx] note: %gs is less than one full rotation (%s) -- some "
            "ordered pairs will not be measured this run; the summary's "
            "COVERAGE section says which"
            % (wait, fmt_secs(m.layering.cycle())))
    end = time.monotonic() + wait
    try:
        while time.monotonic() < end:
            time.sleep(min(5.0, max(0.0, end - time.monotonic())))
    except KeyboardInterrupt:
        log("")
        log("[mx] interrupted -- summarizing what ran so far")
    args.window = max(args.window, int(min(wait, 300)))
    try:
        cmd_summarize(args)
    finally:
        if not args.keep:
            log("")
            cmd_stop(args)
    return 0


def cmd_doctor(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    log("[mx] local checks")
    log("  python      %d.%d.%d" % sys.version_info[:3])
    for tool in ("ssh", "scp"):
        found = any(os.access(os.path.join(p, tool), os.X_OK)
                    for p in os.environ.get("PATH", "").split(os.pathsep) if p)
        log("  %-11s %s" % (tool, "found" if found else "MISSING"))
    log("  matrix      %s: %d hosts, %d flows, %d -> %d bytes, port %d"
        % (m.path, len(m.hosts), len(m.rates), m.tx_size, m.rx_size, m.port))

    # Every flow is a socket. The agent raises its own SOFT fd limit at
    # startup (no privilege needed), so the only real constraint is the
    # HARD limit -- checked on the hosts, where the sockets open.
    need_fds = len(m.hosts) - 1 + 64      # flows + listeners/report/headroom
    script = """
py=$({py} -V 2>&1 || echo 'MISSING python')
running=no
{pgrep} >/dev/null 2>&1 && running=yes
fds=$(ulimit -Hn 2>/dev/null || echo ?)
low=""
[ "$fds" != unlimited ] && [ "$fds" -lt {need} ] 2>/dev/null \
    && low=" FDS-TOO-LOW(need>{need})"
echo "$py; cores=$(nproc 2>/dev/null || echo ?); nofile_hard=$fds$low; agent_running=$running"
""".format(py=shlex.quote(fleet.python), pgrep=PGREP, need=need_fds)
    log("")
    failed = fleet.each(lambda h: fleet.sh(h, script, timeout=30),
                        "checking hosts (ssh + python + fd limit)")
    if failed:
        log("[mx] fix ssh/python on those hosts first: key-based ssh must work "
            "non-interactively (ssh-copy-id) and `%s` must exist." % fleet.python)
        return 1
    if need_fds > 512:
        log("[mx] every peer is one socket (~%d per agent here, more with "
            "--streams). The agent raises its own soft fd limit, so only a "
            "host marked FDS-TOO-LOW needs an admin: raise the HARD limit "
            "above %d (limits.conf / systemd LimitNOFILE) before start."
            % (len(m.hosts) - 1, need_fds))
    log("[mx] fleet looks ready: mx start")
    return 0


# ---------------------------------------------------------------------------
# hints
# ---------------------------------------------------------------------------

CHEATSHEET = """\
mx hints -- what you want, and the command that gets it

  RUN SOMETHING RIGHT NOW
    printf '%s\\n' host1 host2 host3 > servers.txt
    mx gen --servers servers.txt --pps 20000
    mx run --for 60
      start everywhere, hold 20k requests/sec per pair for a minute,
      summarize, stop. Nothing is left running.

  SMALL PACKETS, HIGH PACKET RATE (the usual fabric torture test)
    mx gen --servers servers.txt --pps max --tx-size 64 --rx-size 64
    mx start --workers auto
      64-byte requests answered by 64-byte replies, unpaced. Packet rate
      is the metric; bandwidth will be tiny and that is the point.
      `--workers auto` is the default and runs one process per core.

  ASYMMETRIC REQUEST/RESPONSE (small ask, big answer)
    mx gen --servers servers.txt --pps 5000 --tx-size 128 --rx-size 8192
      Emulates an RPC/read workload: every 128-byte request comes back as
      8 KB. Watch the reply direction saturate first.

  A FIXED BANDWIDTH INSTEAD OF A FIXED PACKET RATE
    mx gen --servers servers.txt --gbps 10 --tx-size 1400
      Sizes the per-pair rate so each host offers 10 Gbps of requests on
      the wire, framing included.

  FIND THE FABRIC'S LIMIT
    mx gen --servers servers.txt --pps 50000 && mx run --for 120
    mx gen --servers servers.txt --pps 100000 && mx run --for 120
      Double until loss appears or the p99 tail lifts off p50. The last
      clean rate is the sustainable all-to-all packet rate. `mx summarize
      --grid g` writes the per-pair grids so you can see which pairs gave
      out first.

  SUSTAINED EQUAL LOAD ON EVERY SERVER, WITHOUT THE FULL MESH
    mx gen --servers servers.txt --peers 8 --pps 100000
      Every host sends to and receives from exactly 8 randomly-shuffled
      others -- all hosts loaded identically and simultaneously, held
      until you stop it, exactly like the full mesh, but with 8 sockets
      per host instead of N-1. At random nearly all flows cross the
      spine, so the fabric still carries the same aggregate. The seed is
      printed and stored in matrix.csv; re-run with --seed to replay the
      same shuffle, or without it to re-roll every path. Combine with
      --streams to multiply the ECMP tuples per flow.

  ONLY SOME PAIRS, OR DIFFERENT RATES PER PAIR
    mx gen ... then edit matrix.csv by hand.
      It is a plain grid: rows send, columns receive, cells are
      packets/sec. Blank a cell to remove that flow, write `max` to let
      that one pair run unpaced. Re-run `mx start` to apply.

  PIN THE TRAFFIC TO ONE NIC
    mx start --bind eth1          (or --bind 192.168.50.)

  WATCH IT LIVE
    mx status --watch 5

  SEE WHERE IT HURTS
    mx summarize --grid grids
      pps_grid.csv, delivered_grid.csv, loss_grid.csv, rtt_p99_grid.csv.
      A dark row is a sick sender, a dark column a sick receiver, a dark
      block a congested pair of leaves.

  GET THE LOGS / STOP / LEAVE NO TRACE
    mx logs            # every agent's stdout into logs/
    mx stop            # agents down, files still there
    mx clean --yes     # agents down, /var/tmp/mx gone, nothing left

  ONE CORE PEGGED WHILE THE REST OF THE BOX IDLES
    mx start --streams 8
      The classic small-mesh problem. Each pair is one socket and one
      4-tuple, and everything downstream -- our worker processes, the
      NIC's receive queues, the fabric's ECMP hash -- spreads load by
      hashing that tuple. With 3 peers there are 3 tuples, so 3 cores do
      the work however many the box has. `--streams N` gives each pair N
      sockets instead of one, splitting its rate across them (the offered
      load is unchanged) and so spreading it over N times as many
      workers, queues and paths. Raise --workers to match.

  WHEN THE NUMBERS DISAPPOINT
    Packet rate is CPU work, and one python process is one GIL: a worker
    sustains ~200k packets/sec of request+reply and stops at 100% of one
    core. `--workers auto` runs one per core, capped at 8 -- on a big box
    say `--workers 32` explicitly. Workers can never outnumber flows, so
    on a small mesh raise --streams first. Watch the `agent` column in
    `mx summarize`: near 100% means that worker is the limit, not the
    network. Meanwhile `mx check --nic-gbps 25 --nic-mpps 15` tells you
    whether you asked for something the NICs could never have done.

  SIZE A RUN BEFORE RUNNING IT
    mx hints --servers servers.txt --pps-per-host 2000000 --tx-size 64
"""


def cmd_hints(args):
    if not (args.pps_per_host or args.gbps_per_host or args.pps_per_pair):
        sys.stdout.write(CHEATSHEET)
        return 0
    if not os.path.isfile(args.servers):
        die("sizing a run needs the server list: --servers %s not found" % args.servers)
    tokens = read_server_list(args.servers)
    n = len(tokens)
    tx_size = check_size("--tx-size", args.tx_size)
    rx_size = check_size("--rx-size", args.rx_size)

    if args.pps_per_pair:
        per_pair = args.pps_per_pair
    elif args.pps_per_host:
        per_pair = args.pps_per_host / (n - 1)
    else:
        per_pair = (args.gbps_per_host * 1e9 / ((tx_size + WIRE_OVERHEAD) * 8.0)
                    / (n - 1))
    per_host = per_pair * (n - 1)
    out_bps = wire_bps(per_host, tx_size) + wire_bps(per_host, rx_size)

    log("to get that across %d hosts (%d flows):" % (n, n * (n - 1)))
    log("")
    log("  per pair        %s" % fmt_pps(per_pair))
    log("  per host        %s of requests out, %s of replies back"
        % (fmt_pps(per_host), fmt_pps(per_host)))
    log("                  %s per NIC direction (requests one way, replies "
        "the other, framing included)" % fmt_gbps(out_bps))
    log("  fleet total     %s requests + %s replies"
        % (fmt_pps(per_host * n), fmt_pps(per_host * n)))
    log("")
    log("  mx gen --servers %s --pps %.0f --tx-size %d --rx-size %d"
        % (args.servers, per_pair, tx_size, rx_size))
    log("  mx check --nic-gbps 25 --nic-mpps 15    # your NIC's real numbers")
    log("  mx run --for 60")
    log("")
    # One worker process sustains ~200k packets/sec of request+reply, and
    # a host does both, so size the workers off the combined rate.
    need = int((per_host * 2) / WORKER_PPS) + 1
    if need > 1:
        log("  that needs about %d worker processes per host (one worker "
            "sustains" % need)
        log("  ~%s of request+reply, and each host does both directions):"
            % fmt_pps(WORKER_PPS))
        log("      mx start --workers %d        # needs >= %d cores per host"
            % (need, need))
        if need > MAX_AUTO_WORKERS:
            log("  `--workers auto` would only use %d, so set it explicitly."
                % MAX_AUTO_WORKERS)
    if need > 32:
        log("  heads up: %s per host is a lot to ask of a socket-based agent."
            % fmt_pps(per_host))
        log("  Past a few Mpps per host the kernel's own UDP path is the limit")
        log("  too -- widen the fleet instead, or reach for AF_XDP/DPDK.")
    if out_bps > 25e9:
        log("  heads up: %s per host needs a 40/100G NIC." % fmt_gbps(out_bps))
    log("  a smaller --tx-size raises packets/sec for the same bandwidth;")
    log("  a bigger --rx-size makes the reply direction the heavy one.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_fleet_flags(p):
    p.add_argument("--matrix", default=_env("MX_MATRIX", DEFAULT_MATRIX),
                   help="matrix CSV (default: %(default)s)")
    p.add_argument("--user", default=_env("MX_USER", _env("SSH_USER", "")),
                   help="ssh user (default: your ssh config)")
    p.add_argument("--jobs", type=int, default=int(_env("MX_JOBS", "64")),
                   help="ssh fan-out concurrency (default: %(default)s)")
    p.add_argument("--remote-dir", default=_env("MX_REMOTE_DIR", DEFAULT_REMOTE_DIR),
                   help="working directory on each host (default: %(default)s)")
    p.add_argument("--python", default=_env("MX_PYTHON", "python3"),
                   help="python on the hosts (default: %(default)s)")
    p.add_argument("--ssh", default=_env("MX_SSH", "ssh"), help=argparse.SUPPRESS)
    p.add_argument("--scp", default=_env("MX_SCP", "scp"), help=argparse.SUPPRESS)
    p.add_argument("--dry-run", action="store_true",
                   help="print the ssh/scp commands instead of running them")


def _add_run_flags(p):
    p.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                   help="agent report interval, seconds (default: %(default)s)")
    p.add_argument("--duration", type=float, default=0.0,
                   help="agents exit after N seconds (default: run until stopped)")
    p.add_argument("--workers", default="auto", metavar="N",
                   help="worker processes per host, or 'auto' for one per "
                        "core capped at %d. Packet rate scales with these, "
                        "because one python process is one GIL "
                        "(default: %%(default)s)" % MAX_AUTO_WORKERS)
    p.add_argument("--streams", type=int, default=1, metavar="N",
                   help="sockets per peer (default: 1). Each is its own "
                        "4-tuple, so raising this is what lets workers, NIC "
                        "RSS queues and ECMP paths share a small mesh's "
                        "load; the pair's packet rate is split across them, "
                        "not multiplied")
    p.add_argument("--bind", default=_env("IPERF_BIND", ""), metavar="SPEC",
                   help="pin traffic to a NIC: substring matched against "
                        "`ip -o -4 addr show`, so 'eth1' or '10.0.' both work")
    p.add_argument("--sndbuf", type=int, default=0, metavar="BYTES",
                   help="SO_SNDBUF per flow (default: kernel default)")
    p.add_argument("--rcvbuf", type=int, default=0, metavar="BYTES",
                   help="SO_RCVBUF on the responder (default: 8 MB)")
    p.add_argument("--no-deploy", action="store_true",
                   help="do not copy the agent/matrix, just start what is there")


def _add_report_flags(p):
    p.add_argument("--reports", default=_env("MX_REPORTS", "reports"),
                   help="local directory for collected reports (default: %(default)s)")
    p.add_argument("--window", type=int, default=60,
                   help="seconds of history to average (default: %(default)s)")
    p.add_argument("--top", type=int, default=10, help="worst flows to list")
    p.add_argument("--top-hosts", type=int, default=15, help="hosts to list")
    p.add_argument("--grid", metavar="DIR",
                   help="also write per-pair pps/loss/rtt grid CSVs into DIR")
    p.add_argument("--no-collect", action="store_true",
                   help="summarize the reports already collected, no ssh")


def build_parser():
    ap = argparse.ArgumentParser(
        prog="mx", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Version line first (scripts parse it), then holder, license and the
    # two GPL notice lines — the same layout every provost-suite tool uses.
    ap.add_argument("--version", action="version",
                    version="mx %s\n"
                            "Copyright (C) 2026 Martin J. Gallagher\n"
                            "License: GPL-3.0-or-later "
                            "<https://www.gnu.org/licenses/gpl-3.0.html>\n"
                            "This is free software: you are free to change "
                            "and redistribute it.\n"
                            "There is no warranty, to the extent permitted "
                            "by law." % VERSION)
    sub = ap.add_subparsers(dest="cmd")

    g = sub.add_parser("gen", help="build matrix.csv from a server list")
    g.add_argument("--servers", default=_env("MX_SERVERS", DEFAULT_SERVERS),
                   help="one host per line: name[=addr[:port]] (default: %(default)s)")
    rate = g.add_mutually_exclusive_group()
    rate.add_argument("--pps", type=lambda v: parse_pps(v, "--pps"),
                      help="packets/sec per pair, or 'max' for unpaced "
                           "(default: 10000)")
    rate.add_argument("--gbps", type=float,
                      help="per-host request bandwidth on the wire, split "
                           "evenly across peers")
    g.add_argument("--peers", type=int, metavar="K",
                   help="flows per host: each host sends to and receives from "
                        "exactly K randomly-shuffled others instead of all "
                        "N-1. Same per-host load, exactly, with K sockets "
                        "instead of N-1 (default: full mesh)")
    g.add_argument("--seed", type=int, metavar="N",
                   help="replay a --peers shuffle (default: random, printed "
                        "and stored in the matrix header)")
    g.add_argument("--dwell", type=float, metavar="SECONDS",
                   help="rotate the --peers K matrix through "
                        "ceil((N-1)/K) disjoint layers, holding each for "
                        "SECONDS, so a full cycle measures every ordered "
                        "pair exactly once while only K flows per host are "
                        "ever live. Each agent switches on its own wall "
                        "clock; dwell must be a whole multiple of the "
                        "run's --interval, and 3x the interval is the "
                        "sensible floor")
    g.add_argument("--equal-layers", action="store_true",
                   help="pad the rotation's remainder layer with repeated "
                        "pairs so every layer carries exactly K flows per "
                        "host and the load never dips. The repeated pairs "
                        "are measured twice per cycle, so 'exactly once' "
                        "softens to 'at least once'; a no-op when K "
                        "divides N-1")
    g.add_argument("--tx-size", type=int, default=64, metavar="BYTES",
                   help="request size, %d-%d (default: %%(default)s)" % (HDR_SIZE, MAX_SIZE))
    g.add_argument("--rx-size", type=int, default=64, metavar="BYTES",
                   help="reply size sent back for each request (default: %(default)s)")
    g.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help="UDP port the agents listen on (default: %(default)s)")
    g.add_argument("--output", "-o", default=DEFAULT_MATRIX,
                   help="output file, '-' for stdout (default: %(default)s)")

    c = sub.add_parser("check", help="will the fleet carry this matrix?")
    c.add_argument("--matrix", default=_env("MX_MATRIX", DEFAULT_MATRIX),
                   help="matrix CSV to check (default: %(default)s)")
    c.add_argument("--nic-gbps", type=float, help="per-host NIC bandwidth cap")
    c.add_argument("--nic-mpps", type=float, help="per-host NIC packet-rate cap, Mpps")
    c.add_argument("--top", type=int, default=15, help="hosts to list")

    h = sub.add_parser("hints", help="what you want -> the command for it")
    h.add_argument("--servers", default=_env("MX_SERVERS", DEFAULT_SERVERS),
                   help="server list the sizing math is for (default: %(default)s)")
    h.add_argument("--pps-per-host", type=float, help="size a run for this per-host rate")
    h.add_argument("--pps-per-pair", type=float, help="size a run for this per-pair rate")
    h.add_argument("--gbps-per-host", type=float, help="size a run for this per-host Gbps")
    h.add_argument("--tx-size", type=int, default=64,
                   help="request size the sizing assumes (default: %(default)s)")
    h.add_argument("--rx-size", type=int, default=64,
                   help="reply size the sizing assumes (default: %(default)s)")

    s = sub.add_parser("start", help="deploy the agent and start it everywhere")
    _add_fleet_flags(s)
    _add_run_flags(s)

    st = sub.add_parser("status", help="one line per host: running? how fast?")
    _add_fleet_flags(st)
    st.add_argument("--watch", type=float, default=0, metavar="SECONDS",
                    help="refresh every N seconds")

    sm = sub.add_parser("summarize", help="collect reports, print pps/Gbps/loss/rtt")
    _add_fleet_flags(sm)
    _add_report_flags(sm)

    ex = sub.add_parser("export",
                        help="the run as overlay samples for the datacenter "
                             "layout viewer")
    _add_fleet_flags(ex)
    ex.add_argument("--reports", default=_env("MX_REPORTS", "reports"),
                    help="local directory of collected reports "
                         "(default: %(default)s)")
    ex.add_argument("--window", type=int, default=60,
                    help="seconds of history to reduce, 0 for everything the "
                         "reports hold (default: %(default)s)")
    ex.add_argument("--no-collect", action="store_true",
                    help="export the reports already collected, no ssh")
    ex.add_argument("--output", "-o", default="-",
                    help="results file to write ('-' for stdout, the default)")
    ex.add_argument("--append", action="store_true",
                    help="append to --output instead of replacing it: results "
                         "files are append-only, so a run per append is a "
                         "history the viewer can aggregate over")
    ex.add_argument("--json", action="store_true",
                    help="write NDJSON (one sample object per line) instead "
                         "of the tab-separated form")
    ex.add_argument("--raw", action="store_true",
                    help="one sample per host per report interval instead of "
                         "one per host: the viewer can then show min/max/p95 "
                         "over the run, not just the window's mean")
    ex.add_argument("--peers", action="store_true",
                    help="also export one sample per flow (mx_peer_*), each "
                         "carrying peer= so the worst peer of a host is one "
                         "`max` away. Reduced over the window even with "
                         "--raw, which a full mesh's row count is the reason "
                         "for")
    ex.add_argument("--nic-gbps", type=float, metavar="GBPS",
                    help="per-host NIC line rate. Adds mx_line_util and pins "
                         "the throughput overlays to an absolute scale, which "
                         "is what catches a whole fabric running at half "
                         "speed -- every host reads a comfortable 100%% of a "
                         "median that is itself wrong")
    ex.add_argument("--names", metavar="FILE",
                    help="map mx host names to the names the layout uses: "
                         "one `mxname target` per line")
    ex.add_argument("--target-prefix", default="", metavar="STR",
                    help="string prepended to every target, e.g. 'DH1/A/' "
                         "when the layout addresses nodes by path")
    ex.add_argument("--test-prefix", default="mx_", metavar="STR",
                    help="string prepended to every test name, so mx overlays "
                         "cannot collide with another tool's in the same "
                         "results file (default: %(default)s)")
    ex.add_argument("--run", metavar="LABEL",
                    help="tag every sample with run=LABEL")
    ex.add_argument("--no-meta", action="store_true",
                    help="do not write the !test metadata lines")

    co = sub.add_parser("collect", help="pull report CSVs, no analysis")
    _add_fleet_flags(co)
    co.add_argument("--reports", default=_env("MX_REPORTS", "reports"),
                    help="local directory to pull into (default: %(default)s)")

    sp = sub.add_parser("stop", help="stop the agents (files stay)")
    _add_fleet_flags(sp)

    lg = sub.add_parser("logs", help="collect each agent's log")
    _add_fleet_flags(lg)
    lg.add_argument("--output", "-o", default="logs",
                    help="local directory (default: %(default)s)")

    cl = sub.add_parser("clean", help="stop, then delete every trace from the hosts")
    _add_fleet_flags(cl)
    cl.add_argument("--yes", "-y", action="store_true", help="do not ask")

    r = sub.add_parser("run", help="start, hold for --for seconds, summarize, stop")
    _add_fleet_flags(r)
    _add_run_flags(r)
    _add_report_flags(r)
    r.add_argument("--for", dest="for_seconds", type=float, default=60.0,
                   metavar="SECONDS", help="how long to hold the load (default: 60)")
    r.add_argument("--keep", action="store_true",
                   help="leave the agents running afterwards")

    d = sub.add_parser("doctor", help="check ssh and python on every host")
    _add_fleet_flags(d)

    a = sub.add_parser("agent", help="the per-host worker (started by `mx start`)")
    a.add_argument("--matrix", default=DEFAULT_MATRIX,
                   help="matrix CSV (default: %(default)s)")
    a.add_argument("--host", help="this host's name in the matrix "
                                  "(default: gethostname, then address match)")
    a.add_argument("--report", default=REPORT_NAME,
                   help="report CSV path (default: %(default)s)")
    a.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                   help="report interval, seconds (default: %(default)s)")
    a.add_argument("--duration", type=float, default=0.0,
                   help="exit after N seconds (default: run until signalled)")
    a.add_argument("--workers", default="auto",
                   help="worker processes, or 'auto' (default: %(default)s)")
    a.add_argument("--streams", type=int, default=1,
                   help="sockets per peer (default: %(default)s)")
    a.add_argument("--bind", default="",
                   help="pin to a NIC; must match the matrix addresses")
    a.add_argument("--sndbuf", type=int, default=0,
                   help="SO_SNDBUF per flow (default: kernel default)")
    a.add_argument("--rcvbuf", type=int, default=0,
                   help="SO_RCVBUF per responder (default: 8 MB)")

    sub.add_parser("help", help="every switch of every command, one page")
    return ap


def cmd_full_help(ap):
    """`mx help`: the complete flag reference, generated from the real
    parsers so it cannot drift from what the code accepts."""
    sub_action = next(a for a in ap._actions
                      if isinstance(a, argparse._SubParsersAction))
    log("mx %s -- every command, every switch. `mx CMD --help` shows one" % VERSION)
    log("command with its defaults; `mx hints` maps goals to commands.")
    for name, parser in sub_action.choices.items():
        if name == "help":
            continue
        log("")
        log("=" * 74)
        text = parser.format_help().rstrip()
        # The per-command usage line already says everything the header
        # would; strip the "options:" boilerplate line for density.
        for line in text.splitlines():
            if line.strip() == "options:":
                continue
            log(line)
    log("")
    log("=" * 74)
    log("environment variables (each is the default for the matching flag):")
    log("  MX_MATRIX MX_SERVERS MX_REPORTS MX_REMOTE_DIR MX_USER (or SSH_USER)")
    log("  MX_JOBS MX_PYTHON IPERF_BIND (same variable iperf-orchestrator uses)")
    return 0


COMMANDS = {
    "gen": cmd_gen, "check": cmd_check, "hints": cmd_hints, "start": cmd_start,
    "status": cmd_status, "summarize": cmd_summarize, "collect": cmd_collect,
    "export": cmd_export, "stop": cmd_stop, "logs": cmd_logs,
    "clean": cmd_clean, "run": cmd_run, "doctor": cmd_doctor,
    "agent": cmd_agent,
}


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    if not args.cmd:
        ap.print_help()
        log("")
        log("start here:  mx gen --servers servers.txt --pps 20000 && mx run --for 60")
        log("stuck?       mx hints        every switch:  mx help")
        return 2
    if args.cmd == "help":
        return cmd_full_help(ap)
    try:
        return COMMANDS[args.cmd](args) or 0
    except KeyboardInterrupt:
        log("")
        log("[mx] interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
