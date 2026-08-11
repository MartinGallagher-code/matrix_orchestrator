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
import os
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor

VERSION = "1.0.0"

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

REPORT_NAME = "report.csv"
LOG_NAME = "agent.log"
PID_NAME = "agent.pid"
MATRIX_NAME = "matrix.csv"          # name the matrix always takes on a host

ZEROS = b"\0" * MAX_SIZE

CONFIG_KEYS = ("tx_size", "rx_size", "port")

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

class Matrix(object):
    """Host list, endpoints, per-pair target rates and the run config."""

    def __init__(self, path, hosts, addrs, ports, rates, tx_size, rx_size, port):
        self.path = path
        self.hosts = hosts                  # ordered names; index == matrix row
        self.addrs = addrs                  # name -> address
        self.ports = ports                  # name -> port
        self.rates = rates                  # (src, dst) -> pps (inf == unpaced)
        self.tx_size = tx_size
        self.rx_size = rx_size
        self.port = port
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
    return Matrix(path, hosts, addrs, ports, rates, tx_size, rx_size, port)


def write_matrix(path, tokens, cell_for, tx_size, rx_size, port):
    """Write a matrix CSV with the run config in its header comment."""
    names = [parse_token(t)[0] for t in tokens]
    out = sys.stdout if path == "-" else open(path, "w", newline="")
    try:
        out.write("# mx matrix v%s -- rows send, columns receive, cells are packets/sec\n"
                  % VERSION.split(".")[0])
        out.write("# tx_size=%d rx_size=%d port=%d\n" % (tx_size, rx_size, port))
        out.write("# every request of %d bytes is answered with a reply of %d bytes\n"
                  % (tx_size, rx_size))
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
# Agent: client flows
# ---------------------------------------------------------------------------

class Flow(object):
    """One client flow: this host -> one peer, request in / reply back.

    Counters are plain ints with a single writer each (the send thread
    owns tx_*, the receive thread owns rx_* and the rtt fields), so the
    reporter can read them without locking or perturbing the hot loop.
    """

    def __init__(self, agent, peer, addr, port, target_pps):
        self.agent = agent
        self.peer = peer
        self.addr = addr
        self.port = port
        self.target_pps = target_pps
        self.tx_pkts = 0
        self.tx_bytes = 0
        self.rx_pkts = 0
        self.rx_bytes = 0
        self.errors = 0
        self.rtt_sum = 0
        self.rtt_max = 0
        self.rtt_n = 0
        self.rtt_hist = [0] * RTT_NBUCKETS
        self.sock = None
        self.dest = None
        self.ready = threading.Event()
        self.threads = []

    def start(self):
        for target, name in ((self._send_loop, "tx"), (self._recv_loop, "rx")):
            t = threading.Thread(target=target, daemon=True,
                                 name="%s-%s" % (name, self.peer))
            t.start()
            self.threads.append(t)

    def _open(self):
        a = self.agent
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if a.sndbuf:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, a.sndbuf)
            except OSError:
                pass
        if a.bind_ip:
            sock.bind((a.bind_ip, 0))
        # Resolve once, then sendto() a literal address forever -- a
        # hostname in sendto() would mean a resolver call per packet.
        #
        # Deliberately NOT connected: a multi-homed peer whose responder
        # is bound to the wildcard address answers from whichever local
        # address the route picks, which a connected socket would discard
        # as coming from the wrong peer. This socket only ever carries
        # one flow's replies, so accepting them all is right.
        self.dest = socket.getaddrinfo(self.addr, self.port, socket.AF_INET,
                                       socket.SOCK_DGRAM)[0][4]
        return sock

    def _send_loop(self):
        a = self.agent
        try:
            self.sock = self._open()
        except OSError as exc:
            a.fail("cannot open flow to %s (%s:%d): %s"
                   % (self.peer, self.addr, self.port, exc))
            self.ready.set()
            return
        self.ready.set()

        stop = a.stop
        pack, sendto, dest = HDR.pack, self.sock.sendto, self.dest
        monotonic = time.monotonic
        idx, tx_size, rx_size = a.index, a.tx_size, a.rx_size
        pad = pad_for(tx_size)
        pps = self.target_pps
        paced = pps != float("inf")
        # Bucket holds 5ms of traffic: enough to amortize the syscall
        # cadence, too little for a stall to earn a catch-up burst.
        burst_cap = max(2.0, pps * 0.005) if paced else 0.0
        tokens, last = 0.0, monotonic()
        seq = 0
        while not stop.is_set():
            if paced:
                now = monotonic()
                tokens = min(tokens + (now - last) * pps, burst_cap)
                last = now
                if tokens < 1.0:
                    stop.wait(min(0.25, (1.0 - tokens) / pps))
                    continue
                batch = int(tokens)
                tokens -= batch
            else:
                batch = 256
            for _ in range(batch):
                seq += 1
                try:
                    sendto(pack(MAGIC, KIND_REQ, 0, idx, tx_size, rx_size,
                                seq, int(monotonic() * 1e6)) + pad, dest)
                except OSError:
                    self.errors += 1
                    stop.wait(0.05)
                    break
                self.tx_pkts += 1
                self.tx_bytes += tx_size

    def _recv_loop(self):
        self.ready.wait()
        sock = self.sock
        if sock is None:
            return
        stop = self.agent.stop
        sock.settimeout(0.5)
        unpack, monotonic = HDR.unpack_from, time.monotonic
        hist = self.rtt_hist
        while not stop.is_set():
            try:
                data = sock.recv(MAX_SIZE)
            except socket.timeout:
                continue
            except OSError:
                stop.wait(0.05)
                continue
            if len(data) < HDR_SIZE:
                continue
            magic, kind, _flags, _idx, _plen, _rsize, _seq, tsend = unpack(data)
            if magic != MAGIC or kind != KIND_REP:
                continue
            self.rx_pkts += 1
            self.rx_bytes += len(data)
            rtt = int(monotonic() * 1e6) - tsend
            if rtt >= 0:
                self.rtt_sum += rtt
                self.rtt_n += 1
                if rtt > self.rtt_max:
                    self.rtt_max = rtt
                hist[rtt_bucket(rtt)] += 1


# ---------------------------------------------------------------------------
# Agent: responder
# ---------------------------------------------------------------------------

class Responder(object):
    """A server thread: answer every request with a reply of the size the
    request asked for.

    One of these per `--workers`, each with its own SO_REUSEPORT socket
    and its own preallocated counter row per peer -- so several workers
    share the port with no locking anywhere in the packet path, and the
    reporter just sums the rows. The rows are allocated up front, so the
    reporter can read them while a worker writes without ever seeing a
    resized list.
    """

    def __init__(self, agent, nhosts):
        self.agent = agent
        # per peer index: [requests, request bytes, replies, reply bytes];
        # the extra last row absorbs packets from unknown senders.
        self.counts = [[0, 0, 0, 0] for _ in range(nhosts + 1)]
        self.unknown = self.counts[nhosts]
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True,
                                       name="server")
        self.thread.start()

    def _loop(self):
        a = self.agent
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        reuseport = getattr(socket, "SO_REUSEPORT", None)
        if reuseport is not None:
            try:
                sock.setsockopt(socket.SOL_SOCKET, reuseport, 1)
            except OSError:
                pass
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                            a.rcvbuf or 8 * 1024 * 1024)
        except OSError:
            pass
        try:
            sock.bind((a.bind_ip, a.port))
        except OSError as exc:
            a.fail("cannot listen on %s:%d: %s" % (a.bind_ip or "0.0.0.0", a.port, exc))
            return
        sock.settimeout(0.5)

        stop = a.stop
        counts, unknown = self.counts, self.unknown
        nhosts = len(counts) - 1
        pack, unpack = HDR.pack, HDR.unpack_from
        idx = a.index
        pads = {}
        while not stop.is_set():
            try:
                data, peer = sock.recvfrom(MAX_SIZE)
            except socket.timeout:
                continue
            except OSError:
                stop.wait(0.05)
                continue
            if len(data) < HDR_SIZE:
                continue
            magic, kind, _flags, src, _plen, rsize, seq, tsend = unpack(data)
            if magic != MAGIC or kind != KIND_REQ:
                continue
            row = counts[src] if src < nhosts else unknown
            row[0] += 1
            row[1] += len(data)
            if rsize < HDR_SIZE:
                rsize = HDR_SIZE
            elif rsize > MAX_SIZE:
                rsize = MAX_SIZE
            pad = pads.get(rsize)
            if pad is None:
                pad = pads[rsize] = pad_for(rsize)
            # t_send is echoed untouched: the client measures its own
            # round trip without either side knowing the other's clock.
            try:
                sock.sendto(pack(MAGIC, KIND_REP, 0, idx, rsize, 0, seq, tsend) + pad,
                            peer)
            except OSError:
                continue
            row[2] += 1
            row[3] += rsize
        sock.close()


# ---------------------------------------------------------------------------
# Agent: reporting
# ---------------------------------------------------------------------------

REPORT_FIELDS = ["ts", "host", "dir", "peer", "size", "rep_size", "target_pps",
                 "pps", "mbps", "rep_pps", "rep_mbps", "loss_pct",
                 "rtt_avg_us", "rtt_p50_us", "rtt_p99_us", "rtt_max_us",
                 "cpu_pct", "cpu_max_pct", "agent_cpu_pct"]


class Reporter(object):
    """Every interval: append per-flow rows to report.csv and print one
    line to stdout (which is what `mx status` shows)."""

    def __init__(self, agent, path):
        self.agent = agent
        fresh = not os.path.exists(path)
        self.fh = open(path, "a", newline="")
        self.csv = csv.writer(self.fh)
        if fresh:
            self.csv.writerow(REPORT_FIELDS)
            self.fh.flush()
        self.prev = {}
        self.prev_cpu = read_cpu()
        self.prev_self = read_self_cpu()
        self.last = time.monotonic()

    def _delta(self, key, value):
        prev = self.prev.get(key, 0)
        self.prev[key] = value
        return value - prev

    def tick(self):
        a = self.agent
        now = int(time.time())
        mono = time.monotonic()
        # Rate over the elapsed time we actually measured, not the
        # nominal interval, so drift and the final partial tick stay true.
        elapsed = max(mono - self.last, 1e-3)
        self.last = mono
        cur_cpu = read_cpu()
        cpu, cpu_max = cpu_delta(self.prev_cpu, cur_cpu)
        self.prev_cpu = cur_cpu
        cur_self = read_self_cpu()
        self_pct = None
        if cur_self is not None and self.prev_self is not None:
            self_pct = (cur_self - self.prev_self) / elapsed * 100.0
        self.prev_self = cur_self

        tx_pps = tx_bps = rx_pps = rx_bps = 0.0
        target_total = 0.0
        unpaced = 0
        rtt_hist_total = [0] * RTT_NBUCKETS
        rtt_sum_total = rtt_n_total = 0
        rows = []
        for fl in a.flows:
            d_tx = self._delta(("tx", fl.peer), fl.tx_pkts)
            d_txb = self._delta(("txb", fl.peer), fl.tx_bytes)
            d_rx = self._delta(("rx", fl.peer), fl.rx_pkts)
            d_rxb = self._delta(("rxb", fl.peer), fl.rx_bytes)
            d_rtt_sum = self._delta(("rtts", fl.peer), fl.rtt_sum)
            d_rtt_n = self._delta(("rttn", fl.peer), fl.rtt_n)
            hist = [n - self.prev.get(("h", fl.peer, i), 0)
                    for i, n in enumerate(fl.rtt_hist)]
            for i, n in enumerate(fl.rtt_hist):
                self.prev[("h", fl.peer, i)] = n
            for i, n in enumerate(hist):
                rtt_hist_total[i] += n
            rtt_sum_total += d_rtt_sum
            rtt_n_total += d_rtt_n

            f_tx_pps = d_tx / elapsed
            f_rx_pps = d_rx / elapsed
            tx_pps += f_tx_pps
            rx_pps += f_rx_pps
            tx_bps += d_txb * 8.0 / elapsed
            rx_bps += d_rxb * 8.0 / elapsed
            if fl.target_pps == float("inf"):
                unpaced += 1
            else:
                target_total += fl.target_pps
            rows.append([now, a.host, "tx", fl.peer, a.tx_size, a.rx_size,
                         "" if fl.target_pps == float("inf") else "%.1f" % fl.target_pps,
                         "%.1f" % f_tx_pps, "%.3f" % (d_txb * 8.0 / elapsed / 1e6),
                         "%.1f" % f_rx_pps, "%.3f" % (d_rxb * 8.0 / elapsed / 1e6),
                         "%.3f" % (100.0 - pct(d_rx, d_tx)) if d_tx else "",
                         "%.0f" % (d_rtt_sum / d_rtt_n) if d_rtt_n else "",
                         "%.0f" % (rtt_percentile(hist, 0.50) or 0) if d_rtt_n else "",
                         "%.0f" % (rtt_percentile(hist, 0.99) or 0) if d_rtt_n else "",
                         "%.0f" % fl.rtt_max, "", "", ""])

        srv_pps = srv_bps = 0.0
        for i, peer in enumerate(a.hosts):
            row = [sum(r.counts[i][j] for r in a.responders) for j in range(4)]
            if peer == a.host:
                continue
            d_req = self._delta(("sq", peer), row[0])
            d_reqb = self._delta(("sqb", peer), row[1])
            d_rep = self._delta(("sp", peer), row[2])
            d_repb = self._delta(("spb", peer), row[3])
            if not (d_req or d_rep):
                continue
            srv_pps += d_req / elapsed
            srv_bps += d_reqb * 8.0 / elapsed
            rows.append([now, a.host, "rx", peer, a.tx_size, a.rx_size, "",
                         "%.1f" % (d_req / elapsed),
                         "%.3f" % (d_reqb * 8.0 / elapsed / 1e6),
                         "%.1f" % (d_rep / elapsed),
                         "%.3f" % (d_repb * 8.0 / elapsed / 1e6),
                         "", "", "", "", "", "", "", ""])

        # An all-unpaced host has no target to report; a mixed one reports
        # what the paced flows asked for.
        rows.append([now, a.host, "host", "*", a.tx_size, a.rx_size,
                     "" if unpaced and not target_total else "%.1f" % target_total,
                     "%.1f" % tx_pps,
                     "%.3f" % (tx_bps / 1e6), "%.1f" % rx_pps,
                     "%.3f" % (rx_bps / 1e6),
                     "%.3f" % (100.0 - pct(rx_pps, tx_pps)) if tx_pps else "",
                     "%.0f" % (rtt_sum_total / rtt_n_total) if rtt_n_total else "",
                     "%.0f" % (rtt_percentile(rtt_hist_total, 0.50) or 0),
                     "%.0f" % (rtt_percentile(rtt_hist_total, 0.99) or 0), "",
                     "%.1f" % cpu if cpu is not None else "",
                     "%.1f" % cpu_max if cpu_max is not None else "",
                     "%.1f" % self_pct if self_pct is not None else ""])

        for row in rows:
            self.csv.writerow(row)
        self.fh.flush()

        log("ts=%d tx=%s/%s rx=%s serving=%s loss=%.2f%% rtt_p50=%s rtt_p99=%s "
            "cpu=%s flows=%d"
            % (now, fmt_pps(tx_pps),
               "max" if unpaced and not target_total else fmt_pps(target_total),
               fmt_pps(rx_pps),
               fmt_pps(srv_pps), max(0.0, 100.0 - pct(rx_pps, tx_pps)),
               fmt_us(rtt_percentile(rtt_hist_total, 0.50)),
               fmt_us(rtt_percentile(rtt_hist_total, 0.99)),
               "%.0f%%(max %.0f%%)" % (cpu, cpu_max) if cpu is not None else "n/a",
               len(a.flows))
        + (" agent_cpu=%.0f%%" % self_pct if self_pct is not None else ""))


# ---------------------------------------------------------------------------
# Agent: the process that runs on every host
# ---------------------------------------------------------------------------

class Agent(object):
    def __init__(self, matrix, host, args):
        self.matrix = matrix
        self.host = host
        self.hosts = matrix.hosts
        self.index = matrix.index[host]
        self.port = matrix.ports[host]
        self.tx_size = matrix.tx_size
        self.rx_size = matrix.rx_size
        self.sndbuf = args.sndbuf
        self.rcvbuf = args.rcvbuf
        self.bind_ip = ""
        self.stop = threading.Event()
        self.flows = []
        self.responders = []
        self.errors = []

    def fail(self, msg):
        self.errors.append(msg)
        sys.stderr.write("mx agent: %s\n" % msg)
        sys.stderr.flush()


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


def cmd_agent(args):
    try:
        threading.stack_size(256 * 1024)     # many flows, small stacks
    except (ValueError, RuntimeError):
        pass

    matrix = load_matrix(args.matrix)
    host = resolve_self(matrix, args.host)
    agent = Agent(matrix, host, args)
    if args.bind:
        iface, agent.bind_ip = resolve_bind(args.bind)
        log("bind: %r -> %s (%s)" % (args.bind, agent.bind_ip, iface))

    def on_stop(_sig, _frame):
        agent.stop.set()

    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGINT, on_stop)

    for _ in range(max(1, args.workers)):
        r = Responder(agent, len(matrix.hosts))
        agent.responders.append(r)
        r.start()

    peers = matrix.peers_of(host)
    for peer, pps in peers:
        addr, port = matrix.endpoint(peer)
        fl = Flow(agent, peer, addr, port, pps)
        agent.flows.append(fl)
    # Give every responder a moment to bind before the first request.
    time.sleep(0.1)
    for fl in agent.flows:
        fl.start()

    os.makedirs(os.path.dirname(os.path.abspath(args.report)) or ".", exist_ok=True)
    reporter = Reporter(agent, args.report)

    log("mx agent %s: host=%s port=%d flows=%d tx_size=%d rx_size=%d "
        "workers=%d report=%s"
        % (VERSION, host, agent.port, len(agent.flows), agent.tx_size,
           agent.rx_size, max(1, args.workers), args.report))

    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    next_tick = time.monotonic() + args.interval
    while not agent.stop.is_set():
        agent.stop.wait(max(0.0, min(next_tick - time.monotonic(), 0.25)))
        if time.monotonic() >= next_tick:
            reporter.tick()
            next_tick += args.interval
        if deadline is not None and time.monotonic() >= deadline:
            agent.stop.set()
    # A final partial interval, but only if it covers enough time to
    # carry an honest rate.
    if time.monotonic() - reporter.last >= args.interval * 0.5:
        reporter.tick()
    log("mx agent: stopped")
    return 0


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
    if args.workers:
        flags += ["--workers", str(args.workers)]
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


def cmd_gen(args):
    tokens = read_server_list(args.servers)
    n = len(tokens)
    tx_size = check_size("--tx-size", args.tx_size)
    rx_size = check_size("--rx-size", args.rx_size)

    if args.pps is not None:
        cell = args.pps
        if cell <= 0:
            die("--pps must be greater than 0 (or 'max' for unpaced)")
    elif args.gbps is not None:
        if args.gbps <= 0:
            die("--gbps must be greater than 0")
        # Per-host egress budget in wire bits, split evenly across peers.
        per_host_pps = args.gbps * 1e9 / ((tx_size + WIRE_OVERHEAD) * 8.0)
        cell = per_host_pps / (n - 1)
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
        return "" if si == di else text

    write_matrix(args.output, tokens, cell_for, tx_size, rx_size, args.port)
    if args.output == "-":
        return 0

    log("wrote %s" % args.output)
    log("  %d hosts, %d flows, %d bytes out -> %d bytes back"
        % (n, n * (n - 1), tx_size, rx_size))
    if cell == float("inf"):
        log("  rate: unpaced (send as fast as each host can)")
    else:
        eg = cell * (n - 1)
        log("  per pair : %s" % fmt_pps(cell))
        log("  per host : %s out, %s back in" % (fmt_pps(eg), fmt_pps(eg)))
        log("             %s out on the wire (%s payload)"
            % (fmt_gbps(wire_bps(eg, tx_size)), fmt_gbps(payload_bps(eg, tx_size))))
        log("             %s back on the wire (%s payload)"
            % (fmt_gbps(wire_bps(eg, rx_size)), fmt_gbps(payload_bps(eg, rx_size))))
        log("  fleet    : %s offered" % fmt_pps(cell * n * (n - 1)))
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
    log("  note: a per-host python agent tops out around a few hundred kpps;")
    log("        `mx start --workers N` scales the responder across cores.")
    return 0


def cmd_start(args):
    m = load_matrix(args.matrix)
    fleet = Fleet(m, args)
    agent_src = _agent_source()
    flags = _agent_flags(args)

    if not args.no_deploy:
        # scp lands a file under its own basename, and the agent always
        # looks for matrix.csv -- so stage a copy under that name when the
        # local file is called something else.
        tmpdir = None
        matrix_src = args.matrix
        if os.path.basename(matrix_src) != MATRIX_NAME:
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

    # Packet sizes come from the reports, not from the matrix: the matrix
    # may have been re-generated since the run, and the bytes that were
    # actually on the wire are the ones worth converting to Gbps.
    tx_size, rx_size = m.tx_size, m.rx_size
    for r in recent:
        if r.get("size") and r.get("rep_size"):
            try:
                tx_size, rx_size = int(r["size"]), int(r["rep_size"])
            except ValueError:
                pass
            break

    tx = {}       # (host, peer) -> Agg   (client side: what we sent/got back)
    srv = {}      # (host, peer) -> Agg   (server side: what actually arrived)
    hostagg = {}  # host -> Agg
    TXK = ["target_pps", "pps", "mbps", "rep_pps", "rep_mbps",
           "rtt_avg_us", "rtt_p50_us", "rtt_p99_us", "rtt_max_us"]
    for r in recent:
        d = r.get("dir")
        if d == "tx":
            tx.setdefault((r["host"], r["peer"]), Agg()).add(r, TXK)
        elif d == "rx":
            srv.setdefault((r["host"], r["peer"]), Agg()).add(r, ["pps", "rep_pps"])
        elif d == "host":
            hostagg.setdefault(r["host"], Agg()).add(
                r, ["cpu_pct", "cpu_max_pct", "agent_cpu_pct"])
    if not tx:
        die("no client rows in the last %ds -- are the agents still running?"
            % args.window)

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
    per_host = {}
    for (host, peer), a in tx.items():
        h = per_host.setdefault(host, [0.0, 0.0, 0.0, 0.0])
        h[0] += a.mean("pps")
        h[1] += a.mean("rep_pps")
        h[2] += a.mean("target_pps")
        h[3] = max(h[3], a.mean("rtt_p99_us"))
    served = {}
    for (host, peer), a in srv.items():
        served[host] = served.get(host, 0.0) + a.mean("pps")

    log("")
    # "agent" is the agent process's own CPU as a share of ONE core --
    # near 100% means the agent is the ceiling, whatever the box shows.
    log("  %-20s %11s %11s %11s %8s %9s %6s %7s %6s"
        % ("host", "sent", "back", "serving", "loss", "rtt p99",
           "cpu", "1 core", "agent"))
    ranked = sorted(per_host.items(),
                    key=lambda kv: pct(kv[1][1], kv[1][0]) if kv[1][0] else 100.0)
    for host, (s, b, t, p99) in ranked[:args.top_hosts]:
        ha = hostagg.get(host)
        log("  %-20s %11s %11s %11s %7.2f%% %9s %5s %6s %5s"
            % (host, fmt_pps(s), fmt_pps(b), fmt_pps(served.get(host, 0.0)),
               max(0.0, 100.0 - pct(b, s)) if s else 0.0, fmt_us(p99),
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
        back = a.mean("rep_pps")
        arrived = srv[(peer, host)].mean("pps") if (peer, host) in srv else None
        flows.append((pct(back, sent), host, peer, sent, back, arrived,
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

    if args.grid:
        _write_grids(args.grid, m, tx, srv)

    _summary_hints(rx_size, hostagg, target, tx_pps, rep_pps, fwd_pps, rtt_p50, rtt_p99)
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


def _summary_hints(rx_size, hostagg, target, tx_pps, rep_pps, fwd_pps,
                   rtt_p50, rtt_p99):
    """Turn what the numbers say into what to do next."""
    hints = []
    hot = [(h, a.mean("cpu_max_pct")) for h, a in hostagg.items()
           if a.mean("cpu_max_pct") >= 85.0]
    # The agent is one Python process, so the GIL caps it near 100% of a
    # single core. That, not the box-wide figure, is the number that says
    # "you are measuring the tool, not the network".
    busy_agents = [(h, a.mean("agent_cpu_pct")) for h, a in hostagg.items()
                   if a.mean("agent_cpu_pct") >= 75.0]
    short = target and tx_pps < target * 0.95
    fwd_loss = max(0.0, 100.0 - pct(fwd_pps, tx_pps)) if tx_pps else 0.0
    ret_loss = max(0.0, pct(fwd_pps, tx_pps) - pct(rep_pps, tx_pps)) if tx_pps else 0.0

    if short and busy_agents:
        worst = max(busy_agents, key=lambda kv: kv[1])
        hints.append("sending %.0f%% of target, and the agent process on %s is "
                     "at %.0f%% of one core: you are measuring the agent, not "
                     "the fabric. One python process cannot go much past 100%% "
                     "of a core. Split the load across more hosts, lower --pps, "
                     "or raise `mx start --workers N` (responder threads spend "
                     "most of their time in send/recv, where the GIL is "
                     "released, so it helps -- but not linearly)."
                     % (pct(tx_pps, target), worst[0], worst[1]))
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

    script = """
py=$({py} -V 2>&1 || echo 'MISSING python')
running=no
{pgrep} >/dev/null 2>&1 && running=yes
echo "$py; cores=$(nproc 2>/dev/null || echo ?); agent_running=$running"
""".format(py=shlex.quote(fleet.python), pgrep=PGREP)
    log("")
    failed = fleet.each(lambda h: fleet.sh(h, script, timeout=30),
                        "checking hosts (ssh + python)")
    if failed:
        log("[mx] fix ssh/python on those hosts first: key-based ssh must work "
            "non-interactively (ssh-copy-id) and `%s` must exist." % fleet.python)
        return 1
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
    mx start --workers 4
      64-byte requests answered by 64-byte replies, unpaced. Packet rate
      is the metric; bandwidth will be tiny and that is the point.

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

  WHEN THE NUMBERS DISAPPOINT
    packet rate is CPU work: one python agent sends a few hundred kpps
    per core. `mx start --workers N` scales the responder; more hosts
    scale the senders. `mx check --nic-gbps 25 --nic-mpps 15` tells you
    whether you asked for something the NICs cannot do in the first place.

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
    if per_host > 400000:
        log("  heads up: %s per host is beyond what one python agent thread"
            % fmt_pps(per_host))
        log("  sustains (~200-400 kpps). Use `mx start --workers %d`, and expect"
            % max(2, int(per_host / 250000) + 1))
        log("  the senders to be the limit before the fabric is.")
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
    p.add_argument("--workers", type=int, default=0,
                   help="responder threads per host; raise when a core pegs "
                        "(default: 1)")
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
    ap.add_argument("--version", action="version", version="mx %s" % VERSION)
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
    g.add_argument("--tx-size", type=int, default=64, metavar="BYTES",
                   help="request size, %d-%d (default: %%(default)s)" % (HDR_SIZE, MAX_SIZE))
    g.add_argument("--rx-size", type=int, default=64, metavar="BYTES",
                   help="reply size sent back for each request (default: %(default)s)")
    g.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help="UDP port the agents listen on (default: %(default)s)")
    g.add_argument("--output", "-o", default=DEFAULT_MATRIX,
                   help="output file, '-' for stdout (default: %(default)s)")

    c = sub.add_parser("check", help="will the fleet carry this matrix?")
    c.add_argument("--matrix", default=_env("MX_MATRIX", DEFAULT_MATRIX))
    c.add_argument("--nic-gbps", type=float, help="per-host NIC bandwidth cap")
    c.add_argument("--nic-mpps", type=float, help="per-host NIC packet-rate cap, Mpps")
    c.add_argument("--top", type=int, default=15, help="hosts to list")

    h = sub.add_parser("hints", help="what you want -> the command for it")
    h.add_argument("--servers", default=_env("MX_SERVERS", DEFAULT_SERVERS))
    h.add_argument("--pps-per-host", type=float, help="size a run for this per-host rate")
    h.add_argument("--pps-per-pair", type=float, help="size a run for this per-pair rate")
    h.add_argument("--gbps-per-host", type=float, help="size a run for this per-host Gbps")
    h.add_argument("--tx-size", type=int, default=64)
    h.add_argument("--rx-size", type=int, default=64)

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

    co = sub.add_parser("collect", help="pull report CSVs, no analysis")
    _add_fleet_flags(co)
    co.add_argument("--reports", default=_env("MX_REPORTS", "reports"))

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
    a.add_argument("--matrix", default=DEFAULT_MATRIX)
    a.add_argument("--host", help="this host's name in the matrix")
    a.add_argument("--report", default=REPORT_NAME, help="report CSV path")
    a.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    a.add_argument("--duration", type=float, default=0.0)
    a.add_argument("--workers", type=int, default=1)
    a.add_argument("--bind", default="")
    a.add_argument("--sndbuf", type=int, default=0)
    a.add_argument("--rcvbuf", type=int, default=0)
    return ap


COMMANDS = {
    "gen": cmd_gen, "check": cmd_check, "hints": cmd_hints, "start": cmd_start,
    "status": cmd_status, "summarize": cmd_summarize, "collect": cmd_collect,
    "stop": cmd_stop, "logs": cmd_logs, "clean": cmd_clean, "run": cmd_run,
    "doctor": cmd_doctor, "agent": cmd_agent,
}


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    if not args.cmd:
        ap.print_help()
        log("")
        log("start here:  mx gen --servers servers.txt --pps 20000 && mx run --for 60")
        log("stuck?       mx hints")
        return 2
    try:
        return COMMANDS[args.cmd](args) or 0
    except KeyboardInterrupt:
        log("")
        log("[mx] interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
