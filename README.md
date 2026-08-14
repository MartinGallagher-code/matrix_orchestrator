<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# matrix_orchestrator (`mx`)

[![PyPI version](https://img.shields.io/pypi/v/matrix-orchestrator.svg)](https://pypi.org/project/matrix-orchestrator/)
[![Python versions](https://img.shields.io/pypi/pyversions/matrix-orchestrator.svg)](https://pypi.org/project/matrix-orchestrator/)
[![CI](https://github.com/MartinGallagher-code/matrix_orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/MartinGallagher-code/matrix_orchestrator/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Run a **request/response traffic matrix** across a fleet of servers, and
get **packets per second** back as the headline number.

Every host talks to every other host: it sends a request of *x* bytes,
and the host that receives it answers with a reply of *y* bytes. That is
the whole model. Six commands drive it, one file configures it, and
`mx clean` removes every trace when you are done.

```bash
printf '%s\n' 10.0.0.10 10.0.0.11 10.0.0.12 > servers.txt

mx gen --servers servers.txt --pps 20000   # 1. build matrix.csv
mx start                                   # 2. deploy + run everywhere
mx status                                  # 3. is it running, and how fast?
mx summarize                               # 4. pps / Gbps / loss / latency
mx stop                                    # 5. stop the agents
mx clean                                   # 6. leave no trace
```

Or all of it in one command:

```bash
mx run --for 60
```

Not sure what to ask for? `mx hints` turns a goal into the command that
gets you there.

---

## Why this exists

[`iperf_orchestrator`](https://github.com/MartinGallagher-code/iperf_orchestrator)
sweeps a mesh pair by pair and tells you each pair's maximum bandwidth.
Its `matrix_agent` sustains a one-way traffic matrix and tells you
whether the fabric delivers it.

This tool answers a third question: **how many packets per second can the
whole fleet exchange, when every packet has to be answered?** That is the
shape of real request/response traffic (RPC, storage reads, control
planes), it is where fabrics and NICs actually fall over, and it makes
the round-trip time fall out for free.

---

## Install

```bash
pip install matrix-orchestrator
```

That puts `mx` on your `PATH` (and `matrix-orchestrator` as an alias).
No dependencies — the package is standard-library only.

Or skip installing entirely: `mx` is one self-contained file.

```bash
git clone https://github.com/MartinGallagher-code/matrix_orchestrator
cd matrix_orchestrator
./mx hints
```

**Requirements.** Python 3.6+ and `ssh`/`scp` on the machine you drive
from; Python 3.6+ on every server. Nothing else — no agents to install,
no packages, no root. Key-based SSH must already work
(`ssh-copy-id host`). Check the whole fleet at once with `mx doctor`.

---

## The six commands

| Command | What it does |
|---|---|
| `mx gen` | Build `matrix.csv` from your server list. |
| `mx start` | Copy the agent + matrix to every host and start them. |
| `mx status` | One line per host: the live ticker, or `NOT-RUNNING`. |
| `mx summarize` | Collect the reports and print pps, Gbps, loss, latency — and what to do next. |
| `mx stop` | Stop the agents. Reports and logs stay on the hosts. |
| `mx clean` | Stop, then delete everything. No trace left. |

And five more when you want them: `mx run` (all of the above in one
shot), `mx check` (will the NICs carry this?), `mx hints` (goal →
command), `mx logs` (collect agent logs), `mx doctor` (is the fleet
ready?).

---

## Request size in, reply size out

The point of the tool. `--tx-size` is what every request carries;
`--rx-size` is what the receiving host sends back for each one:

```bash
mx gen --servers servers.txt --pps 5000 --tx-size 128 --rx-size 8192
```

Every host now sends 5,000 128-byte requests per second to every peer,
and answers every request it receives with an 8 KB reply. The packet
rates in both directions are identical — one reply per request — while
the *bandwidth* is 64× heavier on the reply path. That asymmetry is
usually what breaks first, and `mx summarize` reports the two directions
separately so you can see it.

Sizes are the packet payload, 32–65507 bytes. `mx` also reports the
**wire** rate (payload + 66 bytes of Ethernet/IP/UDP framing per packet),
because that is the number a NIC actually has to carry.

---

## The matrix file

`mx gen` writes a plain grid CSV, and everything about the traffic lives
in it — the hosts, the per-pair rates, the packet sizes, the port. No
other command needs those flags again:

```text
# mx matrix v1 -- rows send, columns receive, cells are packets/sec
# tx_size=64 rx_size=512 port=5300
src\dst,10.0.0.10,10.0.0.11,10.0.0.12
10.0.0.10,,20000,20000
10.0.0.11,20000,,20000
10.0.0.12,20000,20000,
```

Edit it by hand for anything non-uniform:

- **blank a cell** to remove that flow,
- **change a cell** to give one pair its own rate,
- write **`max`** in a cell to let that pair run unpaced,
- change the `tx_size`/`rx_size`/`port` line to reshape the packets.

Then `mx start` again. Host tokens are `name[=addr[:port]]`, so a bare
list of IPs works, and `hostA=10.0.0.10:5399` works when the name, the
address and the port all differ.

---

## Reading the summary

```
mx summarize -- last 60s, 12 hosts, 132 flows, 64 bytes out -> 64 bytes back

  REQUESTS       2.640 Mpps       2.75 Gbps wire       1.35 Gbps payload
  DELIVERED      2.601 Mpps    98.52% of what was sent
  REPLIES        2.598 Mpps       2.71 Gbps wire       1.33 Gbps payload
  TARGET         2.640 Mpps   100.0% achieved
  TOTAL          5.238 Mpps       5.46 Gbps wire, both directions
  LOSS               1.59%   round trip (1.48% forward, 0.11% on the way back)
  RTT       avg 240us over all flows; worst flow p50 190us  p99 4.1ms  max 31ms
```

- **REQUESTS** is what the senders put on the wire; **DELIVERED** is what
  the receiving hosts actually counted. Senders cannot see their own
  drops, so DELIVERED is the honest number.
- **LOSS** is split into the forward leg and the return leg, because a
  fabric that drops your 64-byte requests and one that drops your 8 KB
  replies need different fixes.
- **RTT** comes free with request/response: the sender stamps each
  request and the reply carries the stamp back, so no clock sync is
  involved and the number is a true round trip.

Then a per-host table (worst delivery first), the worst flows, and a
**WHAT TO DO NEXT** section that reads the numbers and tells you which
knob they point at.

The per-host table carries three CPU numbers, and the third is the one
that matters most:

| Column | Meaning |
|---|---|
| `cpu` | the whole box, averaged over its cores |
| `1 core` | the busiest single core |
| `agent` | the **busiest agent worker**, as a share of *one* core |

Each worker is one Python process, so the GIL holds it near 100% of one
core. When `agent` approaches 100%, that worker is the ceiling and you
are measuring the tool rather than the network — add workers if the host
has spare cores, or add hosts. `mx summarize` says so in as many words.

`mx summarize --grid g` also writes `pps_grid.csv`, `delivered_grid.csv`,
`loss_grid.csv` and `rtt_p99_grid.csv` — N×N grids in the matrix's own
shape. A dark row is a sick sender, a dark column a sick receiver, a dark
block a congested pair of leaves.

---

## Equal load without the full mesh (`--peers`)

"All-to-all" usually means two separable things: every host carries the
same sustained load, and every layer of the fabric is exercised. Neither
requires every host to talk to every other host.

`mx gen --peers K` builds a **k-regular shuffle**: each host sends to
and receives from exactly K randomly-chosen others. The balance is by
construction, not statistical — the graph is K superimposed
permutations, so every host has exactly K flows out and K in at the same
rate, all held continuously and simultaneously, just like the full mesh.
With equal-size racks a random shuffle sends the vast majority of flows
across spine and superspine, so the fabric aggregate is the same too.

What changes is the machinery: K sockets per host instead of N−1, per
flow rates fat enough to pace cleanly, reports K rows per interval
instead of a thousand. At high per-host packet rates on a big fleet,
sparse is not a compromise — it is the only shape that sustains cleanly.

The shuffle is seeded and replayable (`--seed`, also stored in the
matrix header). Path coverage through ECMP is the one statistical part:
raise `--streams` to multiply the 4-tuples per flow, and run successive
soaks with different seeds to re-roll every path.

```bash
mx gen --servers servers.txt --peers 8 --pps 250000 --seed 42
mx start --streams 4 --workers auto
```

### How `--peers` and `--streams` compose

The two flags answer different questions and multiply cleanly:

- **`--peers K`** picks *who* each host talks to — K peers instead of
  N−1. It shapes the graph and the per-flow rate (each pair's cell gets
  the whole per-pair rate).
- **`--streams S`** picks *how many sockets carry each pair* — the
  pair's rate is split across S sockets, not multiplied. More 4-tuples
  is what gives worker processes, NIC RSS queues and ECMP paths
  something to spread.

A host therefore holds **K × S client sockets**, and that product is
what everything scales by: the fd budget (the agent raises its own soft
limit to cover it), the worker ceiling (workers are capped at flows+1,
so on a sparse mesh `--streams` is the lever that lets more cores help),
and the number of distinct paths the fabric sees. `--peers` without
`--streams` concentrates each pair onto one path; `--peers 8
--streams 4` keeps 8 fat flows but hashes each across 4 paths — usually
the shape you want on an ECMP fabric.

## Every pair, K sockets: the layered rotation (`--peers K --dwell T`)

`--peers K` holds equal load with K flows per host, but it measures only
those K·N of the N·(N−1) ordered pairs. When the question is "show me
the sick *pair*", coverage has to be complete. The full mesh gets you
that at N−1 sockets per host; the layered rotation gets you the same
guarantee at K:

```bash
mx gen --servers servers.txt --peers 8 --pps 20000 --dwell 60
mx run --for 7500        # >= one full cycle
mx summarize --grid g    # COVERAGE section + g/coverage_grid.csv
```

The construction is why this is a guarantee and not a hope. The
`--peers` graph is K shifted permutations of one shuffle, and the
complete digraph is exactly the union of all N−1 possible shifts — so
`--dwell` deals those N−1 shifts out K at a time into **⌈(N−1)/K⌉
edge-disjoint layers**. Every agent holds one layer's flows for T
seconds, then switches; after one full cycle every ordered pair has been
measured **exactly once** — no pair repeated, none missed, and the layer
count is derived rather than chosen so the guarantee cannot be
configured away. (Independent random matrices per layer would instead
collide by birthday and leave a tail of pairs never measured.) If K does
not divide N−1 the last layer carries the remainder and is simply a
little lighter.

Per-pair pps is held constant across layers, so **per-host load never
changes** — the rotation only changes who carries it. `mx check` on a
layered matrix is therefore checking every layer at once.

One exception: when K does not divide N−1, the last layer carries only
the remainder, so per-host load dips to R·rate for one dwell per cycle.
If the point of your run is a soak whose offered load never dips, add
**`--equal-layers`**: the short layer is padded back up to K with pairs
repeated from the other layers, so every layer carries exactly K flows
per host and every host is equally busy all the time. The repeated
pairs are measured twice per cycle, so the guarantee softens from
"exactly once" to "at least once" — stated in the matrix header, the
gen output and the agent log. It is a no-op when K divides N−1 (pick
such a K and you need neither the flag nor the trade).

The file stays one ordinary matrix: the grid in it is layer 0, and the
other layers exist only as four header keys (`peers seed layers dwell`).
Each agent derives the whole schedule from the seed — every host already
has the identical host list — and switches on its own wall clock
(`layer = walltime // dwell mod layers`). No control channel, no
coordinated cutover: the responder answers whatever arrives, so a host
that switches a second late just reads as a mixed boundary interval.
At each switch the old layer's sockets stay open one report interval to
catch replies still in flight (peak fd cost 2·K·S, independent of layer
count), and those tails land in rows whose send-side cells are blank, so
no average downstream is poisoned by them.

**How short can the dwell be?** The mechanical floor is one report
interval — switches land on report ticks, and `mx start` refuses a dwell
that is not a whole multiple of `--interval`. The *useful* floor is
about **3× the interval**, so each layer gets a couple of clean interior
intervals; packet counts stop mattering long before that (at 10k pps
even a 1 s visit is 10 000 samples). At the default 5 s interval start
at `--dwell 15`; for the fastest full sweeps drop the interval too:

```bash
mx gen --servers servers.txt --peers 8 --pps 20000 --dwell 3
mx start --interval 1      # 1000 hosts: 125 layers x 3s = full
                           # every-pair-once coverage every ~6 min,
                           # 8 sockets per host at any moment
```

Below that, the boundary blur (one drain interval per switch, plus NTP
skew) starts to be a visible fraction of every layer, and you are
measuring the switching, not the fabric.

`mx summarize` on a layered run reports time-averaged rates, a
**COVERAGE** line (cumulative pairs measured across the whole run, with
the still-unmeasured ones named), a per-layer table for the window, and
with `--grid` a `coverage_grid.csv` — the N×N of how many intervals each
pair has been measured, where an empty cell means "never yet".

## Finding the limit

Raise the rate until delivery stops keeping up:

```bash
mx gen --servers servers.txt --pps 50000 && mx run --for 120
mx gen --servers servers.txt --pps 100000 && mx run --for 120
```

The last rate that delivers cleanly is the fleet's sustainable
all-to-all packet rate. Two things to watch, in this order:

1. **The p99 latency lifting off the p50** — queues are filling. This
   usually happens before loss does.
2. **DELIVERED falling behind REQUESTS** — something is dropping.

`mx gen --pps max` skips the ramp and sends unpaced, which finds the
ceiling fastest but tells you less about where it is.

Before you blame the network, check what you asked for was possible:

```bash
mx check --nic-gbps 25 --nic-mpps 15
```

## How fast can it go

Packet rate is CPU work, and in Python one process is one GIL. So the
agent runs **P worker processes** per host — `--workers auto` (the
default) starts one per core, capped at 8. Each worker drives all of its
sockets from a single event loop, and workers share the listening port
through `SO_REUSEPORT`, so the kernel spreads inbound requests across
them too.

Measured on one ordinary core:

| | Rate |
|---|---|
| One worker, request + reply | **~200k pps** |
| Per host | ~200k × workers |
| Fleet | ~200k × workers × hosts |

So 9 Mpps across the fleet is 45 hosts at one worker each, or 12 hosts
with 4 workers apiece — `mx hints --pps-per-host N` does that arithmetic
and tells you how many workers to ask for.

Per *host*, expect 1–4 Mpps on a typical server. Beyond a few Mpps on a
single box the kernel's own UDP socket path becomes the limit as much as
Python does, and the answer is a wider fleet — or a different kind of
tool entirely (AF_XDP, DPDK).

Why processes and not threads: on a 4-core box, the same send loop runs
at 300k pps in one thread, 199k across two, and 57k across four — threads
convoy on the GIL and make it *worse*. The same work in four processes
runs at 1.09M pps. Watch the `agent` column in `mx summarize`; as it
approaches 100% that worker is saturated.

### One core pegged while the rest of the box idles

Workers can never outnumber flows, and by default a pair is **one socket,
one 4-tuple**. Everything that spreads load downstream — our workers, the
NIC's receive queues, the fabric's ECMP hash — does it by hashing that
tuple. So a mesh with 3 peers puts 3 cores to work no matter how many the
box has, and those cores sit at 100% while the rest idle.

`--streams N` gives each pair N sockets instead of one. The pair's packet
rate is **split** across them, so the offered load is identical; what
changes is that there are now N times as many tuples to spread. Measured
on 4 workers with one peer, unpaced:

| `--streams` | workers used | achieved | busiest worker |
|---|---|---|---|
| 1 | 2 | 203.8 kpps | **100% of a core** |
| 4 | 4 | 323.2 kpps | 51% |
| 8 | 4 | 359.5 kpps | 56% |

So the recipe for a big box against a small mesh is to raise both:

```bash
mx start --streams 8 --workers 32
```

`mx summarize` detects this case by itself — when a worker is pegged and
the worker count is already at the flow-count cap, it says so and names
the flag.

### File descriptors take care of themselves

Every flow is a socket, and the common soft `ulimit -n` default of 1024
is exactly where a big mesh or a high `--streams` count used to die on
startup. The agent now raises its own *soft* limit to what the run needs
— that requires no privilege, and the raise lives and dies with the
process, so nothing on the box changes. Only a too-low *hard* limit
still needs an administrator: the agent refuses to start with the fix
named (`limits.conf` / systemd `LimitNOFILE`), and `mx doctor` flags
such hosts (`FDS-TOO-LOW`, from `ulimit -Hn`) before you deploy.

---

## Leaving no trace

Everything the tool touches on a server lives in one directory
(`/var/tmp/mx` by default, `--remote-dir` to change it): the agent file,
the matrix, the log, the report. Nothing is installed, no package is
added, no sysctl or qdisc is changed, no unit file is written.

```bash
mx logs         # take the logs first if you want them
mx summarize    # and the reports
mx clean        # stop everything, remove the directory, verify it is gone
```

`mx clean` refuses to report success unless the directory is actually
gone and no agent is still running.

---

## Common runs

```bash
# Small-packet torture test, unpaced, one worker process per core
mx gen --servers servers.txt --pps max --tx-size 64 --rx-size 64
mx start --workers auto

# RPC-shaped: small ask, large answer
mx gen --servers servers.txt --pps 5000 --tx-size 128 --rx-size 8192

# Sustained equal load on every host without the full mesh: each host
# talks to exactly 8 shuffled peers -- identical per-host load, held
# continuously, with 8 sockets instead of N-1 (seed printed, replayable)
mx gen --servers servers.txt --peers 8 --pps 100000

# ...and rotated through disjoint layers so every ordered pair is
# measured exactly once per cycle, still with only 8 sockets per host
mx gen --servers servers.txt --peers 8 --pps 100000 --dwell 15
mx run --for 2000 && mx summarize --grid g   # coverage grid included

# Size the rate from a bandwidth budget instead of a packet rate
mx gen --servers servers.txt --gbps 10 --tx-size 1400

# Pin everything to one NIC (interface name or address, both work)
mx start --bind eth1

# The complete flag reference, generated from the real parsers
mx help

# Watch it live
mx status --watch 5

# Work out the settings before committing to them
mx hints --servers servers.txt --pps-per-host 2000000 --tx-size 64
```

Every fleet command takes `--user`, `--jobs`, `--remote-dir`, `--python`
and `--dry-run`; each has an `MX_*` environment variable
(`MX_USER`, `MX_JOBS`, `MX_REMOTE_DIR`, `MX_PYTHON`, `MX_MATRIX`,
`MX_SERVERS`, `MX_REPORTS`). `--dry-run` prints the ssh and scp commands
instead of running them. `mx help` prints every switch of every command
on one page.

### How `--bind` really works (the two-NIC case)

Fleets usually have a management NIC (the addresses in `servers.txt`,
where ssh goes) and a data NIC (the one you want to load). Binding the
local sockets to the data NIC is only half the job: the *destinations*
have to be the peers' data-NIC addresses too, or every request goes to
an address nobody is listening on and the run reports 100% loss that has
nothing to do with the network.

So `mx start --bind eth1` does both halves, the same way
`iperf-orchestrator` does: it resolves the pattern **on every host over
ssh** (substring match against that host's `ip -o -4 addr show`), then
deploys a matrix retargeted at those data-plane addresses — ssh keeps
using the login addresses, the traffic rides the bound NIC end to end.
If any host has no matching interface, `start` aborts up front and names
the hosts, before launching a mesh that cannot work.

A hand-run `mx agent --bind ...` whose bound address does not match what
the matrix tells peers refuses to start, with the explanation, instead
of running a guaranteed-100%-loss test.

---

## Why UDP only

"Every request of *x* bytes is answered with a reply of *y* bytes" is a
statement about packets, and only a datagram protocol keeps that promise
on the wire. Over TCP the kernel would coalesce and re-segment your
requests, and the packets-per-second number — the whole point here —
would be fiction.

If you want TCP goodput, sweep it with
[`iperf_orchestrator`](https://github.com/MartinGallagher-code/iperf_orchestrator);
if you want a sustained one-way TCP matrix, use its `matrix_agent`. This
tool is the packet-rate and round-trip half of that family.

---

## The report CSV

Each agent appends one row per flow per interval to `report.csv`, which
`mx summarize` collects into `reports/<host>.csv`:

```
ts,host,dir,peer,size,rep_size,target_pps,pps,mbps,rep_pps,rep_mbps,
loss_pct,rtt_avg_us,rtt_p50_us,rtt_p99_us,rtt_max_us,cpu_pct,cpu_max_pct,
agent_cpu_pct,workers,layer
```

`dir=tx` rows are this host as a client (requests it sent, replies it got
back, and the latency between them). `dir=rx` rows are this host as a
server (requests that *arrived* from that peer, replies it sent). One
`dir=host` row per interval carries the CPU samples and the worker count.

One row per peer per interval, whatever the worker count — the parent
merges its workers' numbers before writing, so nothing downstream has to
know how the host was sharded. It is a plain CSV; take it to whatever you
normally plot with.

On a layered run (`--dwell`) each `tx` row also carries its `layer`, and
a switch leaves one *drain* row per finished flow: the replies that were
still in flight when the layer ended, with the send-side cells left
blank. Blank, not zero — a zero rate there is an artifact of the switch,
and any tool averaging the column would be poisoned by it. Treat
`pps == ""` as "not sending this interval", not as zero.

With `--equal-layers` a boundary interval at the cycle wrap can hold
*two* rows for the same peer — the old layer's drain tail and the new
layer's live flow — told apart by the `layer` column. Group by
`(peer, layer)` rather than peer alone if you post-process layered
reports yourself.

---

## Testing

```bash
tests/run_tests.sh          # everything
tests/run_tests.sh -v       # with full output
```

The suite runs real agents exchanging real packets over loopback, and
drives the entire fleet lifecycle — deploy, start, status, summarize,
logs, stop, clean — through a fake `ssh`/`scp` that executes the remote
commands in a local sandbox. No second machine required.

---

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
