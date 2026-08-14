<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project uses [semantic versioning](https://semver.org/).

## [1.4.0] - 2026-08-14

### Added

- **`mx gen --equal-layers`: a rotation whose load never dips.** When K
  does not divide N−1, the plain rotation's last layer carries only the
  remainder, so per-host load drops for one dwell per cycle.
  `--equal-layers` pads that layer back up to K with shifts repeated
  from the front of the deal: every layer then carries exactly K flows
  per host — equally busy, all the time, whatever N and K are. The
  padded shifts come from other layers, so each layer is still exactly
  k-regular with no duplicate edges inside it; the trade is that the
  K−R repeated pairs per host are measured twice per cycle, so the
  coverage guarantee softens from "exactly once" to "at least once" —
  and `gen`, `check`, the agent log and the matrix header all say so.
  Off by default: without the flag, nothing changes and "exactly once"
  stands. The pad is one header key (`fill=1`), derived from the same
  seed like everything else, so it deploys as the same single file. A
  no-op (with a note) when K already divides N−1.

## [1.3.1] - 2026-08-14

### Fixed

- **Peer addresses are resolved once, in the agent's parent process,
  before any worker forks.** Workers used to call `getaddrinfo` per
  flow as they opened it; harmless at startup, but a layered agent
  rebuilds its flows at every switch, so a matrix that carries
  hostnames put the DNS resolver — with its own timeouts — inside the
  workers' report schedule. A worker blocked in a lookup misses its
  ticks, a host whose workers never all report in one interval never
  logs a stats line, and `mx status` (which tails the log) shows the
  startup banner forever while `report.csv` quietly keeps growing.
  Resolution now happens once per peer in the parent; workers only
  ever see IP literals, and layer switches touch no resolver at all.
  A name that fails to resolve is still reported per flow, once, with
  the real error, exactly as before.
- **Partial intervals say so in the log.** A report interval missing
  some workers writes its per-peer rows but skips the host totals —
  that part is unchanged — but it used to skip silently. It now logs
  `partial interval: N of M workers reported`, so a host in that state
  shows a live, truthful line in `mx status` instead of an agent that
  looks like it went quiet at startup.

## [1.3.0] - 2026-08-14

### Added

- **`mx gen --peers K --dwell T`: measure every ordered pair with only K
  flows per host.** The `--peers` shuffle already builds its k-regular
  graph as K shifted permutations of one relabeling; the complete digraph
  is exactly the union of all N−1 shifts. So the layered mode deals those
  N−1 shifts out K at a time: `⌈(N−1)/K⌉` edge-disjoint layers on the one
  shared shuffle, each held for `--dwell` seconds, and one full rotation
  measures every ordered pair **exactly once** — a schedule, not a
  probabilistic hope. The layer count is derived, never chosen, so the
  guarantee cannot be configured away. Per-pair pps stays constant across
  layers, which keeps per-host load flat — the rotation only changes *who*
  carries it. The grid in the file is layer 0 and the file stays a valid
  matrix; the other layers exist only as four header keys
  (`peers seed layers dwell`), because every agent derives the whole
  schedule itself from the seed and switches on its own wall clock —
  one file to deploy, no control channel to fail, and an old agent
  simply runs layer 0 forever.
- At every layer switch the finished layer's sockets stay open one more
  report interval, so replies still in flight are counted for their own
  layer instead of booked as loss — the tails land in `drain` rows whose
  send-side cells stay blank rather than zero, which is what keeps the
  averages honest. Peak fd cost is 2·K·streams, independent of the layer
  count, and the agent sizes its fd limit for exactly that.
- The report CSV gained a `layer` column (last, so existing column
  positions are unchanged), and `mx summarize` understands it: fleet and
  per-host rates become time averages over the window (summing each
  pair's active-only mean would count every layer as if it ran the whole
  time), per-pair delivery is the ratio of totals so the drain tails
  reconcile, and two new sections appear — **COVERAGE**, the cumulative
  count of ordered pairs measured across the whole report history with
  the still-unmeasured ones named, and a per-layer table for the window.
  `--grid` additionally writes `coverage_grid.csv`: the N×N of how many
  intervals each pair has been measured, empty cells meaning never yet.
- `mx start` refuses a dwell that is not a whole multiple of
  `--interval` (switches must land on report ticks, or every boundary
  row blurs) and prints the cycle time; `mx check` and `mx run` say what
  the rotation covers and how long full coverage takes.
- The per-host table in `mx summarize` gained the missing **egress**
  column: everything the host puts on the wire — its own requests plus
  the replies it owes its callers — in wire bits/sec.

### Notes

- The floor for `--dwell` is one report interval (switches land on
  ticks); 3× the interval is the sensible minimum so each layer gets a
  couple of clean interior intervals. With `--interval 1` a dwell of 3 s
  is sound: 1000 hosts at `--peers 8` is 125 layers, a full
  every-pair-once sweep every ~6¼ minutes, with only 8 sockets live per
  host at any moment.

## [1.2.0] - 2026-08-12

### Added

- **The agent raises its own fd limit.** Every flow is a socket, and the
  default soft limit of 1024 is exactly where a big mesh used to die on
  startup. Raising the soft limit up to the hard limit needs no
  privilege and ends with the process, so the agent now does it itself
  and says so. Only when the HARD limit is too low does it refuse to
  start -- up front, naming the limit, what the run needs, and the ways
  out (limits.conf / systemd LimitNOFILE, fewer --streams, `--peers K`,
  or more hosts). Nothing on the box is ever changed behind the
  operator's back.
- `mx doctor` accordingly now checks the hosts' HARD limit (`ulimit
  -Hn`), the only one that still needs an administrator.
- **Read the Docs integration.** `.readthedocs.yaml` plus a Sphinx/MyST
  setup under `docs/` that *includes* the repository's own README,
  CHANGELOG and PUBLISHING pages rather than duplicating them, and
  generates the CLI reference from the live argparse parsers at build
  time — the same anti-drift trick as `mx help`, so a flag cannot exist
  undocumented or linger documented after removal. CI builds the docs
  with warnings-as-errors on every push, so a docs break fails the PR
  instead of surfacing on RTD. Importing the repo on readthedocs.org is
  the only remaining step.
- **PyPI packaging.** The distribution name `matrix-orchestrator` was
  verified available; `pip install matrix-orchestrator` will put `mx`
  on PATH (the console script collides with nothing — the old eGenix
  `mx` distribution is a library with a different import package). The
  sdist ships the license texts, changelog, REUSE metadata and the full
  test suite so packagers can test exactly what they unpacked; a
  `publish.yml` workflow uploads via PyPI trusted publishing (OIDC, no
  stored token) on every GitHub Release, after building, `twine
  check`ing and smoke-testing the wheel it is about to upload. See
  PUBLISHING.md for the release ritual and one-time PyPI setup.

## [1.1.0] - 2026-08-11

### Added

- **`mx gen --peers K --seed N`**: sustained equal load without the full
  mesh. Each host sends to and receives from exactly K randomly-shuffled
  others — a k-regular digraph built from K shifted permutations, so the
  equal-load property is by construction, not statistical. Same per-host
  throughput and packet rate, held continuously by every host at once,
  with K sockets instead of N−1. The seed is printed and stored in the
  matrix header, so a shuffle replays exactly; a new seed re-rolls every
  path through the fabric.
- **`--streams N`** on `start`/`run`/`agent`: split each pair's rate
  across N sockets. Each socket is its own 4-tuple, which is what lets
  worker processes, NIC RSS queues and ECMP paths share a small mesh's
  load — the fix for one core pegged while the rest of the box idles.
  The offered rate is unchanged; reports still carry one row per peer.
- **`mx help`**: every switch of every command on one page, generated
  from the real parsers so it cannot drift. Bare flags gained help text.
- `mx doctor` checks `ulimit -n` on every host against the mesh size —
  a 1000-peer agent holds ~1000 sockets, exactly where the common 1024
  default dies on startup.

### Fixed

- **`--bind` on two-NIC fleets reported 100% loss.** Binding pinned the
  local sockets (including every listener) to the named NIC while
  requests still targeted the server-list addresses on the other NIC.
  `mx start --bind` now resolves the pattern on every host over ssh and
  deploys a matrix retargeted at those data-plane addresses, the same
  scheme `iperf-orchestrator` uses; it aborts up front, naming the
  hosts, if any lacks a matching interface. A hand-run agent whose bound
  address does not match the matrix refuses to start, with the
  explanation, instead of running a guaranteed-100%-loss test.
- Multi-worker agents no longer pickle the whole rates dict (~N² cells)
  into every worker, and a partial final interval no longer prints a
  summed status line that read as a host going quiet.

## [1.0.0] - 2026-08-11

First release. A matrix-only companion to `iperf_orchestrator`: it holds
a request/response traffic matrix across a fleet and reports packets per
second as the headline number.

### Added

- **`mx`**, a single standard-library Python file that is both the
  controller and the agent it deploys. Nothing to install on the servers.
- **Request/response traffic model.** Every request of `--tx-size` bytes
  is answered by the receiving host with a reply of `--rx-size` bytes.
  Packet rates match in both directions; bandwidth need not.
- **Six-command workflow** — `gen`, `start`, `status`, `summarize`,
  `stop`, `clean` — plus `run` (all of them in one shot), `check`,
  `hints`, `logs`, `collect` and `doctor`.
- **`mx gen`** builds the matrix from a server list, sized either by
  packet rate (`--pps`, or `--pps max` for unpaced) or by bandwidth
  budget (`--gbps`). The generated `matrix.csv` carries the packet sizes
  and port in its header, so no later command repeats them.
- **`mx summarize`** reports packets/sec first and Gbps alongside, in
  both payload and wire terms; splits loss into its forward and return
  legs using receiver-side counters; reports round-trip latency
  (avg/p50/p99/max) measured from an echoed timestamp, so no clock sync
  is needed; and ends with a **WHAT TO DO NEXT** section that reads the
  numbers and names the knob they point at.
- **`mx hints`** maps goals to commands, and sizes a run from a target
  rate or bandwidth (`--pps-per-host`, `--gbps-per-host`,
  `--pps-per-pair`) — including a warning when the ask exceeds what one
  agent can send.
- **`mx check`** tests a matrix against NIC bandwidth and packet-rate
  caps before anything is deployed, counting both the requests a host
  sends and the replies it owes.
- **`mx clean`** stops every agent, removes the working directory and
  verifies both. Nothing is ever installed, and no sysctl, qdisc or unit
  file is touched, so there is nothing else to undo.
- **`mx logs`** collects each agent's log; **`mx collect`** pulls the raw
  report CSVs without analysis.
- Per-pair grid CSVs (`--grid DIR`): achieved pps, delivered pps, loss
  and p99 latency, in the matrix's own N×N shape.
- **A multi-process agent.** Packet rate is CPU work, and one Python
  process is one GIL, so each host runs `--workers` worker *processes*
  (default `auto`: one per core, capped at 8). Each drives all its
  sockets from a single event loop; they split the send flows between
  them and share the listening port via `SO_REUSEPORT`. Measured on a
  4-core box, the same send loop runs at 300k pps in one thread, 199k
  across two and 57k across four — threads convoy on the GIL — against
  1.09M pps across four processes. On a five-host loopback matrix the
  change took achieved rate from 93% to 100% of target, loss from 32% to
  0.01%, and p50 latency from 393ms to 256us.
- Per-host CPU sampling: the whole box, the busiest single core, and the
  busiest agent worker's share of one core. The last one is what
  separates "the fabric is the limit" from "the agent is", and the
  summary says so in words when it happens.
- `--workers N` to scale the responder across cores, `--bind` to pin
  traffic to one NIC (`iperf_orchestrator` semantics), and `--dry-run`
  on every fleet command.
- A test suite that runs real agents over loopback and drives the whole
  fleet lifecycle through a fake `ssh`/`scp` sandbox.

### Notes

- Traffic is UDP only, deliberately: per-packet request/response sizing
  is not something TCP can honour on the wire. For TCP goodput, use
  [`iperf_orchestrator`](https://github.com/MartinGallagher-code/iperf_orchestrator).
- Python 3.6+ on the orchestrator and on every server; nothing else.

[1.2.0]: https://github.com/MartinGallagher-code/matrix_orchestrator/releases/tag/v1.2.0
[1.1.0]: https://github.com/MartinGallagher-code/matrix_orchestrator/releases/tag/v1.1.0
[1.0.0]: https://github.com/MartinGallagher-code/matrix_orchestrator/releases/tag/v1.0.0
