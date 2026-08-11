<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project uses [semantic versioning](https://semver.org/).

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
- Per-host CPU sampling: the whole box, the busiest single core, and the
  agent process's own share of one core. The last one is what separates
  "the fabric is the limit" from "the agent is" — a Python process is
  GIL-bound near 100% of a core, and the summary says so when it happens.
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

[1.0.0]: https://github.com/MartinGallagher-code/matrix_orchestrator/releases/tag/v1.0.0
