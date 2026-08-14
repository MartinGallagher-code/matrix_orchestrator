#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
# Real agents exchanging real packets on loopback: pacing accuracy, the
# request-in/reply-out size contract, the report CSV, and summarize.
#
# Rate bounds are deliberately loose -- CI runners are noisy neighbours
# and a tight bound here is a flake generator, not a better test.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

# run_agents DURATION HOST... [-- EXTRA-FLAGS...]
# Start one agent per host and wait for all of them. Anything after the
# host names that starts with '--' is passed through to every agent.
#
# Default --workers 1 keeps these tests deterministic: the runner has few
# cores and several agents share them, so letting each fork one worker per
# core would measure the contention, not the tool.
run_agents() {
    local duration="$1"; shift
    local h pids=() hosts=() extra=()
    for h in "$@"; do
        if [ "${#extra[@]}" -gt 0 ] || [ "${h#--}" != "$h" ]; then
            extra+=("$h")
        else
            hosts+=("$h")
        fi
    done
    [ "${#extra[@]}" -gt 0 ] || extra=(--workers 1)
    mkdir -p rep
    for h in "${hosts[@]}"; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration "$duration" \
            "${extra[@]}" > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
}

# csv_col FILE DIR COLUMN -- mean of one column over the rows of one dir.
csv_col() {
    python3 - "$1" "$2" "$3" <<'EOF'
import csv, sys
path, want, col = sys.argv[1:4]
vals = []
with open(path, newline="") as f:
    for r in csv.DictReader(f):
        if r["dir"] == want and r.get(col):
            vals.append(float(r[col]))
print("%.3f" % (sum(vals) / len(vals)) if vals else "0")
EOF
}

test_two_agents_hit_the_target_rate() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 3000 --tx-size 64 --rx-size 64
    assert_status 0 "$RUN_RC" || return 1
    run_agents 6 alpha beta

    assert_file_exists rep/alpha.csv || return 1
    local sent; sent=$(csv_col rep/alpha.csv tx pps)
    assert_between 2400 3600 "$sent" "alpha should pace near 3000 pps" || return 1
    local back; back=$(csv_col rep/alpha.csv tx rep_pps)
    assert_between 2400 3600 "$back" "every request should come back" || return 1
    # The peer's own count of what arrived: the receiver-side truth.
    local arrived; arrived=$(csv_col rep/beta.csv rx pps)
    assert_between 2400 3600 "$arrived" "beta should see alpha's requests" || return 1
}

test_reply_size_differs_from_request_size() {
    # The core contract: send x bytes, get y bytes back. With x=64 and
    # y=512 the reply direction must carry ~8x the bytes at the same
    # packet rate.
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 2000 --tx-size 64 --rx-size 512
    run_agents 6 alpha beta

    local out back ratio
    out=$(csv_col rep/alpha.csv tx mbps)
    back=$(csv_col rep/alpha.csv tx rep_mbps)
    ratio=$(python3 -c "print('%.2f' % ($back / $out))" 2>/dev/null)
    assert_between 7.0 9.0 "$ratio" "512-byte replies to 64-byte requests" || return 1
    # ...while the packet rates match, because it is one reply per request.
    local tx_pps rx_pps
    tx_pps=$(csv_col rep/alpha.csv tx pps)
    rx_pps=$(csv_col rep/alpha.csv tx rep_pps)
    ratio=$(python3 -c "print('%.2f' % ($rx_pps / $tx_pps))")
    assert_between 0.9 1.1 "$ratio" "one reply per request" || return 1
}

test_report_has_every_column_summarize_needs() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_agents 5 alpha beta
    local head; head=$(head -1 rep/alpha.csv)
    local col
    for col in ts host dir peer size target_pps pps mbps rep_pps rep_mbps \
               loss_pct rtt_avg_us rtt_p99_us cpu_pct cpu_max_pct \
               agent_cpu_pct; do
        assert_contains "$head" "$col" "report column $col" || return 1
    done
    # One host row per interval carries the CPU samples.
    local cpu; cpu=$(csv_col rep/alpha.csv host cpu_pct)
    assert_between 0 100 "$cpu" "cpu percentage is a percentage" || return 1
    # The agent's own CPU is a share of ONE core, so it can exceed 100%
    # on a multi-core box -- but never by much, thanks to the GIL.
    local agent; agent=$(csv_col rep/alpha.csv host agent_cpu_pct)
    assert_between 0 400 "$agent" "agent cpu is measured" || return 1
}

test_rtt_is_measured() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_agents 5 alpha beta
    local rtt; rtt=$(csv_col rep/alpha.csv tx rtt_avg_us)
    # Loopback: microseconds, not milliseconds, and never zero or negative.
    assert_between 1 500000 "$rtt" "loopback rtt should be plausible" || return 1
}

test_three_hosts_form_a_full_mesh() {
    local p; p=$(pick_port)
    write_servers "$p" a b c > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_agents 6 a b c
    assert_contains "$(cat rep/a.log)" "flows=2" "each host talks to both peers" || return 1
    local peers
    peers=$(python3 - <<'EOF'
import csv
seen = set()
with open("rep/a.csv", newline="") as f:
    for r in csv.DictReader(f):
        if r["dir"] == "tx":
            seen.add(r["peer"])
print(",".join(sorted(seen)))
EOF
)
    assert_eq "b,c" "$peers" "a should have a flow to b and to c" || return 1
}

test_summarize_reports_pps_gbps_loss_and_hints() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 2000 --tx-size 100 --rx-size 100
    run_agents 6 alpha beta
    run_mx summarize --reports rep --no-collect --window 30
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "REQUESTS" "packets/sec is the headline" || return 1
    assert_contains "$RUN_OUT" "kpps" || return 1
    assert_contains "$RUN_OUT" "wire" "bandwidth is reported too" || return 1
    assert_contains "$RUN_OUT" "DELIVERED" "receiver-side truth is reported" || return 1
    assert_contains "$RUN_OUT" "LOSS" || return 1
    assert_contains "$RUN_OUT" "RTT" || return 1
    assert_contains "$RUN_OUT" "WHAT TO DO NEXT" "a summary always advises" || return 1
}

test_summarize_writes_grids() {
    local p; p=$(pick_port)
    write_servers "$p" a b c > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_agents 6 a b c
    run_mx summarize --reports rep --no-collect --window 30 --grid g
    assert_status 0 "$RUN_RC" || return 1
    local f
    for f in pps_grid.csv delivered_grid.csv loss_grid.csv rtt_p99_grid.csv; do
        assert_file_exists "g/$f" || return 1
        assert_contains "$(head -1 "g/$f")" "a,b,c" "$f is an N x N grid" || return 1
    done
    # The diagonal is always empty: a host does not test itself.
    assert_contains "$(sed -n 2p g/pps_grid.csv)" "a,," "diagonal stays empty" || return 1
}

test_agent_refuses_a_host_not_in_the_matrix() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 100
    run_mx agent --matrix matrix.csv --host nope --duration 1
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "not in the matrix" || return 1
}

test_unpaced_flows_send_as_fast_as_they_can() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps max --tx-size 64
    run_agents 5 a b
    local sent; sent=$(csv_col rep/a.csv tx pps)
    # Any modern box clears 10k pps on loopback by a wide margin; the
    # point is only that 'max' is not being paced to something small.
    assert_between 10000 100000000 "$sent" "'max' should not be paced" || return 1
    assert_contains "$(cat rep/a.log)" "tx=" || return 1
}

test_multiple_workers_share_the_port_and_the_flows() {
    # Four hosts so every agent has 3 flows to shard across its workers,
    # and every worker's responder shares one port via SO_REUSEPORT.
    local p; p=$(pick_port)
    write_servers "$p" a b c d > /dev/null
    run_mx gen --servers servers.txt --pps 2000
    run_agents 7 a b c d --workers 3
    assert_contains "$(cat rep/a.log)" "workers=3" "workers are reported" || return 1
    assert_not_contains "$(cat rep/a.log)" "not listening" \
        "every worker should get the port via SO_REUSEPORT" || return 1
    # All 3 flows must be present regardless of which worker owns each.
    local peers
    peers=$(python3 - <<'EOF'
import csv
seen = set()
with open("rep/a.csv", newline="") as f:
    for r in csv.DictReader(f):
        if r["dir"] == "tx":
            seen.add(r["peer"])
print(",".join(sorted(seen)))
EOF
)
    assert_eq "b,c,d" "$peers" "every flow reports, whichever worker owns it" || return 1
    local sent; sent=$(csv_col rep/a.csv tx pps)
    assert_between 1600 2400 "$sent" "sharded flows still hit their target" || return 1
}

test_one_row_per_peer_per_interval_however_many_workers() {
    # Worker rows are merged before they are written: two workers each
    # reporting half a peer's traffic must not become two half-rate rows.
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 2000
    run_agents 7 a b --workers 4
    local dupes
    dupes=$(python3 - <<'EOF'
import collections, csv
n = collections.Counter()
with open("rep/a.csv", newline="") as f:
    for r in csv.DictReader(f):
        n[(r["ts"], r["dir"], r["peer"])] += 1
print(sum(1 for v in n.values() if v > 1))
EOF
)
    assert_eq "0" "$dupes" "no duplicate (ts, dir, peer) rows" || return 1
    # And the rate is the whole rate, not one worker's share of it.
    local arrived; arrived=$(csv_col rep/b.csv rx pps)
    assert_between 1600 2400 "$arrived" "merged rows carry the full rate" || return 1
}

test_workers_auto_picks_a_sane_number() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_agents 5 a b --workers auto
    local line; line=$(grep -m1 "^mx agent" rep/a.log)
    assert_contains "$line" "workers=" || return 1
    local n; n=$(printf '%s' "$line" | sed -n 's/.*workers=\([0-9]*\).*/\1/p')
    # One flow, so sharding caps it at 2; never zero, never unbounded.
    assert_between 1 8 "$n" "auto should pick between 1 and the cap" || return 1
}

test_streams_split_the_rate_rather_than_multiplying_it() {
    # The whole premise: N sockets per pair carry the same offered load,
    # so a run with --streams 8 must not send 8x the packets.
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 4000
    run_agents 7 a b --workers 2 --streams 8
    assert_contains "$(cat rep/a.log)" "streams=8 flows=8" \
        "8 sockets for the one peer" || return 1
    local sent; sent=$(csv_col rep/a.csv tx pps)
    assert_between 3200 4800 "$sent" "8 streams still offer 4000 pps total" || return 1
    local arrived; arrived=$(csv_col rep/b.csv rx pps)
    assert_between 3200 4800 "$arrived" "and the peer receives 4000 pps" || return 1
}

test_streams_report_as_one_row_per_peer() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 4000
    run_agents 7 a b --workers 2 --streams 8
    local dupes
    dupes=$(python3 - <<'EOF'
import collections, csv
n = collections.Counter()
with open("rep/a.csv", newline="") as f:
    for r in csv.DictReader(f):
        n[(r["ts"], r["dir"], r["peer"])] += 1
print(sum(1 for v in n.values() if v > 1))
EOF
)
    assert_eq "0" "$dupes" "streams merge into one row per peer" || return 1
}

test_streams_raise_the_worker_ceiling() {
    # Workers are capped at flows+1, and streams are what create flows --
    # this is the fix for one pegged core on a small mesh.
    python3 - <<'EOF'
import sys
sys.path.insert(0, __import__("os").environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
one = mx.resolve_workers("32", 1 * 1)      # 1 peer, 1 stream
many = mx.resolve_workers("32", 1 * 8)     # 1 peer, 8 streams
assert one == 2, "1 flow should cap workers at 2, got %d" % one
assert many == 9, "8 flows should cap workers at 9, got %d" % many
EOF
    assert_status 0 $? "streams should raise the worker cap" || return 1
}

test_streams_flag_is_validated() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_mx agent --matrix matrix.csv --host a --duration 1 --streams 0
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "at least 1" || return 1
    run_mx agent --matrix matrix.csv --host a --duration 1 --streams 99999
    assert_status 2 "$RUN_RC" "an absurd stream count is refused" || return 1
}

test_bind_refuses_an_address_the_matrix_does_not_target() {
    # The 100%-loss trap: bind the listeners to one NIC while the matrix
    # tells peers to send to another. The agent must refuse to start.
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    mkdir -p bin
    cat > bin/ip <<'EOF'
#!/usr/bin/env bash
echo '2: eth0    inet 127.0.0.1/8 scope global eth0'
echo '3: eth1    inet 127.0.0.2/8 scope global eth1'
EOF
    chmod +x bin/ip
    PATH="$PWD/bin:$PATH" run_mx agent --matrix matrix.csv --host a \
        --duration 1 --workers 1 --bind eth1
    assert_status 2 "$RUN_RC" "mismatched bind must refuse to start" || return 1
    assert_contains "$RUN_OUT" "100% loss" "the error explains the outcome" || return 1
    assert_contains "$RUN_OUT" "127.0.0.2" "...naming the bound address" || return 1
    assert_contains "$RUN_OUT" "127.0.0.1" "...and the matrix address" || return 1
    # The matching interface is accepted and actually runs.
    PATH="$PWD/bin:$PATH" run_mx agent --matrix matrix.csv --host a \
        --duration 1 --workers 1 --bind eth0
    assert_status 0 "$RUN_RC" "matching bind should run" || return 1
}

test_low_soft_fd_limit_is_raised_automatically() {
    # The classic 1024-soft-limit box: the agent must raise its own soft
    # limit (needs no privilege) and open every flow, not die on socket().
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 2000
    mkdir -p rep
    ( ulimit -Sn 100
      python3 "$MX" agent --matrix matrix.csv --host a --report rep/a.csv \
          --interval 2 --duration 5 --workers 1 --streams 150 \
          > rep/a.log 2>&1 ) &
    python3 "$MX" agent --matrix matrix.csv --host b --report rep/b.csv \
        --interval 2 --duration 5 --workers 1 --streams 150 \
        > rep/b.log 2>&1 &
    wait
    assert_contains "$(cat rep/a.log)" "fd limit: soft 100 ->" \
        "the raise is announced" || return 1
    assert_not_contains "$(cat rep/a.log)" "cannot reach" \
        "every flow must open" || return 1
    local sent; sent=$(csv_col rep/a.csv tx pps)
    assert_between 1600 2400 "$sent" "traffic flows at the target rate" || return 1
}

test_too_low_hard_fd_limit_is_refused_with_the_fix() {
    # Past the hard limit only an admin can help; the agent must refuse
    # up front with the fix named, not fail flow by flow.
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 100
    local out rc
    out=$( ( ulimit -Sn 100 -Hn 100 2>/dev/null || ulimit -n 100
             python3 "$MX" agent --matrix matrix.csv --host a --duration 2 \
                 --workers 1 --streams 150 2>&1 ) )
    rc=$?
    assert_status 2 "$rc" "hard-limited agent must refuse to start" || return 1
    assert_contains "$out" "hard limit" || return 1
    assert_contains "$out" "LimitNOFILE" "the fix is named" || return 1
}

test_workers_flag_is_validated() {
    local p; p=$(pick_port)
    write_servers "$p" a b > /dev/null
    run_mx gen --servers servers.txt --pps 1000
    run_mx agent --matrix matrix.csv --host a --duration 1 --workers banana
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "number or 'auto'" || return 1
    run_mx agent --matrix matrix.csv --host a --duration 1 --workers 0
    assert_status 2 "$RUN_RC" || return 1
}

test_layered_agents_rotate_and_cover_every_pair() {
    # The whole point of --dwell: 4 hosts at --peers 1 rotate through 3
    # layers, and after one full cycle every host has measured every
    # other -- with only ONE flow live per host at any moment. 14s at a
    # 4s dwell spans a 12s cycle from any starting phase.
    local p; p=$(pick_port)
    write_servers "$p" a b c d > /dev/null
    run_mx gen --servers servers.txt --pps 1000 --peers 1 --dwell 4 --seed 7
    assert_status 0 "$RUN_RC" || return 1
    mkdir -p rep
    local h pids=()
    for h in a b c d; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration 14 --workers 1 \
            > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    assert_contains "$(cat rep/a.log)" "layered -- 3 layers" \
        "the agent announces the rotation" || return 1
    # Every host must have visited every peer, one layer at a time.
    python3 - <<'EOF'
import csv
for h in "abcd":
    peers, layers = set(), set()
    with open("rep/%s.csv" % h, newline="") as f:
        for r in csv.DictReader(f):
            if r["dir"] == "tx" and r.get("pps"):
                peers.add(r["peer"])
                layers.add(r["layer"])
    want = set("abcd") - {h}
    assert peers == want, "%s covered %r, wanted %r" % (h, peers, want)
    assert len(layers) == 3, "%s saw layers %r" % (h, layers)
    assert all(l != "" for l in layers), "tx rows must carry their layer"
EOF
    assert_status 0 $? "a full cycle covers every ordered pair" || return 1
    # And the per-host rate never exceeds one layer's load: the rotation
    # changes who carries the pps, not how much of it there is.
    local sent; sent=$(csv_col rep/a.csv tx pps)
    assert_between 700 1300 "$sent" "per-host load stays one layer's" || return 1
}

test_layered_matrix_with_hostnames_resolves_in_the_parent() {
    # A matrix that carries names, not IP literals: every peer must be
    # resolved once in the agent's parent, never per flow in a worker --
    # a layered agent rebuilds flows at every switch, and a resolver
    # (with its timeouts) inside the workers' schedule is how a fleet
    # ends up with report.csv growing but not one stats line in the log.
    local p; p=$(pick_port)
    { echo "a=localhost:$p"; echo "b=localhost:$((p+1))"
      echo "c=localhost:$((p+2))"; echo "d=localhost:$((p+3))"; } > servers.txt
    run_mx gen --servers servers.txt --pps 1000 --peers 1 --dwell 4 --seed 7
    assert_status 0 "$RUN_RC" || return 1
    mkdir -p rep
    local h pids=()
    for h in a b c d; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration 10 --workers 2 \
            > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    assert_contains "$(cat rep/a.log)" "ts=" \
        "stats lines must reach the log" || return 1
    assert_not_contains "$(cat rep/a.log)" "cannot reach" || return 1
    local sent; sent=$(csv_col rep/a.csv tx pps)
    assert_between 700 1300 "$sent" "named peers carry traffic" || return 1
}

test_layered_summarize_reports_coverage() {
    local p; p=$(pick_port)
    write_servers "$p" a b c d > /dev/null
    run_mx gen --servers servers.txt --pps 1000 --peers 1 --dwell 4 --seed 7
    mkdir -p rep
    local h pids=()
    for h in a b c d; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration 14 --workers 1 \
            > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    run_mx summarize --reports rep --no-collect --window 60 --grid g
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "COVERAGE  12 of 12" \
        "a full cycle measures all N(N-1) pairs" || return 1
    assert_contains "$RUN_OUT" "LAYERS in the window" || return 1
    assert_contains "$RUN_OUT" "egress" "the per-host table shows egress" || return 1
    assert_file_exists g/coverage_grid.csv || return 1
    # Sums of pair means would triple the per-host rate here; the time
    # average must stay one layer's worth.
    printf '%s' "$RUN_OUT" > summ.txt
    python3 - <<'EOF'
import re
out = open("summ.txt").read()
m = re.search(r"^  a\s+([\d.]+) (k?)pps", out, re.M)
assert m, "no per-host row for a in:\n%s" % out
pps = float(m.group(1)) * (1000 if m.group(2) else 1)
assert 600 <= pps <= 1400, "per-host sent %s pps, want ~one layer" % pps
EOF
    assert_status 0 $? "per-host rate is a time average, not a sum of layers" || return 1
}

run_test test_two_agents_hit_the_target_rate
run_test test_multiple_workers_share_the_port_and_the_flows
run_test test_one_row_per_peer_per_interval_however_many_workers
run_test test_workers_auto_picks_a_sane_number
run_test test_streams_split_the_rate_rather_than_multiplying_it
run_test test_streams_report_as_one_row_per_peer
run_test test_streams_raise_the_worker_ceiling
run_test test_streams_flag_is_validated
run_test test_bind_refuses_an_address_the_matrix_does_not_target
run_test test_low_soft_fd_limit_is_raised_automatically
run_test test_too_low_hard_fd_limit_is_refused_with_the_fix
run_test test_workers_flag_is_validated
run_test test_reply_size_differs_from_request_size
run_test test_report_has_every_column_summarize_needs
run_test test_rtt_is_measured
run_test test_three_hosts_form_a_full_mesh
run_test test_layered_agents_rotate_and_cover_every_pair
run_test test_layered_matrix_with_hostnames_resolves_in_the_parent
run_test test_layered_summarize_reports_coverage
run_test test_summarize_reports_pps_gbps_loss_and_hints
run_test test_summarize_writes_grids
run_test test_agent_refuses_a_host_not_in_the_matrix
run_test test_unpaced_flows_send_as_fast_as_they_can
report_tests
