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

# run_agents DURATION HOST... -- start one agent per host, wait for all.
run_agents() {
    local duration="$1"; shift
    local h pids=()
    mkdir -p rep
    for h in "$@"; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration "$duration" \
            > "rep/$h.log" 2>&1 &
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

run_test test_two_agents_hit_the_target_rate
run_test test_reply_size_differs_from_request_size
run_test test_report_has_every_column_summarize_needs
run_test test_rtt_is_measured
run_test test_three_hosts_form_a_full_mesh
run_test test_summarize_reports_pps_gbps_loss_and_hints
run_test test_summarize_writes_grids
run_test test_agent_refuses_a_host_not_in_the_matrix
run_test test_unpaced_flows_send_as_fast_as_they_can
report_tests
