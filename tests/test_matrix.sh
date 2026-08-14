#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
# Matrix generation, parsing, config-header round-tripping, and the
# admissibility check.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

test_gen_writes_grid_with_config_header() {
    write_servers 5300 alpha beta gamma > /dev/null
    run_mx gen --servers servers.txt --pps 5000 --tx-size 128 --rx-size 512
    assert_status 0 "$RUN_RC" "gen should succeed" || return 1
    assert_file_exists matrix.csv || return 1
    local m; m="$(cat matrix.csv)"
    assert_contains "$m" "tx_size=128 rx_size=512" "config header carries sizes" || return 1
    assert_contains "$m" "alpha=127.0.0.1:5300" "host tokens survive" || return 1
    assert_contains "$m" "alpha=127.0.0.1:5300,,5000,5000" "diagonal is empty" || return 1
    # The summary tells the operator what they just asked for.
    assert_contains "$RUN_OUT" "128 bytes out -> 512 bytes back" || return 1
    assert_contains "$RUN_OUT" "per pair" || return 1
}

test_gen_max_writes_unpaced_cells() {
    write_servers 5300 a b > /dev/null
    run_mx gen --servers servers.txt --pps max
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$(cat matrix.csv)" ",max" "unpaced cells say max" || return 1
    assert_contains "$RUN_OUT" "unpaced" || return 1
}

test_gen_gbps_sizes_the_rate() {
    write_servers 5300 a b c > /dev/null
    # 1 Gbps of 1400-byte requests per host, over 2 peers:
    #   1e9 / ((1400+66)*8) = 85266 pps per host = 42633 per pair.
    run_mx gen --servers servers.txt --gbps 1 --tx-size 1400
    assert_status 0 "$RUN_RC" || return 1
    local cell; cell=$(grep -v '^#' matrix.csv | sed -n '2p' | cut -d, -f3)
    assert_between 42000 43300 "$cell" "per-pair rate from --gbps" || return 1
}

test_gen_rejects_impossible_sizes() {
    write_servers 5300 a b > /dev/null
    run_mx gen --servers servers.txt --pps 100 --tx-size 8
    assert_status 2 "$RUN_RC" "a packet smaller than the header is refused" || return 1
    assert_contains "$RUN_OUT" "must be between" || return 1
    run_mx gen --servers servers.txt --pps 100 --rx-size 999999
    assert_status 2 "$RUN_RC" "a packet bigger than a datagram is refused" || return 1
    run_mx gen --servers servers.txt --pps 0
    assert_status 2 "$RUN_RC" "--pps 0 would mean no flows at all" || return 1
}

test_gen_needs_two_servers() {
    printf 'lonely\n' > servers.txt
    run_mx gen --servers servers.txt --pps 100
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "at least 2 servers" || return 1
}

test_gen_rejects_duplicate_servers() {
    printf 'a\nb\na\n' > servers.txt
    run_mx gen --servers servers.txt --pps 100
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "duplicate" || return 1
}

test_hand_edited_matrix_round_trips() {
    # A blanked cell removes that flow; the rest survive. This is the
    # documented way to shape a run, so it has to keep working.
    cat > matrix.csv <<'EOF'
# tx_size=64 rx_size=64 port=5300
src\dst,a,b,c
a,,100,200
b,,,300
c,400,,
EOF
    run_mx check
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "3 hosts, 4 flows" "blank cells drop flows" || return 1
}

test_check_flags_over_capacity() {
    write_servers 5300 a b c > /dev/null
    run_mx gen --servers servers.txt --pps 100000 --tx-size 1400
    run_mx check --nic-gbps 100
    assert_status 0 "$RUN_RC" "generous cap is admissible" || return 1
    assert_contains "$RUN_OUT" "admissible against the caps" || return 1
    run_mx check --nic-gbps 1
    assert_status 1 "$RUN_RC" "tight cap must fail" || return 1
    assert_contains "$RUN_OUT" "OVER CAPACITY" || return 1
    assert_contains "$RUN_OUT" "fix it" "a failure says what to do about it" || return 1
    run_mx check --nic-mpps 0.05
    assert_status 1 "$RUN_RC" "packet-rate cap is checked too" || return 1
    assert_contains "$RUN_OUT" "NIC packet rate" || return 1
}

test_check_counts_both_directions() {
    # Every request out is a reply back, so a host's ingress load is not
    # just what the matrix column says.
    write_servers 5300 a b > /dev/null
    run_mx gen --servers servers.txt --pps 1000 --tx-size 100 --rx-size 100
    run_mx check
    assert_status 0 "$RUN_RC" || return 1
    # 1000 pps out + 1000 pps in = 2000 packets/s touching the NIC.
    assert_contains "$RUN_OUT" "2.0 kpps" || return 1
}

test_missing_matrix_says_how_to_make_one() {
    run_mx check
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "matrix not found" || return 1
    assert_contains "$RUN_OUT" "mx gen" "the error names the fix" || return 1
}

test_bad_cell_is_reported_with_its_location() {
    cat > matrix.csv <<'EOF'
src\dst,a,b
a,,banana
b,10,
EOF
    run_mx check
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "a -> b" "the error points at the cell" || return 1
}

test_peers_builds_an_exactly_k_regular_graph() {
    printf 'h%02d\n' $(seq 1 12) > servers.txt
    run_mx gen --servers servers.txt --peers 4 --pps 1000 --seed 11
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "12 hosts, 48 flows" "N x K flows" || return 1
    assert_contains "$RUN_OUT" "every host identical" || return 1
    # Every host: exactly 4 out, exactly 4 in, no self-loops. This is the
    # whole promise of --peers, so it is asserted, not assumed.
    python3 - <<'EOF'
import csv, sys
rows = [r for r in csv.reader(open("matrix.csv")) if r and not r[0].startswith("#")]
hosts = rows[0][1:]
in_deg = dict((h, 0) for h in hosts)
for r in rows[1:]:
    nz = [hosts[i] for i, c in enumerate(r[1:]) if c.strip()]
    assert len(nz) == 4, "%s has %d out-flows" % (r[0], len(nz))
    assert r[0] not in nz, "%s sends to itself" % r[0]
    for d in nz:
        in_deg[d] += 1
assert set(in_deg.values()) == {4}, "in-degrees: %r" % in_deg
EOF
    assert_status 0 $? "graph must be exactly 4-regular" || return 1
}

test_peers_seed_replays_and_is_recorded() {
    printf 'h%02d\n' $(seq 1 30) > servers.txt
    run_mx gen --servers servers.txt --peers 3 --pps 100 --seed 42 -o a.csv
    run_mx gen --servers servers.txt --peers 3 --pps 100 --seed 42 -o b.csv
    cmp -s a.csv b.csv || { echo "same seed differed" >&2; return 1; }
    run_mx gen --servers servers.txt --peers 3 --pps 100 --seed 43 -o c.csv
    cmp -s a.csv c.csv && { echo "different seed matched" >&2; return 1; }
    assert_contains "$(cat a.csv)" "peers=3 seed=42" "seed recorded in the header" || return 1
}

test_peers_splits_a_gbps_budget_over_k_not_n() {
    # --gbps is a per-host budget: with 3 peers instead of 9 each flow
    # must carry 3x the rate, keeping the host total constant.
    printf 'h%02d\n' $(seq 1 10) > servers.txt
    run_mx gen --servers servers.txt --gbps 1 --tx-size 1400 --peers 3 --seed 1
    assert_status 0 "$RUN_RC" || return 1
    local cell
    cell=$(grep -v '^#' matrix.csv | sed -n 2p | tr ',' '\n' | grep -v '^$' | sed -n 3p)
    assert_between 27000 30000 "$cell" "per-flow rate is budget/K" || return 1
}

test_peers_bounds_are_enforced() {
    printf 'a\nb\nc\n' > servers.txt
    run_mx gen --servers servers.txt --peers 0
    assert_status 2 "$RUN_RC" "--peers 0 refused" || return 1
    run_mx gen --servers servers.txt --peers 3
    assert_status 2 "$RUN_RC" "--peers >= N refused" || return 1
    assert_contains "$RUN_OUT" "between 1 and 2" || return 1
    run_mx gen --servers servers.txt --peers 2
    assert_status 0 "$RUN_RC" "--peers N-1 is just the full mesh" || return 1
}

test_dwell_layers_partition_every_ordered_pair() {
    # The hard guarantee of --dwell: the layers are edge-disjoint and
    # their union is the complete digraph -- every ordered pair exactly
    # once per rotation. Asserted on the generator itself, because a
    # collision or a gap here would silently break the whole feature.
    printf 'h%02d\n' $(seq 1 11) > servers.txt
    run_mx gen --servers servers.txt --peers 3 --pps 1000 --seed 5 --dwell 30
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "layers   : 4" "ceil(10/3) = 4 layers" || return 1
    assert_contains "$RUN_OUT" "remainder: 1 peer" "10 = 3+3+3+1" || return 1
    assert_contains "$(head -8 matrix.csv)" "layers=4 dwell=30" \
        "the schedule lives in the header" || return 1
    python3 - <<'EOF'
import sys, os
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
n = 11
relabel, layers = mx.layer_partition(n, 3, 5)
assert [len(s) for s in layers] == [3, 3, 3, 1], layers
seen = set()
for shifts in layers:
    edges = mx.shift_edges(relabel, shifts)
    assert not (edges & seen), "layers share an edge"
    seen |= edges
assert seen == set((s, d) for s in range(n) for d in range(n) if s != d), \
    "union of the layers is not the complete digraph"
EOF
    assert_status 0 $? "layers must partition the N(N-1) ordered pairs" || return 1
}

test_dwell_flag_is_validated() {
    printf 'a\nb\nc\nd\n' > servers.txt
    run_mx gen --servers servers.txt --dwell 60
    assert_status 2 "$RUN_RC" "--dwell without --peers refused" || return 1
    assert_contains "$RUN_OUT" "--peers" || return 1
    run_mx gen --servers servers.txt --peers 3 --dwell 60
    assert_status 2 "$RUN_RC" "nothing to rotate when one layer covers all" || return 1
    assert_contains "$RUN_OUT" "nothing to rotate" || return 1
    run_mx gen --servers servers.txt --peers 1 --dwell 0.5
    assert_status 2 "$RUN_RC" "a dwell below the report interval floor refused" || return 1
}

test_edited_layers_header_is_refused() {
    # layers is derived from (hosts, peers), never free: an edited count
    # would break the every-pair-once guarantee, so loading refuses it.
    printf 'a\nb\nc\nd\n' > servers.txt
    run_mx gen --servers servers.txt --peers 1 --dwell 10 --seed 3
    assert_status 0 "$RUN_RC" || return 1
    sed -i 's/layers=3/layers=2/' matrix.csv
    run_mx check --matrix matrix.csv
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "partition into 3 layers" || return 1
}

test_start_refuses_a_dwell_off_the_report_tick() {
    # Switches must land on report ticks or every boundary row blurs;
    # this is caught before a single agent is deployed.
    printf 'a\nb\nc\nd\n' > servers.txt
    run_mx gen --servers servers.txt --peers 1 --dwell 7
    run_mx start --matrix matrix.csv --interval 5 --dry-run
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "whole multiple" || return 1
    assert_contains "$RUN_OUT" "Try --interval 7" "a working interval is suggested" || return 1
}

run_test test_gen_writes_grid_with_config_header
run_test test_gen_max_writes_unpaced_cells
run_test test_gen_gbps_sizes_the_rate
run_test test_gen_rejects_impossible_sizes
run_test test_gen_needs_two_servers
run_test test_gen_rejects_duplicate_servers
run_test test_hand_edited_matrix_round_trips
run_test test_check_flags_over_capacity
run_test test_check_counts_both_directions
run_test test_missing_matrix_says_how_to_make_one
run_test test_bad_cell_is_reported_with_its_location
run_test test_peers_builds_an_exactly_k_regular_graph
run_test test_peers_seed_replays_and_is_recorded
run_test test_peers_splits_a_gbps_budget_over_k_not_n
run_test test_peers_bounds_are_enforced
run_test test_dwell_layers_partition_every_ordered_pair
run_test test_dwell_flag_is_validated
run_test test_edited_layers_header_is_refused
run_test test_start_refuses_a_dwell_off_the_report_tick
report_tests
