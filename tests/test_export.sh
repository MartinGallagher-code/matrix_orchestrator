#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
# `mx export`: reports -> overlay samples for the datacenter layout viewer.
#
# Most of these run against a hand-written report so the arithmetic can be
# asserted exactly; the last one exports a real run end to end.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

REPORT_HEAD="ts,host,dir,peer,size,rep_size,target_pps,pps,mbps,rep_pps,rep_mbps,loss_pct,rtt_avg_us,rtt_p50_us,rtt_p99_us,rtt_max_us,cpu_pct,cpu_max_pct,agent_cpu_pct,workers,layer"

# Two hosts, two intervals each, 5% of the replies missing. The second
# interval's cpu_pct is blank on purpose: "not measured" must not be
# averaged in as zero, in the export as in everything else.
write_reports() {
    mkdir -p rep
    {
        echo "$REPORT_HEAD"
        echo "1000,alpha,tx,beta,64,64,2000.0,2000.0,1.040,1900.0,0.988,5.000,100,90,400,900,,,,,"
        echo "1000,alpha,rx,beta,64,64,,1000.0,0.520,1000.0,0.520,,,,,,,,,,"
        echo "1000,alpha,host,*,64,64,2000.0,2000.0,1.040,1900.0,0.988,5.000,,90,400,,10.0,20.0,30.0,1,"
        echo "1002,alpha,tx,beta,64,64,2000.0,2000.0,1.040,1900.0,0.988,5.000,100,90,400,900,,,,,"
        echo "1002,alpha,rx,beta,64,64,,1000.0,0.520,1000.0,0.520,,,,,,,,,,"
        echo "1002,alpha,host,*,64,64,2000.0,2000.0,1.040,1900.0,0.988,5.000,,90,400,,,20.0,30.0,1,"
    } > rep/alpha.csv
    {
        echo "$REPORT_HEAD"
        echo "1000,beta,tx,alpha,64,64,1000.0,1000.0,0.520,1000.0,0.520,0.000,50,40,80,120,,,,,"
        echo "1000,beta,rx,alpha,64,64,,1950.0,1.014,1950.0,1.014,,,,,,,,,,"
        echo "1000,beta,host,*,64,64,1000.0,1000.0,0.520,1000.0,0.520,0.000,,40,80,,5.0,9.0,12.0,1,"
    } > rep/beta.csv
}

# A host that reports its own send side and nothing else: no peer of its
# ever wrote an rx row naming it, so its receive side was never measured.
write_lonely_report() {
    mkdir -p rep
    {
        echo "$REPORT_HEAD"
        echo "1000,delta,tx,alpha,64,64,2000.0,2000.0,1.040,2000.0,1.040,0.000,100,90,400,900,,,,,"
        echo "1000,delta,host,*,64,64,2000.0,2000.0,1.040,2000.0,1.040,0.000,,90,400,,10.0,20.0,30.0,1,"
    } > rep/delta.csv
}

# A matrix the reports belong to, so export can say which hosts are silent.
write_matrix() {
    local p; p=$(pick_port)
    write_servers "$p" "$@" > /dev/null
    run_mx gen --servers servers.txt --pps 2000
}

# sample TEXT TEST TARGET -- the value of one sample, or "" if absent.
sample() {
    printf '%s\n' "$1" | awk -F'\t' -v t="$2" -v g="$3" '$1==t && $2==g {print $3; exit}'
}

test_export_declares_and_files_every_host_overlay() {
    write_reports
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    # The !test lines are what make an overlay readable the moment it loads.
    assert_contains "$RUN_OUT" "!test	mx_pps	unit=pps higher=good" || return 1
    assert_contains "$RUN_OUT" "!test	mx_loss	unit=% higher=bad" || return 1
    local t
    for t in mx_pps mx_rep_pps mx_served_pps mx_request_gbps mx_egress_gbps \
             mx_loss mx_forward_loss mx_return_loss mx_achieved mx_rtt_avg \
             mx_rtt_p50 mx_rtt_p99 mx_cpu mx_agent_cpu mx_peers mx_workers \
             mx_intervals mx_state; do
        assert_contains "$RUN_OUT" "$t	alpha	" "$t sampled for alpha" || return 1
    done
}

test_export_computes_what_summarize_prints() {
    write_reports
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    assert_eq "2000" "$(sample "$RUN_OUT" mx_pps alpha)" "requests sent" || return 1
    assert_eq "1900" "$(sample "$RUN_OUT" mx_rep_pps alpha)" "replies back" || return 1
    # What beta's own rows say arrived at alpha -- the receiver-side truth.
    assert_eq "1000" "$(sample "$RUN_OUT" mx_served_pps alpha)" "requests served" || return 1
    assert_eq "5" "$(sample "$RUN_OUT" mx_loss alpha)" "5% of replies missing" || return 1
    assert_eq "100" "$(sample "$RUN_OUT" mx_achieved alpha)" "target achieved" || return 1
    assert_eq "400" "$(sample "$RUN_OUT" mx_rtt_p99 alpha)" "worst peer p99" || return 1
    assert_eq "2" "$(sample "$RUN_OUT" mx_intervals alpha)" "two intervals" || return 1
    assert_eq "REPORTING" "$(sample "$RUN_OUT" mx_state alpha)" || return 1
}

test_export_does_not_average_a_blank_cell_as_zero() {
    write_reports
    run_mx export --no-collect --reports rep --window 0
    # alpha reported cpu once (10%) and left the second interval blank.
    # Counting the blank would make it 5.
    assert_eq "10" "$(sample "$RUN_OUT" mx_cpu alpha)" "blank is not measured" || return 1
}

test_export_names_the_hosts_that_said_nothing() {
    write_matrix alpha beta gamma
    write_reports
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    assert_eq "NO-DATA" "$(sample "$RUN_OUT" mx_state gamma)" "gamma never reported" || return 1
    assert_contains "$RUN_OUT" "mx_state	alpha	REPORTING" || return 1
}

test_export_peer_samples_carry_the_peer() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 --peers
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "mx_peer_pps	alpha	2000	peer=beta" || return 1
    assert_contains "$RUN_OUT" "mx_peer_loss	alpha	5	peer=beta" || return 1
    # Per-peer samples live under their own test names, so a `mean` over
    # mx_pps can never quietly mix per-host and per-flow numbers.
    assert_contains "$RUN_OUT" "!test	mx_peer_loss" "peer overlays are declared" || return 1
    assert_not_contains "$(sample "$RUN_OUT" mx_pps alpha)" "peer=" || return 1
}

test_export_raw_gives_one_sample_per_interval() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 --raw --no-meta
    assert_status 0 "$RUN_RC" || return 1
    local n; n=$(printf '%s\n' "$RUN_OUT" | grep -c '^mx_pps	alpha	')
    assert_eq "2" "$n" "one mx_pps sample per interval" || return 1
    assert_contains "$RUN_OUT" "ts=1002" "each sample says when it was taken" || return 1
}

test_export_window_keeps_only_recent_intervals() {
    write_reports
    run_mx export --no-collect --reports rep --window 1 --raw --no-meta
    assert_status 0 "$RUN_RC" || return 1
    local n; n=$(printf '%s\n' "$RUN_OUT" | grep -c '^mx_pps	alpha	')
    assert_eq "1" "$n" "only the last interval is inside a 1s window" || return 1
}

test_export_maps_host_names_onto_the_layout() {
    write_reports
    printf 'alpha wr01r01u07\n# beta keeps its name\n' > names.txt
    run_mx export --no-collect --reports rep --window 0 \
        --names names.txt --target-prefix DH1/A/ --run nightly-7
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "mx_pps	DH1/A/wr01r01u07	2000	run=nightly-7" || return 1
    assert_contains "$RUN_OUT" "mx_pps	DH1/A/beta	" "unmapped hosts keep their name" || return 1
}

test_export_splits_loss_into_the_leg_that_lost_it() {
    write_reports
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    # alpha sent 2000; beta counted 1950 arriving; 1900 replies came back.
    # 2.5% never arrived, another 2.5% arrived but never made it back --
    # which is the difference between a sick sender and a sick receiver.
    assert_eq "5" "$(sample "$RUN_OUT" mx_loss alpha)" "round trip" || return 1
    assert_eq "2.5" "$(sample "$RUN_OUT" mx_forward_loss alpha)" "outbound leg" || return 1
    assert_eq "2.5" "$(sample "$RUN_OUT" mx_return_loss alpha)" "return leg" || return 1
}

test_export_invents_no_zero_for_a_receive_side_nobody_measured() {
    write_reports
    write_lonely_report
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    # No peer of delta's ever wrote an rx row naming it, so "nothing served"
    # is not a measurement of zero -- and a zero here would be averaged into
    # every rack and room it sits in.
    assert_eq "" "$(sample "$RUN_OUT" mx_served_pps delta)" "served is not zero" || return 1
    assert_eq "" "$(sample "$RUN_OUT" mx_forward_loss delta)" "nor is forward loss" || return 1
    # Egress needs the reply half; the request half is known on its own and
    # is exported either way.
    assert_eq "" "$(sample "$RUN_OUT" mx_egress_gbps delta)" "egress needs both halves" || return 1
    assert_contains "$RUN_OUT" "mx_request_gbps	delta	" "requests are known alone" || return 1
    # alpha's receive side *was* measured, so it keeps both.
    assert_contains "$RUN_OUT" "mx_served_pps	alpha	" || return 1
    assert_contains "$RUN_OUT" "mx_egress_gbps	alpha	" || return 1
}

test_export_reports_a_rotation_that_has_not_come_round_yet() {
    write_matrix alpha beta gamma
    run_mx gen --servers servers.txt --pps 2000 --peers 1 --dwell 6
    assert_status 0 "$RUN_RC" || return 1
    mkdir -p rep
    {
        echo "$REPORT_HEAD"
        echo "1000,alpha,tx,beta,64,64,2000.0,2000.0,1.040,2000.0,1.040,0.000,100,90,400,900,,,,,0"
        echo "1000,alpha,host,*,64,64,2000.0,2000.0,1.040,2000.0,1.040,0.000,,90,400,,10.0,20.0,30.0,1,0"
    } > rep/alpha.csv
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    # One of the two peers alpha owes has had its turn: a floor plan drawn
    # now is showing half a mesh, and says so.
    assert_eq "50" "$(sample "$RUN_OUT" mx_coverage alpha)" "half the rotation" || return 1
}

test_export_raw_still_carries_the_derived_overlays() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 --raw --no-meta
    assert_status 0 "$RUN_RC" || return 1
    # --raw is a superset: the per-interval columns many times over, and the
    # overlays with no per-interval meaning exactly once.
    local n; n=$(printf '%s\n' "$RUN_OUT" | grep -c '^mx_pps	alpha	')
    assert_eq "2" "$n" "per-interval samples" || return 1
    n=$(printf '%s\n' "$RUN_OUT" | grep -c '^mx_served_pps	alpha	')
    assert_eq "1" "$n" "derived overlays once per host" || return 1
    assert_contains "$RUN_OUT" "mx_forward_loss	alpha	" || return 1
    assert_contains "$RUN_OUT" "mx_request_gbps	alpha	" || return 1
}

test_export_refuses_a_field_that_would_break_the_line() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 --target-prefix "hall 1/"
    assert_status 2 "$RUN_RC" "a results file is whitespace separated" || return 1
    assert_contains "$RUN_OUT" "whitespace" || return 1
    # Same for the run label, which is written onto every sample line.
    run_mx export --no-collect --reports rep --window 0 --run "last night"
    assert_status 2 "$RUN_RC" "a run label cannot hold a space" || return 1
}

test_export_json_is_one_object_per_line() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 --json -o out.ndjson
    assert_status 0 "$RUN_RC" || return 1
    assert_file_exists out.ndjson || return 1
    # Every line parses on its own, which is what makes `cat a b > c` a
    # valid results file where a JSON array would not be.
    python3 - out.ndjson <<'EOF' || return 1
import json, sys
tests = 0
for line in open(sys.argv[1]):
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    entry = json.loads(line)
    if "!test" in entry:
        assert entry.get("label"), entry
        continue
    assert entry["test"].startswith("mx_"), entry
    assert entry["target"], entry
    tests += 1
assert tests > 10, tests
EOF
    # A label with a space in it survives the JSON form intact.
    assert_contains "$(cat out.ndjson)" '"label": "Requests sent"' || return 1
}

test_export_appends_a_run_to_an_existing_results_file() {
    write_reports
    run_mx export --no-collect --reports rep --window 0 -o results.tsv --run one
    assert_status 0 "$RUN_RC" || return 1
    run_mx export --no-collect --reports rep --window 0 -o results.tsv --run two --append
    assert_status 0 "$RUN_RC" || return 1
    local body; body=$(cat results.tsv)
    assert_contains "$body" "run=one" "the first run is still there" || return 1
    assert_contains "$body" "run=two" "the second run was appended" || return 1
    # Without --append the file is replaced, not grown.
    run_mx export --no-collect --reports rep --window 0 -o results.tsv --run three
    assert_not_contains "$(cat results.tsv)" "run=one" || return 1
}

test_export_says_where_to_look_when_there_is_nothing() {
    run_mx export --no-collect --reports nowhere
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "no reports" || return 1
    assert_contains "$RUN_OUT" "mx status" "and what to do about it" || return 1
}

test_export_of_a_real_run() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 2000
    mkdir -p rep
    local h pids=()
    for h in alpha beta; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" --report "rep/$h.csv" \
            --interval 2 --duration 6 --workers 1 > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    run_mx export --no-collect --reports rep --window 30 --peers
    assert_status 0 "$RUN_RC" || return 1
    local pps; pps=$(sample "$RUN_OUT" mx_pps alpha)
    assert_between 1400 2600 "$pps" "exported pps matches the run" || return 1
    local loss; loss=$(sample "$RUN_OUT" mx_loss alpha)
    assert_between 0 5 "$loss" "loopback should not lose much" || return 1
    assert_contains "$RUN_OUT" "mx_state	beta	REPORTING" || return 1
}

run_test test_export_declares_and_files_every_host_overlay
run_test test_export_computes_what_summarize_prints
run_test test_export_does_not_average_a_blank_cell_as_zero
run_test test_export_names_the_hosts_that_said_nothing
run_test test_export_peer_samples_carry_the_peer
run_test test_export_raw_gives_one_sample_per_interval
run_test test_export_window_keeps_only_recent_intervals
run_test test_export_maps_host_names_onto_the_layout
run_test test_export_splits_loss_into_the_leg_that_lost_it
run_test test_export_invents_no_zero_for_a_receive_side_nobody_measured
run_test test_export_reports_a_rotation_that_has_not_come_round_yet
run_test test_export_raw_still_carries_the_derived_overlays
run_test test_export_refuses_a_field_that_would_break_the_line
run_test test_export_json_is_one_object_per_line
run_test test_export_appends_a_run_to_an_existing_results_file
run_test test_export_says_where_to_look_when_there_is_nothing
run_test test_export_of_a_real_run
report_tests
