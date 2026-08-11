#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
# The whole fleet lifecycle -- start, status, summarize, logs, stop,
# clean -- driven through a fake ssh/scp that executes the "remote"
# commands locally in a per-host sandbox. Real agents, real packets, no
# second machine required.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

# Two hosts on distinct loopback addresses: distinct sandbox roots, and
# it exercises the multi-homed reply path that a wildcard-bound
# responder has to get right.
setup_fleet() {
    local port="${1:-5300}"
    install_fake_ssh
    printf 'alpha=127.0.0.1:%d\nbeta=127.0.0.2:%d\n' "$port" "$((port + 1))" \
        > servers.txt
    run_mx gen --servers servers.txt "${@:2}"
    assert_status 0 "$RUN_RC" "gen" || return 1
}

host_dir() { echo "$FAKE_ROOT/$1$MX_REMOTE_DIR"; }

test_start_deploys_agent_and_matrix() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 20
    assert_status 0 "$RUN_RC" "start" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/mx.py" "the agent is copied" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/matrix.csv" "the matrix is copied" || return 1
    assert_file_exists "$(host_dir 127.0.0.2)/mx.py" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/agent.pid" "a pid file is left" || return 1
    assert_contains "$RUN_OUT" "mx status" "start says what to do next" || return 1
    run_mx stop
}

test_start_returns_instead_of_hanging_on_the_agent() {
    # The agent outlives the ssh session; if `start` waited for it, this
    # would sit here until the duration expired.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    local began ended
    began=$(date +%s)
    run_mx start --interval 2 --duration 60
    ended=$(date +%s)
    assert_status 0 "$RUN_RC" || return 1
    if [ $((ended - began)) -ge 30 ]; then
        echo "start blocked for $((ended - began))s -- it waited for the agent" >&2
        run_mx stop
        return 1
    fi
    run_mx stop
}

test_status_shows_running_then_not_running() {
    setup_fleet "$(pick_port)" --pps 1000 || return 1
    run_mx status
    assert_contains "$RUN_OUT" "NOT-DEPLOYED" "nothing deployed yet" || return 1
    run_mx start --interval 2 --duration 30
    sleep 4
    run_mx status
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "tx=" "status shows the live ticker line" || return 1
    assert_contains "$RUN_OUT" "alpha" || return 1
    assert_contains "$RUN_OUT" "beta" || return 1
    run_mx stop
    run_mx status
    assert_contains "$RUN_OUT" "NOT-RUNNING" || return 1
}

test_start_refuses_to_double_start() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 30
    assert_status 0 "$RUN_RC" || return 1
    run_mx start --interval 2 --duration 30
    assert_status 1 "$RUN_RC" "a second start must fail" || return 1
    assert_contains "$RUN_OUT" "already running" || return 1
    run_mx stop
}

test_summarize_collects_from_the_hosts() {
    setup_fleet "$(pick_port)" --pps 1000 --tx-size 128 --rx-size 256 || return 1
    run_mx start --interval 2 --duration 30
    sleep 6
    run_mx summarize --window 30
    assert_status 0 "$RUN_RC" || return 1
    assert_file_exists reports/alpha.csv "reports land locally" || return 1
    assert_file_exists reports/beta.csv || return 1
    assert_contains "$RUN_OUT" "REQUESTS" || return 1
    assert_contains "$RUN_OUT" "128 bytes out -> 256 bytes back" || return 1
    assert_contains "$RUN_OUT" "WHAT TO DO NEXT" || return 1
    run_mx stop
}

test_logs_are_collected() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 20
    sleep 3
    run_mx logs
    assert_status 0 "$RUN_RC" || return 1
    assert_file_exists logs/alpha.log || return 1
    assert_file_exists logs/beta.log || return 1
    assert_contains "$(cat logs/alpha.log)" "mx agent" "the log has the agent banner" || return 1
    run_mx stop
}

test_stop_leaves_the_data_behind() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 30
    sleep 3
    run_mx stop
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "stopped" || return 1
    # Deliberate: stop is not clean. The report must survive.
    assert_file_exists "$(host_dir 127.0.0.1)/report.csv" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/agent.log" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/agent.pid" "the pid file is tidied up" || return 1
}

test_clean_removes_every_trace() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 30
    sleep 3
    run_mx clean --yes
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "clean" || return 1
    assert_no_file "$(host_dir 127.0.0.1)" "the working dir is gone" || return 1
    assert_no_file "$(host_dir 127.0.0.2)" || return 1
    run_mx status
    assert_contains "$RUN_OUT" "NOT-DEPLOYED" || return 1
}

test_clean_works_when_nothing_is_running() {
    # The obvious failure mode: an early exit on "not running" that skips
    # the removal and silently leaves the files behind.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 30
    sleep 2
    run_mx stop
    run_mx clean --yes
    assert_status 0 "$RUN_RC" || return 1
    assert_no_file "$(host_dir 127.0.0.1)" "clean must remove a stopped run too" || return 1
}

test_clean_on_a_never_deployed_fleet_is_harmless() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx clean --yes
    assert_status 0 "$RUN_RC" || return 1
}

test_run_does_the_whole_thing() {
    setup_fleet "$(pick_port)" --pps 1000 || return 1
    run_mx run --for 8 --interval 2
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "REQUESTS" "run summarizes" || return 1
    assert_contains "$RUN_OUT" "stopped" "run stops the agents afterwards" || return 1
    run_mx status
    assert_contains "$RUN_OUT" "NOT-RUNNING" "nothing is left running" || return 1
}

test_doctor_checks_the_hosts() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx doctor
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "Python" "doctor reports the remote python" || return 1
    assert_contains "$RUN_OUT" "agent_running=no" || return 1
}

test_dry_run_touches_nothing() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --dry-run
    assert_status 0 "$RUN_RC" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/mx.py" "--dry-run must not deploy" || return 1
}

test_a_matrix_by_another_name_is_still_deployed_as_matrix_csv() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    mv matrix.csv custom.csv
    run_mx start --matrix custom.csv --interval 2 --duration 20
    assert_status 0 "$RUN_RC" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/matrix.csv" \
        "the agent always reads matrix.csv" || return 1
    run_mx stop --matrix custom.csv
}

test_streams_and_workers_reach_the_remote_agents() {
    # Runtime knobs have to survive the trip through ssh into the agent
    # command line, or they silently do nothing on the fleet.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 20 --workers 3 --streams 4
    assert_status 0 "$RUN_RC" || return 1
    local sent; sent=$(grep -c -- "--streams 4" "$FAKE_ROOT/calls.log")
    [ "$sent" -ge 1 ] || { echo "--streams never reached the hosts" >&2; return 1; }
    sleep 4
    run_mx logs
    assert_contains "$(cat logs/alpha.log)" "streams=4" \
        "the agent should report the stream count it was given" || return 1
    assert_contains "$(cat logs/alpha.log)" "workers=3" || return 1
    run_mx stop
}

test_bind_retargets_the_matrix_at_data_plane_addresses() {
    # The two-NIC trap: the server list carries login addresses, --bind
    # names the data NIC. start must probe each host's data-plane IP over
    # ssh and retarget the deployed matrix at those, or every request
    # goes to an address nobody listens on (100% loss). Here the "login"
    # addresses are 127.0.0.x and each host's "eth1" is 127.0.1.x --
    # both loopback, so traffic on the retargeted addresses really flows.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    cat > "$FAKE_BIN/ip" <<'EOF'
#!/usr/bin/env bash
# Per-host fake `ip -o -4 addr show`: eth0 carries the login address,
# eth1 a distinct data-plane address derived from it.
addr="${FAKE_HOST_ADDR:-127.0.0.1}"
data="127.0.1.${addr##*.}"
echo "2: eth0    inet ${addr}/8 scope global eth0"
echo "3: eth1    inet ${data}/8 scope global eth1"
EOF
    chmod +x "$FAKE_BIN/ip"
    run_mx start --interval 2 --duration 25 --bind eth1
    assert_status 0 "$RUN_RC" "start with --bind should succeed" || return 1
    assert_contains "$RUN_OUT" "retargeted" "start says what it did" || return 1
    local deployed; deployed="$(cat "$(host_dir 127.0.0.1)/matrix.csv")"
    assert_contains "$deployed" "127.0.1.1" \
        "deployed matrix targets the data-plane IPs" || return 1
    assert_contains "$deployed" "127.0.1.2" || return 1
    assert_not_contains "$deployed" "alpha=127.0.0.1" \
        "login addresses are gone from the deployed matrix" || return 1
    # And the mesh actually carries traffic on those addresses.
    sleep 5
    run_mx status
    assert_contains "$RUN_OUT" "tx=" || return 1
    run_mx stop
}

test_bind_pattern_matching_no_host_fails_before_starting() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    cat > "$FAKE_BIN/ip" <<'EOF'
#!/usr/bin/env bash
echo "2: eth0    inet ${FAKE_HOST_ADDR:-127.0.0.1}/8 scope global eth0"
EOF
    chmod +x "$FAKE_BIN/ip"
    run_mx start --interval 2 --bind eth9
    assert_status 2 "$RUN_RC" "an unmatched bind pattern must abort" || return 1
    assert_contains "$RUN_OUT" "matches no interface" || return 1
    run_mx status
    assert_contains "$RUN_OUT" "NOT-DEPLOYED" "nothing was deployed" || return 1
}

run_test test_bind_retargets_the_matrix_at_data_plane_addresses
run_test test_bind_pattern_matching_no_host_fails_before_starting
run_test test_streams_and_workers_reach_the_remote_agents
run_test test_start_deploys_agent_and_matrix
run_test test_start_returns_instead_of_hanging_on_the_agent
run_test test_status_shows_running_then_not_running
run_test test_start_refuses_to_double_start
run_test test_summarize_collects_from_the_hosts
run_test test_logs_are_collected
run_test test_stop_leaves_the_data_behind
run_test test_clean_removes_every_trace
run_test test_clean_works_when_nothing_is_running
run_test test_clean_on_a_never_deployed_fleet_is_harmless
run_test test_run_does_the_whole_thing
run_test test_doctor_checks_the_hosts
run_test test_dry_run_touches_nothing
run_test test_a_matrix_by_another_name_is_still_deployed_as_matrix_csv
report_tests
