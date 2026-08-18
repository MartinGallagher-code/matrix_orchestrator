#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
# The operator-facing surface: dispatch, help, hints, and the env-var
# defaults. This tool's whole premise is that it is simple to drive, so
# the guidance it prints is part of the contract.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

ALL_COMMANDS="gen check hints start status summarize collect stop logs clean run doctor agent"

test_bare_invocation_points_somewhere() {
    run_mx
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "start here" "no-args help names a first command" || return 1
    assert_contains "$RUN_OUT" "mx hints" || return 1
}

test_every_command_is_dispatchable_and_documented() {
    local c
    for c in $ALL_COMMANDS; do
        run_mx "$c" --help
        assert_status 0 "$RUN_RC" "$c --help should work" || return 1
        assert_contains "$RUN_OUT" "usage: mx $c" "$c has usage" || return 1
    done
    # And every one of them is listed in the top-level help.
    run_mx --help
    for c in $ALL_COMMANDS; do
        assert_contains "$RUN_OUT" "$c" "top-level help lists $c" || return 1
    done
}

test_version() {
    run_mx --version
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "mx " || return 1
    # The full notice, GNU style: holder by full name, license, the two
    # free-software lines.
    assert_contains "$RUN_OUT" "Copyright (C) 2026 Martin J. Gallagher" || return 1
    assert_contains "$RUN_OUT" "GPL-3.0-or-later" || return 1
    assert_contains "$RUN_OUT" "no warranty" || return 1
}

test_hints_cheatsheet_covers_the_goals() {
    run_mx hints
    assert_status 0 "$RUN_RC" || return 1
    local phrase
    # Each of these is a goal an operator actually arrives with.
    for phrase in "RUN SOMETHING RIGHT NOW" "HIGH PACKET RATE" \
                  "ASYMMETRIC REQUEST/RESPONSE" "FIND THE FABRIC'S LIMIT" \
                  "PIN THE TRAFFIC TO ONE NIC" "WATCH IT LIVE" \
                  "LEAVE NO TRACE"; do
        assert_contains "$RUN_OUT" "$phrase" "hints should cover: $phrase" || return 1
    done
}

test_hints_sizes_a_run_and_prints_the_command() {
    printf 'a\nb\nc\nd\ne\n' > servers.txt
    run_mx hints --servers servers.txt --pps-per-host 400000 --tx-size 64
    assert_status 0 "$RUN_RC" || return 1
    # 400k over 4 peers = 100k per pair, and it must hand back the exact
    # command line that produces it.
    assert_contains "$RUN_OUT" "100.0 kpps" "per-pair rate" || return 1
    assert_contains "$RUN_OUT" "mx gen --servers servers.txt --pps 100000" || return 1
    assert_contains "$RUN_OUT" "mx run" || return 1
}

test_hints_warns_when_the_ask_is_beyond_one_agent() {
    printf 'a\nb\n' > servers.txt
    run_mx hints --servers servers.txt --pps-per-host 5000000
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "heads up" "an impossible ask is called out" || return 1
    assert_contains "$RUN_OUT" "--workers" "and the knob to try is named" || return 1
}

test_hints_from_a_bandwidth_goal() {
    printf 'a\nb\nc\n' > servers.txt
    run_mx hints --servers servers.txt --gbps-per-host 10 --tx-size 1400
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "per NIC direction" || return 1
    assert_contains "$RUN_OUT" "mx gen" || return 1
}

test_env_vars_supply_defaults() {
    write_servers 5300 a b > /dev/null
    MX_SERVERS="$TEST_TMPDIR/servers.txt" run_mx gen --pps 100 -o m.csv
    assert_status 0 "$RUN_RC" "MX_SERVERS should be picked up" || return 1
    MX_MATRIX="$TEST_TMPDIR/m.csv" run_mx check
    assert_status 0 "$RUN_RC" "MX_MATRIX should be picked up" || return 1
    assert_contains "$RUN_OUT" "2 hosts" || return 1
}

test_gen_to_stdout() {
    write_servers 5300 a b > /dev/null
    run_mx gen --servers servers.txt --pps 100 -o -
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" 'src\dst' || return 1
    assert_no_file matrix.csv "-o - should not write a file" || return 1
}

test_summarize_without_reports_explains_itself() {
    write_servers 5300 a b > /dev/null
    run_mx gen --servers servers.txt --pps 100
    run_mx summarize --no-collect --reports nowhere
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "mx status" "the error suggests the next step" || return 1
}

test_full_help_lists_every_switch_of_every_command() {
    run_mx help
    assert_status 0 "$RUN_RC" || return 1
    # One flag from each shared group and each command-specific set: if
    # any of these vanish from `mx help`, the reference has drifted.
    local flag
    for flag in --servers --pps --gbps --tx-size --rx-size --port --output \
                --nic-gbps --nic-mpps --pps-per-host --gbps-per-host \
                --matrix --user --jobs --remote-dir --python --dry-run \
                --interval --duration --workers --streams --bind --sndbuf \
                --rcvbuf --no-deploy --watch --reports --window --top \
                --top-hosts --grid --no-collect --for --keep --yes --host \
                --report; do
        assert_contains "$RUN_OUT" "$flag" "mx help must document $flag" || return 1
    done
    # Every command appears as a usage line.
    local c
    for c in gen check hints start status summarize collect stop logs clean run doctor agent; do
        assert_contains "$RUN_OUT" "usage: mx $c" "mx help covers $c" || return 1
    done
    assert_contains "$RUN_OUT" "MX_MATRIX" "env vars are listed" || return 1
}

test_version_is_bumped_and_consistent_everywhere() {
    # The check-in rule: every release-worthy change bumps the version,
    # and the three places it lives must agree -- the tool itself, the
    # package metadata, and the changelog.
    run_mx --version
    assert_status 0 "$RUN_RC" || return 1
    local v; v=$(printf '%s' "$RUN_OUT" | sed -n 's/^mx \([0-9][0-9.]*\)$/\1/p')
    [ -n "$v" ] || { echo "cannot parse version from: $RUN_OUT" >&2; return 1; }
    assert_contains "$(grep '^version' "$REPO_ROOT/pyproject.toml")" "\"$v\"" \
        "pyproject.toml must carry $v" || return 1
    assert_contains "$(cat "$REPO_ROOT/CHANGELOG.md")" "[$v]" \
        "CHANGELOG.md must have an entry for $v" || return 1
}

run_test test_version_is_bumped_and_consistent_everywhere
run_test test_full_help_lists_every_switch_of_every_command
run_test test_bare_invocation_points_somewhere
run_test test_every_command_is_dispatchable_and_documented
run_test test_version
run_test test_hints_cheatsheet_covers_the_goals
run_test test_hints_sizes_a_run_and_prints_the_command
run_test test_hints_warns_when_the_ask_is_beyond_one_agent
run_test test_hints_from_a_bandwidth_goal
run_test test_env_vars_supply_defaults
run_test test_gen_to_stdout
run_test test_summarize_without_reports_explains_itself
report_tests
