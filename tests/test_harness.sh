#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 Martin J. Gallagher

#
# The test harness itself: a per-test wall-clock limit so one hung test
# fails loudly instead of wedging the run. Each case drives a one-test
# inner suite through `run_test`, so the mechanism is exercised end to end.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

# Write a throwaway suite that runs one test function under the harness.
# BODY is the body of test_case; LIMIT is its MX_TEST_TIMEOUT.
write_inner() {
    local path="$1" body="$2"
    cat > "$path" <<EOF
set -u
source "$DIR/test_helper.bash"
test_case() { $body ; }
run_test test_case
report_tests
EOF
}

test_a_hung_test_is_killed_by_the_default_time_limit() {
    local inner="$TEST_TMPDIR/inner.sh"
    # A test that never returns; the loop lets the kill land on a short sleep
    # so nothing is left blasting after the subshell dies.
    write_inner "$inner" 'while :; do sleep 0.5; done'
    local out rc
    out=$(MX_TEST_TIMEOUT=1 bash "$inner" 2>&1)
    rc=$?
    assert_status 1 "$rc" "a timed-out suite exits non-zero" || return 1
    assert_contains "$out" "timed out" "the runner names the timeout" || return 1
    assert_contains "$out" "test_case" "and names the test that hung" || return 1
    assert_contains "$out" "failed: 1" || return 1
}

test_a_fast_test_is_unaffected_by_the_limit() {
    local inner="$TEST_TMPDIR/inner.sh"
    write_inner "$inner" 'true'
    local out rc
    out=$(MX_TEST_TIMEOUT=5 bash "$inner" 2>&1)
    rc=$?
    assert_status 0 "$rc" "a fast test still passes under the limit" || return 1
    assert_contains "$out" "PASS  test_case" || return 1
}

test_the_limit_can_be_turned_off() {
    local inner="$TEST_TMPDIR/inner.sh"
    write_inner "$inner" 'true'
    local out rc
    out=$(MX_TEST_TIMEOUT=0 bash "$inner" 2>&1)
    rc=$?
    assert_status 0 "$rc" "0 disables the limit and the test still runs" || return 1
    assert_contains "$out" "PASS  test_case" || return 1
}

test_a_finished_test_does_not_hold_the_suite_open() {
    # The watchdog's sleep outlived the watchdog it was killed with, and
    # held the suite's output open for the whole limit: run_tests.sh's
    # `out=$(bash test_x.sh)` waited MX_TEST_TIMEOUT seconds after every
    # file's last test (MX-007).
    local inner="$TEST_TMPDIR/inner.sh"
    write_inner "$inner" 'true'
    local out began ended
    began=$(date +%s)
    out=$(MX_TEST_TIMEOUT=20 bash "$inner" 2>&1)
    ended=$(date +%s)
    assert_contains "$out" "PASS  test_case" || return 1
    if [ $((ended - began)) -ge 10 ]; then
        echo "a test that passed at once held its suite open for $((ended - began))s" >&2
        return 1
    fi
}

test_teardown_kills_what_a_test_left_running() {
    # teardown's sweep, `pkill -f "[m]x[.]py agent --matrix $TEST_TMPDIR"`,
    # never matched an agent: they are started with `--matrix matrix.csv`.
    # A test that crashed left its agents sending at the runner (MX-007).
    local inner="$TEST_TMPDIR/inner.sh" pidfile="$TEST_TMPDIR/left.pid"
    write_inner "$inner" "mkdir -p \"\$TEST_TMPDIR/h\" && ( cd \"\$TEST_TMPDIR/h\" && exec -a 'python3 mx.py agent --matrix matrix.csv --host a' sleep 300 ) < /dev/null > /dev/null 2>&1 & echo \$! > '$pidfile'"
    MX_TEST_TIMEOUT=0 bash "$inner" > /dev/null 2>&1
    local left
    left=$(cat "$pidfile" 2>/dev/null)
    [ -n "$left" ] || { echo "the inner test started nothing" >&2; return 1; }
    if [ -r "/proc/$left/status" ] && ! grep -q '^State:[[:space:]]*Z' "/proc/$left/status"; then
        echo "teardown left the test's agent running ($left)" >&2
        kill "$left" 2>/dev/null
        return 1
    fi
}

run_test test_a_hung_test_is_killed_by_the_default_time_limit
run_test test_a_fast_test_is_unaffected_by_the_limit
run_test test_the_limit_can_be_turned_off
run_test test_a_finished_test_does_not_hold_the_suite_open
run_test test_teardown_kills_what_a_test_left_running
report_tests
