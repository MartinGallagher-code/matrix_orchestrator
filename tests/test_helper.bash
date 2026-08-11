# shellcheck shell=bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
#
# Shared helpers for the mx test suite.
#
# Each test file sources this, defines test_* functions, and calls
# run_test for each. Tests run in subshells so one failed assertion
# cannot abort the rest of the file.
#
# $MX is the tool under test (set by run_tests.sh). Each test gets a
# fresh temp dir at $TEST_TMPDIR and is run with that as its cwd.

set -u

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
MX="${MX:-$REPO_ROOT/matrix_orchestrator/mx.py}"
export REPO_ROOT MX

# ---- Per-test environment ---------------------------------------------------

setup_env() {
    TEST_TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/mx-tests-XXXXXX")"
    export TEST_TMPDIR
    export FAKE_BIN="$TEST_TMPDIR/bin"
    export FAKE_ROOT="$TEST_TMPDIR/hosts"
    mkdir -p "$FAKE_BIN" "$FAKE_ROOT"
    # Never let a stray environment leak into a test run.
    unset MX_MATRIX MX_SERVERS MX_REMOTE_DIR MX_REPORTS MX_JOBS MX_USER \
          MX_PYTHON MX_SSH MX_SCP IPERF_BIND SSH_USER
}

teardown_env() {
    # Anything the tests started is a child of this shell; make sure a
    # crashed test cannot leave agents blasting packets at the runner.
    pkill -f "[m]x[.]py agent --matrix $TEST_TMPDIR" 2>/dev/null
    if [ -n "${TEST_TMPDIR:-}" ] && [ -d "$TEST_TMPDIR" ]; then
        rm -rf "$TEST_TMPDIR"
    fi
}

# Run mx as a subprocess. Captures stdout+stderr in $RUN_OUT, status in $RUN_RC.
run_mx() {
    RUN_OUT="$(python3 "$MX" "$@" 2>&1)"
    RUN_RC=$?
    export RUN_OUT RUN_RC
}

# Pick a base port unlikely to collide with anything else on the runner.
pick_port() {
    echo $(( 41000 + (RANDOM % 20000) ))
}

# write_servers PORT NAME... -- a server list on distinct loopback ports.
write_servers() {
    local port="$1"; shift
    : > "$TEST_TMPDIR/servers.txt"
    local i=0 h
    for h in "$@"; do
        printf '%s=127.0.0.1:%d\n' "$h" "$((port + i))" >> "$TEST_TMPDIR/servers.txt"
        i=$((i + 1))
    done
    echo "$TEST_TMPDIR/servers.txt"
}

# Install fake ssh/scp that execute the "remote" commands locally inside
# $FAKE_ROOT/<addr>, so the whole fleet workflow (deploy, start, status,
# stop, clean) can be exercised without a network or a second machine.
install_fake_ssh() {
    cat > "$FAKE_BIN/ssh" <<'SHIM'
#!/usr/bin/env bash
set -u
target=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) shift 2 ;;
        -*) shift ;;
        *) target="$1"; shift; break ;;
    esac
done
host="${target#*@}"
root="$FAKE_ROOT/$host"
mkdir -p "$root"
cmd="$*"
printf 'ssh\t%s\t%s\n' "$host" "$cmd" >> "$FAKE_ROOT/calls.log"
# Rewrite the remote working dir into this host's sandbox.
cmd="${cmd//$MX_REMOTE_DIR/$root$MX_REMOTE_DIR}"
cd "$root" && bash -c "$cmd"
SHIM
    cat > "$FAKE_BIN/scp" <<'SHIM'
#!/usr/bin/env bash
set -u
args=()
while [ $# -gt 0 ]; do
    case "$1" in
        -o) shift 2 ;;
        -q|-r|-B) shift ;;
        *) args+=("$1"); shift ;;
    esac
done
n=${#args[@]}
[ "$n" -lt 2 ] && exit 2
dst="${args[$((n-1))]}"
srcs=("${args[@]:0:$((n-1))}")
remap() {
    case "$1" in
        *:*) local h="${1%%:*}"; h="${h#*@}"
             printf '%s\n' "$FAKE_ROOT/$h${1#*:}" ;;
        *)   printf '%s\n' "$1" ;;
    esac
}
d="$(remap "$dst")"
printf 'scp\t%s\t%s\n' "${srcs[*]}" "$dst" >> "$FAKE_ROOT/calls.log"
case "$d" in
    */) mkdir -p "$d" ;;
    *)  mkdir -p "$(dirname "$d")" ;;
esac
for s in "${srcs[@]}"; do
    cp -f "$(remap "$s")" "$d" || exit 1
done
SHIM
    chmod +x "$FAKE_BIN/ssh" "$FAKE_BIN/scp"
    : > "$FAKE_ROOT/calls.log"
    export PATH="$FAKE_BIN:$PATH"
    export MX_REMOTE_DIR="${MX_REMOTE_DIR:-/var/tmp/mx-test}"
}

# ---- Assertions -------------------------------------------------------------

assert_eq() {
    local expected="$1" actual="$2" msg="${3:-values differ}"
    [ "$expected" = "$actual" ] && return 0
    printf 'ASSERT_EQ FAILED: %s\n  expected: <%s>\n  actual:   <%s>\n' \
        "$msg" "$expected" "$actual" >&2
    return 1
}

assert_contains() {
    local haystack="$1" needle="$2" msg="${3:-substring missing}"
    [[ "$haystack" == *"$needle"* ]] && return 0
    printf 'ASSERT_CONTAINS FAILED: %s\n  needle: <%s>\n  haystack:\n%s\n' \
        "$msg" "$needle" "$haystack" >&2
    return 1
}

assert_not_contains() {
    local haystack="$1" needle="$2" msg="${3:-substring should be absent}"
    [[ "$haystack" != *"$needle"* ]] && return 0
    printf 'ASSERT_NOT_CONTAINS FAILED: %s\n  needle: <%s>\n' "$msg" "$needle" >&2
    return 1
}

assert_status() {
    local expected="$1" actual="$2" msg="${3:-exit status mismatch}"
    [ "$expected" -eq "$actual" ] && return 0
    printf 'ASSERT_STATUS FAILED: %s expected=%d actual=%d\n' \
        "$msg" "$expected" "$actual" >&2
    return 1
}

assert_file_exists() {
    local f="$1" msg="${2:-file missing}"
    [ -f "$f" ] && return 0
    printf 'ASSERT_FILE_EXISTS FAILED: %s (%s)\n' "$msg" "$f" >&2
    return 1
}

assert_no_file() {
    local f="$1" msg="${2:-file should not exist}"
    [ ! -e "$f" ] && return 0
    printf 'ASSERT_NO_FILE FAILED: %s (%s)\n' "$msg" "$f" >&2
    return 1
}

# assert_between LOW HIGH VALUE MSG -- for the inherently noisy rate
# assertions, where CI runners make tight bounds a flake generator.
assert_between() {
    local low="$1" high="$2" value="$3" msg="${4:-value out of range}"
    if python3 -c "import sys; sys.exit(0 if $low <= $value <= $high else 1)"; then
        return 0
    fi
    printf 'ASSERT_BETWEEN FAILED: %s (%s not in [%s, %s])\n' \
        "$msg" "$value" "$low" "$high" >&2
    return 1
}

# ---- Test runner ------------------------------------------------------------

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=()

run_test() {
    local name="$1"; shift
    TESTS_RUN=$((TESTS_RUN + 1))
    setup_env
    local rc=0
    (
        set +e
        cd "$TEST_TMPDIR" || exit 1
        "$name" "$@"
    )
    rc=$?
    if [ "$rc" -eq 0 ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        printf '    PASS  %s\n' "$name"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        FAILED_TESTS+=("$name")
        printf '    FAIL  %s (rc=%d)\n' "$name" "$rc"
    fi
    teardown_env
}

report_tests() {
    echo
    echo "  ----------------------------------------"
    echo "  ran:    $TESTS_RUN"
    echo "  passed: $TESTS_PASSED"
    echo "  failed: $TESTS_FAILED"
    if [ "$TESTS_FAILED" -ne 0 ]; then
        printf '    %s\n' "${FAILED_TESTS[@]}"
        exit 1
    fi
    exit 0
}
