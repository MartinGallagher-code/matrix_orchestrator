#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 Martin J. Gallagher

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

# Whether a process is running: not gone, and not a zombie left for a
# parent that has not reaped it yet -- kill -0 succeeds on those. Read from
# /proc, not ps: a slim container has no procps.
running() {
    [ -r "/proc/$1/status" ] && ! grep -q '^State:[[:space:]]*Z' "/proc/$1/status" 2>/dev/null
}

test_stopping_one_matrix_leaves_another_on_the_same_hosts_running() {
    # Two matrices on the same hosts, out of different --remote-dirs and on
    # different ports: two loads at once. Stopping one must not touch the
    # other -- its agents are told to stop by their own pid, and only they
    # are killed if they will not go.
    local port
    port=$(pick_port)
    setup_fleet "$port" --pps 300 || return 1
    printf 'alpha=127.0.0.1:%d\nbeta=127.0.0.2:%d\n' "$((port + 10))" "$((port + 11))" > servers-b.txt
    run_mx gen --servers servers-b.txt --pps 300 -o matrix-b.csv
    assert_status 0 "$RUN_RC" "gen b" || return 1
    local b="$MX_REMOTE_DIR-b"
    run_mx start --interval 2 --duration 40
    assert_status 0 "$RUN_RC" "start a" || return 1
    run_mx start --matrix matrix-b.csv --remote-dir "$b" --interval 2 --duration 40
    assert_status 0 "$RUN_RC" "start b" || return 1
    sleep 2
    local h pid pids=""
    for h in 127.0.0.1 127.0.0.2; do
        pids="$pids $(cat "$FAKE_ROOT/$h$b/agent.pid" 2>/dev/null)"
    done
    run_mx stop
    assert_status 0 "$RUN_RC" "stop a" || return 1
    for pid in $pids; do
        if ! running "$pid"; then
            echo "stopping matrix.csv killed matrix-b.csv's agent ($pid)" >&2
            run_mx stop --matrix matrix-b.csv --remote-dir "$b"
            return 1
        fi
    done
    run_mx stop --matrix matrix-b.csv --remote-dir "$b"
    assert_status 0 "$RUN_RC" "stop b" || return 1
    for pid in $pids; do
        if running "$pid"; then
            echo "matrix-b.csv's agent ($pid) outlived its own stop" >&2
            return 1
        fi
    done
    assert_no_file "$(host_dir 127.0.0.1)/agent.pid" "a's pid file is tidied up" || return 1
}

# Two matrices on the same hosts, a (matrix.csv, $MX_REMOTE_DIR) and b
# (matrix-b.csv, $B_DIR), both running. Sets A_PIDS and B_PIDS.
start_two_matrices() {
    local port h
    port=$(pick_port)
    setup_fleet "$port" --pps 300 || return 1
    printf 'alpha=127.0.0.1:%d\nbeta=127.0.0.2:%d\n' "$((port + 10))" "$((port + 11))" > servers-b.txt
    run_mx gen --servers servers-b.txt --pps 300 -o matrix-b.csv
    assert_status 0 "$RUN_RC" "gen b" || return 1
    B_DIR="$MX_REMOTE_DIR-b"
    run_mx start --interval 2 --duration 40
    assert_status 0 "$RUN_RC" "start a" || return 1
    run_mx start --matrix matrix-b.csv --remote-dir "$B_DIR" --interval 2 --duration 40
    assert_status 0 "$RUN_RC" "start b" || return 1
    sleep 2
    A_PIDS="" B_PIDS=""
    for h in 127.0.0.1 127.0.0.2; do
        A_PIDS="$A_PIDS $(cat "$FAKE_ROOT/$h$MX_REMOTE_DIR/agent.pid" 2>/dev/null)"
        B_PIDS="$B_PIDS $(cat "$FAKE_ROOT/$h$B_DIR/agent.pid" 2>/dev/null)"
    done
}

stop_b() { run_mx stop --matrix matrix-b.csv --remote-dir "$B_DIR" >/dev/null 2>&1; }

# Fails, and stops b, unless every pid in $1 is still running.
all_running() {
    local pid
    for pid in $1; do
        if ! running "$pid"; then
            echo "$2 ($pid)" >&2
            stop_b
            return 1
        fi
    done
}

all_gone() {
    local pid
    for pid in $1; do
        if running "$pid"; then
            echo "$2 ($pid)" >&2
            stop_b
            return 1
        fi
    done
}

test_clean_on_one_matrix_ignores_another_on_the_same_hosts() {
    # clean's last check was for any mx agent on the box: b's, still
    # running as it should be, read as a LEFTOVER of a's and failed it.
    start_two_matrices || return 1
    run_mx clean --yes
    if [ "$RUN_RC" -ne 0 ]; then
        printf '%s\n' "$RUN_OUT" >&2
        stop_b
        return 1
    fi
    assert_contains "$RUN_OUT" "clean (was: stopped)" || { stop_b; return 1; }
    assert_not_contains "$RUN_OUT" "LEFTOVER" || { stop_b; return 1; }
    all_gone "$A_PIDS" "a's agent outlived its own clean" || return 1
    all_running "$B_PIDS" "cleaning matrix.csv killed matrix-b.csv's agent" || return 1
    assert_no_file "$(host_dir 127.0.0.1)" "a's dir is gone" || { stop_b; return 1; }
    stop_b
}

test_stop_with_a_lost_pid_file_stops_only_this_matrix() {
    # Without agent.pid the agent is found by its working directory, which
    # is --remote-dir: it is started with `cd "$d"`. Not by its name, which
    # every matrix's agent shares.
    start_two_matrices || return 1
    rm -f "$FAKE_ROOT"/127.0.0.*"$MX_REMOTE_DIR"/agent.pid
    run_mx stop
    assert_status 0 "$RUN_RC" "stop a" || { stop_b; return 1; }
    assert_contains "$RUN_OUT" "stopped" || { stop_b; return 1; }
    all_running "$B_PIDS" "a lost pid file let stopping matrix.csv kill matrix-b.csv's agent" || return 1
    all_gone "$A_PIDS" "a's agent outlived its stop with the pid file lost" || return 1
    stop_b
}

test_clean_with_a_lost_pid_file_cleans_only_this_matrix() {
    start_two_matrices || return 1
    rm -f "$FAKE_ROOT"/127.0.0.*"$MX_REMOTE_DIR"/agent.pid
    run_mx clean --yes
    if [ "$RUN_RC" -ne 0 ]; then
        printf '%s\n' "$RUN_OUT" >&2
        stop_b
        return 1
    fi
    all_running "$B_PIDS" "a lost pid file let cleaning matrix.csv kill matrix-b.csv's agent" || return 1
    all_gone "$A_PIDS" "a's agent outlived its clean with the pid file lost" || return 1
    stop_b
}

test_a_lost_pid_file_without_proc_refuses_to_guess() {
    # No /proc (MX_PROC stands one in that is missing): nothing can say
    # whose agent is whose, so nothing is killed and stop says why.
    start_two_matrices || return 1
    # The host stood in has procps, just no /proc, so pgrep still sees that
    # some mx agent is running. Where the test machine has no pgrep itself
    # (python:3.6-slim), give it one that reads the real /proc.
    if ! command -v pgrep >/dev/null 2>&1; then
        cat > "$FAKE_BIN/pgrep" <<'SHIM'
#!/usr/bin/env bash
# pgrep -f PATTERN, quiet or not: enough for mx's _mx_any.
pat="${*: -1}" found=""
for e in /proc/[0-9]*; do
    [ "${e##*/}" = "$$" ] && continue
    if tr '\0' ' ' < "$e/cmdline" 2>/dev/null | grep -Eq -- "$pat"; then
        echo "${e##*/}"
        found=1
    fi
done
[ -n "$found" ]
SHIM
        chmod +x "$FAKE_BIN/pgrep"
    fi
    local h
    for h in 127.0.0.1 127.0.0.2; do
        mv "$FAKE_ROOT/$h$MX_REMOTE_DIR/agent.pid" "$TEST_TMPDIR/$h.pid"
    done
    MX_PROC="$TEST_TMPDIR/no-proc" run_mx stop
    assert_status 1 "$RUN_RC" "stop must fail rather than guess" || { stop_b; return 1; }
    assert_contains "$RUN_OUT" "kill it by hand" || { stop_b; return 1; }
    all_running "$B_PIDS" "without /proc, stopping matrix.csv killed matrix-b.csv's agent" || return 1
    all_running "$A_PIDS" "without /proc, stop guessed and killed" || return 1
    MX_PROC="$TEST_TMPDIR/no-proc" run_mx clean --yes
    assert_status 1 "$RUN_RC" "clean must fail rather than guess" || { stop_b; return 1; }
    assert_contains "$RUN_OUT" "LEFTOVER" || { stop_b; return 1; }
    assert_file_exists "$(host_dir 127.0.0.1)/matrix.csv" "clean keeps a's dir while an agent may be in it" || { stop_b; return 1; }
    all_running "$B_PIDS" "without /proc, clean killed matrix-b.csv's agent" || return 1
    MX_PROC="$TEST_TMPDIR/no-proc" run_mx doctor
    assert_contains "$RUN_OUT" "agent_running=unknown(no-/proc)" "doctor cannot tell either" || { stop_b; return 1; }
    # Put a's pid files back so its agents can be stopped properly.
    for h in 127.0.0.1 127.0.0.2; do
        mv "$TEST_TMPDIR/$h.pid" "$FAKE_ROOT/$h$MX_REMOTE_DIR/agent.pid"
    done
    run_mx stop
    stop_b
    all_gone "$A_PIDS" "a's agents outlived the tidy-up" || return 1
}

test_doctor_reports_only_this_matrix_agent() {
    start_two_matrices || return 1
    run_mx stop
    run_mx doctor
    assert_contains "$RUN_OUT" "agent_running=no" "b's agent is not a's" || { stop_b; return 1; }
    assert_not_contains "$RUN_OUT" "agent_running=yes" || { stop_b; return 1; }
    run_mx doctor --matrix matrix-b.csv --remote-dir "$B_DIR"
    assert_contains "$RUN_OUT" "agent_running=yes" "b's own doctor sees it" || { stop_b; return 1; }
    stop_b
}

# ---- MX-004: what a stale agent.pid, an odd one, or an odd --remote-dir does

# A harmless process of the test's own, standing in for whatever has the pid
# a stale agent.pid names after a reboot: same user, working directory
# elsewhere, not an mx agent. Sets VICTIM, and kills it when the test exits.
start_victim() {
    ( cd / && exec sleep 300 ) &
    VICTIM=$!
    trap 'kill "$VICTIM" 2>/dev/null' EXIT
}

# Write $1 into every host's agent.pid.
plant_pid() {
    local h
    for h in 127.0.0.1 127.0.0.2; do
        mkdir -p "$(host_dir "$h")"
        printf '%s\n' "$1" > "$(host_dir "$h")/agent.pid"
    done
}

test_a_reused_pid_is_not_taken_for_the_agent() {
    # /var/tmp keeps agent.pid across a reboot; by then its number can be any
    # process of the ssh user's. kill -0 alone said "running": status showed
    # it, doctor said agent_running=yes, start refused, and stop and clean
    # killed it.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    run_mx start --interval 2 --duration 30
    assert_status 0 "$RUN_RC" "start" || return 1
    run_mx stop
    start_victim
    plant_pid "$VICTIM"
    run_mx status
    assert_contains "$RUN_OUT" "NOT-RUNNING" "a reused pid is not the agent" || return 1
    run_mx doctor
    assert_contains "$RUN_OUT" "agent_running=no" || return 1
    run_mx start --no-deploy --interval 2 --duration 30
    assert_status 0 "$RUN_RC" "start is not refused over someone else's pid" || return 1
    run_mx stop
    assert_status 0 "$RUN_RC" "stop" || return 1
    running "$VICTIM" || { echo "stop killed a process that was not an agent" >&2; return 1; }
    plant_pid "$VICTIM"
    run_mx clean --yes
    assert_status 0 "$RUN_RC" "clean" || return 1
    running "$VICTIM" || { echo "clean killed a process that was not an agent" >&2; return 1; }
    assert_no_file "$(host_dir 127.0.0.1)" "clean still removes the directory" || return 1
}

test_reload_does_not_kill_a_reused_pid() {
    # reload's probe and its stop used kill -0 too: it took the reused pid
    # for a running agent, TERMed it, and left a host whose agent was dead
    # "unchanged, left running".
    setup_fleet "$(pick_port)" --pps 300 || return 1
    run_mx start --interval 2 --duration 60
    assert_status 0 "$RUN_RC" "start" || return 1
    local h
    for h in 127.0.0.1 127.0.0.2; do
        kill -KILL "$(cat "$(host_dir "$h")/agent.pid")"
    done
    sleep 1
    start_victim
    plant_pid "$VICTIM"
    awk -F, 'BEGIN{OFS=","} /^alpha=/{$3=900} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" "reload" || { run_mx stop; return 1; }
    assert_contains "$RUN_OUT" "not running, will start: 2 host(s)" \
        "neither host's agent is running" || { run_mx stop; return 1; }
    running "$VICTIM" || { echo "reload killed a process that was not an agent" >&2; run_mx stop; return 1; }
    run_mx stop
}

test_a_pid_file_that_is_not_a_plain_pid_signals_nothing() {
    # `kill -TERM -1` is every process the ssh user can signal (for root,
    # the host); `0` and `00` are the snippet's own process group. A pid
    # file is only ever a plain positive pid. kill, pkill and sleep are
    # stood in on the "hosts" for this test -- signal 0 still asks the real
    # kill -- so a regression prints what it would have done, not does it.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    # A real start and stop first, so each host has a stamp and reload has
    # something to restart.
    run_mx start --interval 2 --duration 30
    assert_status 0 "$RUN_RC" "start" || return 1
    run_mx stop
    mv "$FAKE_BIN/ssh" "$FAKE_BIN/ssh.real"
    cat > "$FAKE_BIN/ssh" <<'SHIM'
#!/usr/bin/env bash
args=("$@"); n=${#args[@]}
pre='kill() { if [ "$1" = -0 ]; then builtin kill "$@"; else echo "WOULD SIGNAL: kill $*"; fi; }
pkill() { echo "WOULD SIGNAL: pkill $*"; }
sleep() { :; }'
args[$((n-1))]="$pre
${args[$((n-1))]}"
exec "$(dirname "$0")/ssh.real" "${args[@]}"
SHIM
    chmod +x "$FAKE_BIN/ssh"
    # The real ssh, for stopping what reload starts.
    real_stop() { MX_SSH="$FAKE_BIN/ssh.real" run_mx stop >/dev/null 2>&1; }
    local c i=0 pgid
    pgid=$(awk '{print $5}' "/proc/$$/stat" 2>/dev/null)
    for c in -1 0 00 1 +1 "-${pgid:-1}"; do
        i=$((i + 1))
        plant_pid "$c"
        run_mx stop
        assert_not_contains "$RUN_OUT" "WOULD SIGNAL" "stop, agent.pid=$c" || return 1
        # Without /proc a live pid is all there is to go on, so this is
        # where only the plain-pid check stands in the way.
        plant_pid "$c"
        MX_PROC="$TEST_TMPDIR/no-proc" run_mx stop
        assert_not_contains "$RUN_OUT" "WOULD SIGNAL" "stop without /proc, agent.pid=$c" || return 1
        plant_pid "$c"
        MX_PROC="$TEST_TMPDIR/no-proc" run_mx status
        assert_contains "$RUN_OUT" "NOT-RUNNING" "status without /proc, agent.pid=$c" || return 1
        plant_pid "$c"
        run_mx status
        assert_contains "$RUN_OUT" "NOT-RUNNING" "status, agent.pid=$c" || return 1
        run_mx doctor
        assert_not_contains "$RUN_OUT" "agent_running=yes" "doctor, agent.pid=$c" || return 1
        # A changed row each time, so a host taken for running is restarted.
        awk -F, -v r=$((900 + i)) 'BEGIN{OFS=","} /^alpha=/{$3=r} {print}' \
            matrix.csv > m.new && mv m.new matrix.csv
        run_mx reload
        assert_not_contains "$RUN_OUT" "WOULD SIGNAL" "reload, agent.pid=$c" || { real_stop; return 1; }
        real_stop
    done
    plant_pid -1
    run_mx clean --yes
    assert_not_contains "$RUN_OUT" "WOULD SIGNAL" "clean, agent.pid=-1" || return 1
}

test_a_lost_pid_file_does_not_take_a_command_that_names_mx_for_the_agent() {
    # Without agent.pid, agents are found by working directory and command
    # line. "mx.py agent" anywhere in the command line used to be enough, so
    # a pager reading the log from inside --remote-dir was stopped as one.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    run_mx start --interval 2 --duration 30
    run_mx stop
    ( cd "$(host_dir 127.0.0.1)" && exec -a "less mx.py agent.log" sleep 300 ) &
    VICTIM=$!
    trap 'kill "$VICTIM" 2>/dev/null' EXIT
    sleep 0.5
    run_mx stop
    assert_status 0 "$RUN_RC" || return 1
    running "$VICTIM" || { echo "stop killed a pager that only named mx.py" >&2; return 1; }
    assert_contains "$RUN_OUT" "not-running" || return 1
}

test_a_lost_pid_file_finds_an_agent_file_not_named_mx_py() {
    # `mx` is one self-contained file, and a copy of it need not be called
    # mx.py; the agent is deployed under the name it has. Its agents were
    # invisible to the lost-pid search, so stop and clean left them running
    # and said so was fine.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    mkdir -p alt && cp "$MX" alt/mx
    MX="$TEST_TMPDIR/alt/mx" run_mx start --interval 2 --duration 30
    assert_status 0 "$RUN_RC" "start from a copy named mx" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/mx" "deployed as mx" || return 1
    local h pids=""
    for h in 127.0.0.1 127.0.0.2; do
        pids="$pids $(cat "$(host_dir "$h")/agent.pid")"
        rm -f "$(host_dir "$h")/agent.pid"
    done
    MX="$TEST_TMPDIR/alt/mx" run_mx stop
    local pid
    assert_status 0 "$RUN_RC" "stop" || { kill $pids 2>/dev/null; return 1; }
    assert_contains "$RUN_OUT" "stopped" "the agents are found" || { kill $pids 2>/dev/null; return 1; }
    for pid in $pids; do
        if running "$pid"; then
            echo "agent $pid outlived stop with its pid file lost" >&2
            kill "$pid"
            return 1
        fi
    done
}

test_clean_of_a_symlinked_remote_dir_reports_what_is_left() {
    # rm -rf on a symlink removes the link, not what it points to: every file
    # mx wrote stays there. clean said "nothing of mx remains"; it now says
    # what is left and fails. (What clean deletes is unchanged.)
    setup_fleet "$(pick_port)" --pps 300 || return 1
    local h
    for h in 127.0.0.1 127.0.0.2; do
        mkdir -p "$FAKE_ROOT/$h/data/real" "$(dirname "$(host_dir "$h")")"
        ln -s "$FAKE_ROOT/$h/data/real" "$(host_dir "$h")"
    done
    run_mx start --interval 2 --duration 20
    assert_status 0 "$RUN_RC" "start through the link" || return 1
    run_mx clean --yes
    assert_status 1 "$RUN_RC" "clean must not claim success" || return 1
    assert_contains "$RUN_OUT" "LEFTOVER" || return 1
    assert_contains "$RUN_OUT" "was a symlink" || return 1
    assert_not_contains "$RUN_OUT" "nothing of mx remains" || return 1
    assert_file_exists "$FAKE_ROOT/127.0.0.1/data/real/mx.py" \
        "the files are where the link pointed" || return 1
}

test_a_remote_dir_no_host_can_use_is_refused() {
    # A quoted ~ is never expanded on the hosts: ssh made a directory named
    # "~" while scp wrote to the real home. Refused up front, with the form
    # that does work; so are an empty path and /. --dry-run, so that nothing
    # would run even where the refusal is missing.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    run_mx start --dry-run --remote-dir '~/mx'
    assert_status 2 "$RUN_RC" "~/mx" || return 1
    assert_contains "$RUN_OUT" "not expanded" || return 1
    assert_contains "$RUN_OUT" "--remote-dir mx" "it names the form that works" || return 1
    run_mx clean --yes --dry-run --remote-dir '~'
    assert_status 2 "$RUN_RC" "~" || return 1
    run_mx clean --yes --dry-run --remote-dir ''
    assert_status 2 "$RUN_RC" "an empty --remote-dir" || return 1
    run_mx clean --yes --dry-run --remote-dir /
    assert_status 2 "$RUN_RC" "/" || return 1
    assert_contains "$RUN_OUT" "root directory" || return 1
    MX_REMOTE_DIR='~/mx' run_mx status --dry-run
    assert_status 2 "$RUN_RC" "MX_REMOTE_DIR=~/mx" || return 1
    if [ -s "$FAKE_ROOT/calls.log" ]; then
        echo "a refused --remote-dir still reached a host:" >&2
        cat "$FAKE_ROOT/calls.log" >&2
        return 1
    fi
}

test_run_fails_when_its_closing_stop_fails() {
    # run discarded stop's result: a host it could not stop was still
    # sending, and run exited 0.
    setup_fleet "$(pick_port)" --pps 300 || return 1
    mv "$FAKE_BIN/ssh" "$FAKE_BIN/ssh.real"
    cat > "$FAKE_BIN/ssh" <<'SHIM'
#!/usr/bin/env bash
# The stop snippet (only it starts from "not-deployed") cannot reach beta.
case "$*" in
    *127.0.0.2*status=not-deployed*)
        echo "ssh: connect to host 127.0.0.2 port 22: Connection timed out"
        exit 255 ;;
esac
exec "$(dirname "$0")/ssh.real" "$@"
SHIM
    chmod +x "$FAKE_BIN/ssh"
    run_mx run --for 4 --interval 2
    local rc=$RUN_RC out=$RUN_OUT
    mv "$FAKE_BIN/ssh.real" "$FAKE_BIN/ssh"
    run_mx stop
    assert_status 1 "$rc" "run must fail when its stop did" || { printf '%s\n' "$out" >&2; return 1; }
    assert_contains "$out" "not every agent could be stopped" || return 1
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

test_reload_restarts_only_the_host_whose_row_changed() {
    # The point of reload: an edit confined to one host's row must not
    # disturb the run on every other host.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 60
    assert_status 0 "$RUN_RC" "start" || return 1
    local a0 b0 a1 b1
    a0=$(cat "$(host_dir 127.0.0.1)/agent.pid")
    b0=$(cat "$(host_dir 127.0.0.2)/agent.pid")
    # alpha's outgoing rate only: its row, nobody else's.
    awk -F, 'BEGIN{OFS=","} /^alpha=/{$3=900} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" "reload" || return 1
    a1=$(cat "$(host_dir 127.0.0.1)/agent.pid")
    b1=$(cat "$(host_dir 127.0.0.2)/agent.pid")
    assert_contains "$RUN_OUT" "matrix changed for 1 host" || return 1
    assert_contains "$RUN_OUT" "unchanged, left running: 1 of 2" || return 1
    [ "$a0" != "$a1" ] || { echo "alpha should have restarted" >&2; return 1; }
    [ "$b0" = "$b1" ] || { echo "beta must not be touched" >&2; return 1; }
    run_mx stop
}

test_reload_restarts_every_host_when_the_header_changes() {
    # The host list and packet sizes are a wire contract -- a request
    # carries its sender's index into that list -- so a header edit
    # invalidates every agent at once.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 60
    assert_status 0 "$RUN_RC" || return 1
    local a0 b0
    a0=$(cat "$(host_dir 127.0.0.1)/agent.pid")
    b0=$(cat "$(host_dir 127.0.0.2)/agent.pid")
    sed 's/tx_size=64/tx_size=128/' matrix.csv > m.new && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "matrix changed for 2 host" || return 1
    [ "$a0" != "$(cat "$(host_dir 127.0.0.1)/agent.pid")" ] || {
        echo "alpha should have restarted" >&2; return 1; }
    [ "$b0" != "$(cat "$(host_dir 127.0.0.2)/agent.pid")" ] || {
        echo "beta should have restarted" >&2; return 1; }
    run_mx stop
}

test_reload_on_an_unchanged_matrix_touches_nothing() {
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 60
    assert_status 0 "$RUN_RC" || return 1
    local a0 b0
    a0=$(cat "$(host_dir 127.0.0.1)/agent.pid")
    b0=$(cat "$(host_dir 127.0.0.2)/agent.pid")
    run_mx reload
    assert_status 0 "$RUN_RC" "an unchanged reload is a success" || return 1
    assert_contains "$RUN_OUT" "nothing to restart" || return 1
    [ "$a0" = "$(cat "$(host_dir 127.0.0.1)/agent.pid")" ] || {
        echo "alpha must not be touched" >&2; return 1; }
    [ "$b0" = "$(cat "$(host_dir 127.0.0.2)/agent.pid")" ] || {
        echo "beta must not be touched" >&2; return 1; }
    run_mx stop
}

test_reload_keeps_the_report_across_the_restart() {
    # `start` wipes the report; a reload is one run continuing under an
    # edited matrix, so what was already measured has to survive it.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 60
    assert_status 0 "$RUN_RC" || return 1
    local report; report="$(host_dir 127.0.0.1)/report.csv"
    echo "# sentinel-from-before-the-reload" >> "$report"
    awk -F, 'BEGIN{OFS=","} /^alpha=/{$3=900} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$(cat "$report")" "sentinel-from-before-the-reload" \
        "the report survives a reload" || return 1
    run_mx stop
}

test_reload_reuses_the_flags_the_agent_was_started_with() {
    # reload takes no runtime flags: each host comes back exactly as it was
    # launched, read off the stamp it wrote at start.
    setup_fleet "$(pick_port)" --pps 500 || return 1
    run_mx start --interval 2 --duration 60 --streams 3
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$(cat "$(host_dir 127.0.0.1)/agent.stamp")" "--streams 3" \
        "the stamp records the flags" || return 1
    sed 's/tx_size=64/tx_size=128/' matrix.csv > m.new && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$(cat "$FAKE_ROOT/calls.log")" "--streams 3" \
        "the restart carries the original flags" || return 1
    run_mx stop
}

run_test test_bind_retargets_the_matrix_at_data_plane_addresses
run_test test_bind_pattern_matching_no_host_fails_before_starting
run_test test_streams_and_workers_reach_the_remote_agents
run_test test_reload_restarts_only_the_host_whose_row_changed
run_test test_reload_restarts_every_host_when_the_header_changes
run_test test_reload_on_an_unchanged_matrix_touches_nothing
run_test test_reload_keeps_the_report_across_the_restart
run_test test_reload_reuses_the_flags_the_agent_was_started_with
run_test test_start_deploys_agent_and_matrix
run_test test_start_returns_instead_of_hanging_on_the_agent
run_test test_status_shows_running_then_not_running
run_test test_start_refuses_to_double_start
run_test test_summarize_collects_from_the_hosts
run_test test_logs_are_collected
run_test test_stop_leaves_the_data_behind
run_test test_stopping_one_matrix_leaves_another_on_the_same_hosts_running
run_test test_clean_on_one_matrix_ignores_another_on_the_same_hosts
run_test test_stop_with_a_lost_pid_file_stops_only_this_matrix
run_test test_clean_with_a_lost_pid_file_cleans_only_this_matrix
run_test test_a_lost_pid_file_without_proc_refuses_to_guess
run_test test_doctor_reports_only_this_matrix_agent
run_test test_a_reused_pid_is_not_taken_for_the_agent
run_test test_reload_does_not_kill_a_reused_pid
run_test test_a_pid_file_that_is_not_a_plain_pid_signals_nothing
run_test test_a_lost_pid_file_does_not_take_a_command_that_names_mx_for_the_agent
run_test test_a_lost_pid_file_finds_an_agent_file_not_named_mx_py
run_test test_clean_of_a_symlinked_remote_dir_reports_what_is_left
run_test test_a_remote_dir_no_host_can_use_is_refused
run_test test_run_fails_when_its_closing_stop_fails
run_test test_clean_removes_every_trace
run_test test_clean_works_when_nothing_is_running
run_test test_clean_on_a_never_deployed_fleet_is_harmless
run_test test_run_does_the_whole_thing
run_test test_doctor_checks_the_hosts
run_test test_dry_run_touches_nothing
run_test test_a_matrix_by_another_name_is_still_deployed_as_matrix_csv
report_tests
