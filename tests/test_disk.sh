#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 Martin J. Gallagher

# `--disk`: a disk at each end of the round trip. Real agents on loopback
# with real files, so these assert that the I/O happens where the mode says
# it does -- which end reads, which end writes, how often -- as well as the
# plumbing: the matrix header, the data file, the fleet's prepare step, and
# what summarize, check and export make of it.
#
# The data files are kept tiny (a few MiB) so the suite stays quick; the
# size only spreads the random offsets, it does not change what is counted.

set -u
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=test_helper.bash
source "$DIR/test_helper.bash"

# run_disk_agents DURATION HOST... [--FLAG...] -- one agent per
# host, each with its own small data file, so two agents sharing this
# directory never share (or race to write) one.
run_disk_agents() {
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
            --disk-file "rep/$h.disk" --disk-size 4M \
            "${extra[@]}" > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
}

# host_col FILE COLUMN -- mean of one column over a report's host rows.
host_col() {
    python3 - "$1" "$2" <<'EOF'
import csv, sys
path, col = sys.argv[1:3]
vals = []
with open(path, newline="") as f:
    for r in csv.DictReader(f):
        if r["dir"] == "host" and r.get(col):
            vals.append(float(r[col]))
print("%.3f" % (sum(vals) / len(vals)) if vals else "0")
EOF
}

# disk_io FILE MODE -- "reads expected writes expected": the disk ops the
# report's host rows counted, beside the ops its own packet rows say the
# mode promises -- per --disk MODE, a read for every payload this host sent
# and a write for every one it received. Compared interval by interval, so
# how fast the runner's disk is (and so whether the agents kept up with
# the matrix's target) does not enter into it: what is asserted is which
# end does which I/O, per packet.
disk_io() {
    python3 - "$1" "$2" <<'EOF'
import collections, csv, sys
path, mode = sys.argv[1:3]
reqs, reps = mode in ("requests", "both"), mode in ("replies", "both")
host, pkt = {}, collections.defaultdict(lambda: collections.defaultdict(float))
with open(path, newline="") as f:
    for r in csv.DictReader(f):
        if r["dir"] == "host":
            host[r["ts"]] = r
        elif r["dir"] in ("tx", "rx"):
            for col in ("pps", "rep_pps"):
                pkt[r["ts"]][r["dir"] + "_" + col] += float(r[col] or 0)
rd = wr = rd_want = wr_want = 0.0
for ts, r in host.items():
    p = pkt[ts]
    rd += float(r["disk_rd_iops"] or 0)
    wr += float(r["disk_wr_iops"] or 0)
    # Requests: the requester reads each one it sends, the responder
    # writes each one that arrives. Replies: the responder reads each one
    # it sends back, the requester writes each one that returns.
    rd_want += reqs * p["tx_pps"] + reps * p["rx_rep_pps"]
    wr_want += reqs * p["rx_pps"] + reps * p["tx_rep_pps"]
n = max(1, len(host))
print("%.1f %.1f %.1f %.1f" % (rd / n, rd_want / n, wr / n, wr_want / n))
EOF
}

# assert_io ACTUAL EXPECTED MSG -- one disk op per payload, to within 5%.
# Something must have been sent: matching zero against zero would pass a
# run whose intervals all came out partial, having measured nothing. The
# floor is low on purpose -- a slow runner disk makes the agents fall far
# short of the matrix's rate, and the accounting must hold all the same.
assert_io() {
    if python3 -c "import sys; a, e = float(sys.argv[1]), float(sys.argv[2]); sys.exit(0 if e >= 10 and abs(a / e - 1) <= 0.05 else 1)" "$1" "$2"; then
        return 0
    fi
    printf 'ASSERT_IO FAILED: %s (%s disk ops/s against %s payloads/s)\n' \
        "$3" "$1" "$2" >&2
    return 1
}

two_hosts() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt "$@"
    assert_status 0 "$RUN_RC" "gen $*" || return 1
}

# ---- the matrix ------------------------------------------------------------

test_gen_records_the_disk_mode_in_the_header() {
    two_hosts --pps 1000 --tx-size 128 --rx-size 8192 --disk replies || return 1
    assert_contains "$(sed -n 2p matrix.csv)" \
        "tx_size=128 rx_size=8192 port=5300 disk=replies" \
        "disk= rides the sizes line" || return 1
    assert_contains "$(cat matrix.csv)" "read from the responder's disk" \
        "the header says what it means in words" || return 1
    # gen sizes the disk work as it sizes the network: 1000 pps of 8 KB
    # replies each way is 1k reads and 1k writes of two blocks per host.
    assert_contains "$RUN_OUT" "1.0k IOPS / 8.2 MB/s read" || return 1
    # And no --disk writes no disk= at all, so old matrices stay byte-identical.
    run_mx gen --servers servers.txt --pps 1000 -o plain.csv
    assert_not_contains "$(cat plain.csv)" "disk=" || return 1
}

test_disk_mode_is_validated() {
    two_hosts --pps 1000 || return 1
    run_mx gen --servers servers.txt --pps 1000 --disk sideways
    assert_status 2 "$RUN_RC" "argparse refuses an unknown mode" || return 1
    # A hand edit is the other way in, and is refused with the file named.
    sed 's/port=5300/port=5300 disk=sideways/' matrix.csv > m.new && mv m.new matrix.csv
    run_mx check
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "bad disk='sideways'" || return 1
    assert_contains "$RUN_OUT" "requests, replies, both" "the choices are named" || return 1
}

test_header_only_payloads_are_called_out() {
    two_hosts --pps 1000 --tx-size 32 --disk requests || return 1
    assert_contains "$RUN_OUT" "nothing to put on disk: raise --tx-size" || return 1
}

test_check_sizes_every_hosts_disk() {
    local p; p=$(pick_port)
    write_servers "$p" a b c > /dev/null
    # Each host sends 2 x 1000 requests and serves 2 x 1000: with replies on
    # disk that is 2k block-pair reads (serving) and 2k writes (receiving).
    run_mx gen --servers servers.txt --pps 1000 --rx-size 8192 --disk replies
    run_mx check
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "disk=replies" || return 1
    assert_contains "$RUN_OUT" "read IOPS" || return 1
    local row; row=$(printf '%s\n' "$RUN_OUT" | grep -E '^  a +[0-9.]+k +[0-9.]+ MB/s')
    assert_contains "$row" "2.0k    16.4 MB/s" "reads: 2k x 8 KiB" || return 1
    # Without --disk there is no disk section at all.
    run_mx gen --servers servers.txt --pps 1000 --rx-size 8192
    run_mx check
    assert_not_contains "$RUN_OUT" "read IOPS" || return 1
}

test_fingerprint_is_unchanged_without_disk() {
    # A matrix with no disk= must stamp exactly as it did before the option
    # existed, or upgrading mx would restart a whole running fleet on the
    # next reload for nothing; with one, every host must change.
    two_hosts --pps 1000 || return 1
    python3 - <<'EOF'
import hashlib, os, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
m = mx.load_matrix("matrix.csv")
assert not any(t.startswith("disk=") for t in mx._fleet_terms(m))
# The 1.9.0 formula, verbatim: fleet terms, me=, then one flow= per peer.
terms = ["mx-stamp-v1", "tx=%d" % m.tx_size, "rx=%d" % m.rx_size,
         "port=%d" % m.port]
for h in m.hosts:
    terms.append("host=%s,%s,%d" % (h, m.addrs[h], m.ports[h]))
terms.append("me=alpha")
for peer, pps in m.peers_of("alpha"):
    terms.append("flow=%s,%g" % (peer, pps))
old = hashlib.sha256("\n".join(terms).encode()).hexdigest()[:16]
assert mx.host_fingerprint(m, "alpha") == old, "off-disk stamp drifted"
m.disk = "replies"
assert mx.host_fingerprint(m, "alpha") != old, "disk mode must change it"
EOF
    assert_status 0 $? "stamps" || return 1
}

# ---- the data file ---------------------------------------------------------

test_prepare_disk_writes_real_data_once_and_reuses_it() {
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 2M
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "wrote 2.0 MiB" || return 1
    python3 - <<'EOF'
import os, zlib
st = os.stat("d.dat")
assert st.st_size == 2 << 20, st.st_size
# Allocated, not a sparse file: a hole reads back free of charge.
assert st.st_blocks * 512 >= st.st_size, "the file is sparse"
data = open("d.dat", "rb").read()
# Incompressible, so a compressing filesystem cannot shrink the reads...
assert len(zlib.compress(data)) > 0.99 * len(data), "the data compresses"
# ...and no two blocks alike, so a deduplicating one cannot either.
blocks = set(data[i:i + 4096] for i in range(0, len(data), 4096))
assert len(blocks) == len(data) // 4096, "duplicate blocks"
EOF
    assert_status 0 $? "the file is real, incompressible data" || return 1
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 2M
    assert_contains "$RUN_OUT" "reusing d.dat" "a second prepare is free" || return 1
    # A different size is a different file: rewritten, not trusted.
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 3M
    assert_contains "$RUN_OUT" "wrote 3.0 MiB" || return 1
    assert_eq "0" "$(find . -maxdepth 1 -name 'd.dat.*.tmp' | wc -l | tr -d ' ')" \
        "no temp file left behind" || return 1
    # A sparse file of the right size is not mistaken for a prepared one.
    rm -f d.dat && truncate -s 2M d.dat
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 2M
    assert_contains "$RUN_OUT" "wrote 2.0 MiB" "a sparse file is rewritten" || return 1
}

test_a_small_filesystem_takes_a_file_that_fits() {
    # The refusal keeps headroom in proportion to the filesystem, not a flat
    # amount: a 64 MiB tmpfs (/dev/shm in a container) takes a 1 MiB file,
    # and refuses one that would leave it all but full.
    python3 - <<'EOF'
import collections, contextlib, io, os, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
vfs = collections.namedtuple("vfs", "f_bavail f_frsize")
os.statvfs = lambda p: vfs(64 << 20 >> 12, 4096)
with contextlib.redirect_stdout(io.StringIO()):
    assert mx.disk_prepare("small.dat", 1 << 20), "a 1 MiB file was refused"
err = io.StringIO()
try:
    with contextlib.redirect_stderr(err):
        mx.disk_prepare("big.dat", 63 << 20)
    raise AssertionError("a file filling the filesystem was accepted")
except SystemExit as exc:
    assert exc.code == 2, exc.code
msg = err.getvalue()
assert "63.0 MiB" in msg and "64.0 MiB free" in msg, msg
assert not os.path.exists("big.dat"), "nothing is written when refused"
EOF
    assert_status 0 $? "headroom scales with the filesystem" || return 1
}

test_disk_size_is_validated() {
    run_mx agent --prepare-disk --disk-file d.dat --disk-size banana
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "--disk-size wants a size" || return 1
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 64K
    assert_status 2 "$RUN_RC" "a file the disk cache holds whole is refused" || return 1
    python3 - <<'EOF'
import os, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
assert mx.resolve_disk_size("1G") == 1 << 30
assert mx.resolve_disk_size("512MiB") == 512 << 20
assert mx.resolve_disk_size("1.5G") == 3 << 29
assert mx.resolve_disk_size(str((1 << 20) + 100)) == 1 << 20, "whole blocks"
EOF
    assert_status 0 $? "size parsing" || return 1
}

test_buffered_fallback_still_does_the_io() {
    # Where O_DIRECT is refused the I/O goes through the page cache instead,
    # and each block is dropped from it after use. Forced here, since the
    # runner's filesystem may well take O_DIRECT.
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 2M
    python3 - <<'EOF'
import os, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
d = mx.DiskIO("d.dat", 2 << 20, direct=False)
assert not d.direct
assert d.evict == hasattr(os, "posix_fadvise"), d.mode()
got = d.read(8160)
assert len(got) == 8160
pkt = b"\0" * mx.HDR_SIZE + b"x" * 1000
assert d.write(pkt)
assert (d.rd_ops, d.wr_ops) == (1, 1)
# Buffered I/O moves the payload exactly; direct moves whole blocks.
assert (d.rd_bytes, d.wr_bytes) == (8160, 1000), (d.rd_bytes, d.wr_bytes)
assert sum(d.rd_hist) == 1 and sum(d.wr_hist) == 1, "every op is timed"
d.close()
# Direct, where the filesystem allows it: whole blocks.
ok, why = mx.disk_probe("d.dat")
if ok:
    d = mx.DiskIO("d.dat", 2 << 20, direct=True)
    assert d.direct
    assert len(d.read(8160)) == 8160
    assert d.write(pkt)
    assert (d.rd_bytes, d.wr_bytes) == (8192, 4096), (d.rd_bytes, d.wr_bytes)
    d.close()
# No O_DIRECT on the platform: said in words, not crashed on.
saved = getattr(os, "O_DIRECT", None)
if saved is not None:
    del os.O_DIRECT
try:
    ok, why = mx.disk_probe("d.dat")
    assert not ok and "O_DIRECT" in why, why
finally:
    if saved is not None:
        os.O_DIRECT = saved
EOF
    assert_status 0 $? "buffered and direct I/O are both counted" || return 1
}

test_memory_filesystem_is_called_out() {
    # tmpfs takes O_DIRECT on recent kernels, so the probe passes -- and
    # there is still no disk. The agent must say so rather than report RAM
    # as a disk.
    [ -d /dev/shm ] && [ -w /dev/shm ] || return 0
    python3 -c "
import os, sys; sys.path.insert(0, os.environ['REPO_ROOT'] + '/matrix_orchestrator')
import mx; sys.exit(0 if mx.disk_fs_type('/dev/shm') in mx.MEMORY_FILESYSTEMS else 1)" \
        || return 0
    two_hosts --pps 200 --rx-size 4096 --disk replies || return 1
    local f="/dev/shm/mx-test-$$-$RANDOM"
    python3 "$MX" agent --matrix matrix.csv --host alpha --report a.csv \
        --interval 2 --duration 2 --workers 1 --disk-file "$f" \
        --disk-size 1M > a.log 2>&1
    rm -f "$f"
    assert_contains "$(cat a.log)" "memory, not a disk" || return 1
}

# ---- real agents -----------------------------------------------------------

test_replies_are_read_by_the_responder_and_written_by_the_requester() {
    # Only alpha sends, so each side's disk work is one job: beta reads (it
    # answers every request from disk), alpha writes (it stores every
    # reply). An 8 KB reply is a two-block read and a two-block write.
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 500 --tx-size 128 --rx-size 8192 \
        --disk replies
    awk -F, 'BEGIN{OFS=","} /^beta=/{$2=""} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    run_disk_agents 7 alpha beta
    assert_contains "$(cat rep/alpha.log)" "disk=replies" "the banner says so" || return 1
    assert_contains "$(cat rep/alpha.log)" "disk: replies -- every reply's payload" || return 1
    assert_not_contains "$(cat rep/alpha.log rep/beta.log)" "failed" || return 1
    local rd rd_want wr wr_want
    read -r rd rd_want wr wr_want < <(disk_io rep/beta.csv replies)
    assert_io "$rd" "$rd_want" "beta reads one reply per request it serves" || return 1
    assert_eq "0.0" "$wr" "beta writes nothing: it sends no requests" || return 1
    read -r rd rd_want wr wr_want < <(disk_io rep/alpha.csv replies)
    assert_io "$wr" "$wr_want" "alpha writes every reply it gets back" || return 1
    assert_eq "0.0" "$rd" "alpha reads nothing: it serves no requests" || return 1
    # Two blocks per op, whether direct (exactly 8192) or buffered (8160).
    local per_op
    per_op=$(python3 -c "print($(host_col rep/beta.csv disk_rd_mb_s) * 1e6 / $(host_col rep/beta.csv disk_rd_iops))")
    assert_between 8000 8300 "$per_op" "bytes per read" || return 1
    # And the network test still runs: the requests are still answered.
    # (How many depends on the disk -- these runs are often disk-bound.)
    assert_between 10 600 "$(host_col rep/alpha.csv rep_pps)" \
        "replies still come back" || return 1
    # Latency and busy are measured, not left blank.
    assert_between 1 10000000 "$(host_col rep/beta.csv disk_rd_avg_us)" "read latency" || return 1
    assert_between 0.001 100 "$(host_col rep/beta.csv disk_busy_pct)" "busy share" || return 1
}

test_requests_are_read_by_the_requester_and_written_by_the_responder() {
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 500 --tx-size 4096 --rx-size 64 \
        --disk requests
    awk -F, 'BEGIN{OFS=","} /^beta=/{$2=""} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    run_disk_agents 7 alpha beta
    local rd rd_want wr wr_want
    read -r rd rd_want wr wr_want < <(disk_io rep/alpha.csv requests)
    assert_io "$rd" "$rd_want" "alpha reads every request it sends" || return 1
    assert_eq "0.0" "$wr" "alpha writes nothing: nothing is sent to it" || return 1
    read -r rd rd_want wr wr_want < <(disk_io rep/beta.csv requests)
    assert_io "$wr" "$wr_want" "beta writes every request before answering" || return 1
    assert_eq "0.0" "$rd" "beta reads nothing: its replies are header-only" || return 1
}

test_both_puts_every_payload_on_disk_at_both_ends() {
    # A full mesh of two: each host reads for its requests and for its
    # replies, and writes for the requests arriving and the replies coming
    # back -- four I/Os per round trip, two at each end.
    two_hosts --pps 200 --tx-size 1024 --rx-size 1024 --disk both || return 1
    run_disk_agents 7 alpha beta --workers 2
    local h rd rd_want wr wr_want req_rd rep_rd _
    for h in alpha beta; do
        read -r rd rd_want wr wr_want < <(disk_io "rep/$h.csv" both)
        assert_io "$rd" "$rd_want" "$h reads every payload it sends" || return 1
        assert_io "$wr" "$wr_want" "$h writes every payload it receives" || return 1
        # Both halves really are in that total: on a symmetric mesh the
        # request reads and the reply reads are each about half of it,
        # however far short of the target a slow disk held the run.
        read -r _ req_rd _ _ < <(disk_io "rep/$h.csv" requests)
        read -r _ rep_rd _ _ < <(disk_io "rep/$h.csv" replies)
        assert_between 0.3 0.7 "$(python3 -c "print($req_rd / $rd_want)")" \
            "$h's request reads are a real share of its reads" || return 1
        assert_between 0.3 0.7 "$(python3 -c "print($rep_rd / $rd_want)")" \
            "$h's reply reads are a real share of its reads" || return 1
    done
    # The per-host figures are merged across workers, not one worker's.
    assert_contains "$(grep -m1 'disk=rd' rep/alpha.log)" "busy=" || return 1
}

test_header_only_packets_cost_no_io() {
    two_hosts --pps 1000 --tx-size 32 --rx-size 32 --disk both || return 1
    run_disk_agents 5 alpha beta
    assert_between 0 0 "$(host_col rep/alpha.csv disk_rd_iops)" || return 1
    assert_between 0 0 "$(host_col rep/alpha.csv disk_wr_iops)" || return 1
    assert_between 700 1300 "$(host_col rep/alpha.csv rep_pps)" \
        "the network half runs as ever" || return 1
}

test_disk_columns_stay_blank_without_disk() {
    two_hosts --pps 1000 --rx-size 8192 || return 1
    run_disk_agents 5 alpha beta
    assert_contains "$(head -1 rep/alpha.csv)" "disk_busy_pct" "the columns exist" || return 1
    python3 - <<'EOF'
import csv
for r in csv.DictReader(open("rep/alpha.csv", newline="")):
    for k in ("disk_rd_iops", "disk_wr_iops", "disk_busy_pct"):
        assert r[k] == "", "%s=%r without --disk" % (k, r[k])
EOF
    assert_status 0 $? "blank, not zero" || return 1
    assert_no_file rep/alpha.disk "no data file without --disk" || return 1
    assert_not_contains "$(cat rep/alpha.log)" "disk=" || return 1
}

test_summarize_and_export_report_the_disks() {
    two_hosts --pps 1000 --tx-size 128 --rx-size 8192 --disk replies || return 1
    run_disk_agents 7 alpha beta
    run_mx summarize --reports rep --no-collect --window 30
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "DISK READ" || return 1
    assert_contains "$RUN_OUT" "DISK WRITE" || return 1
    assert_contains "$RUN_OUT" "DISK BUSY" || return 1
    assert_contains "$RUN_OUT" "RTT includes the responder's disk read" || return 1
    assert_contains "$(printf '%s\n' "$RUN_OUT" | grep -m1 '^  host ')" " disk" \
        "the per-host table gains a disk column" || return 1
    run_mx export --no-collect --reports rep --window 0
    assert_status 0 "$RUN_RC" || return 1
    local t
    for t in mx_disk_read_mbs mx_disk_write_mbs mx_disk_read_iops \
             mx_disk_write_iops mx_disk_read_p99 mx_disk_write_p99 mx_disk_busy; do
        assert_contains "$RUN_OUT" "$t	alpha	" "$t exported" || return 1
    done
    assert_contains "$RUN_OUT" "disk=replies" "the export header names the mode" || return 1
}

test_summarize_without_disk_says_nothing_about_disks() {
    two_hosts --pps 1000 || return 1
    run_disk_agents 5 alpha beta
    run_mx summarize --reports rep --no-collect --window 30
    assert_not_contains "$RUN_OUT" "DISK" || return 1
    run_mx export --no-collect --reports rep --window 0
    assert_not_contains "$RUN_OUT" "mx_disk_" "no disk overlays without --disk" || return 1
}

test_a_disk_bound_run_names_the_disk_not_the_fabric() {
    # Hand-written reports: two hosts whose workers are blocked on the disk
    # 90% of the time, with loss. The advice has to point at the disk.
    mkdir -p rep
    local head="ts,host,dir,peer,size,rep_size,target_pps,pps,mbps,rep_pps,rep_mbps,loss_pct,rtt_avg_us,rtt_p50_us,rtt_p99_us,rtt_max_us,cpu_pct,cpu_max_pct,agent_cpu_pct,workers,layer,disk_rd_iops,disk_rd_mb_s,disk_rd_avg_us,disk_rd_p99_us,disk_wr_iops,disk_wr_mb_s,disk_wr_avg_us,disk_wr_p99_us,disk_busy_pct"
    local h peer
    for h in alpha beta; do
        peer=beta; [ "$h" = beta ] && peer=alpha
        {
            echo "$head"
            echo "1000,$h,tx,$peer,128,8192,2000.0,1500.0,1.5,1350.0,88.0,10.0,900,800,9000,20000,,,,,,,,,,,,,,"
            echo "1000,$h,rx,$peer,128,8192,,1400.0,1.4,1400.0,90.0,,,,,,,,,,,,,,,,,,,"
            echo "1000,$h,host,*,128,8192,2000.0,1500.0,1.5,1350.0,88.0,10.0,,800,9000,,20.0,30.0,15.0,1,,1400.0,11.5,600,4096,1350.0,11.0,650,5000,90.0"
        } > "rep/$h.csv"
    done
    two_hosts --pps 2000 --tx-size 128 --rx-size 8192 --disk replies || return 1
    run_mx summarize --reports rep --no-collect --window 30
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "held up by the disk" || return 1
    # At depth 1 the first lever is a deeper queue per worker.
    assert_contains "$RUN_OUT" "--disk-depth 8" "the lever is named" || return 1
    assert_not_contains "$RUN_OUT" "that is the fabric dropping" \
        "loss on a disk-bound run is not blamed on the fabric" || return 1
    assert_not_contains "$RUN_OUT" "a flow may have failed to open" || return 1
}

# ---- --disk-depth: more than one I/O in flight per worker -----------------

test_disk_pool_runs_every_kind_of_job() {
    # The pool on its own: every job kind comes back, in the shape the
    # event loop expects, with its payload copied out of the thread's buffer
    # and every operation counted -- and never more outstanding than its
    # bound. Then every thread stops.
    run_mx agent --prepare-disk --disk-file d.dat --disk-size 2M
    python3 - <<'EOF'
import collections, os, selectors, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx
direct, _why = mx.disk_probe("d.dat")
pool = mx.DiskPool("d.dat", 2 << 20, direct, 4)
assert len(pool.ios) == 4 and all(t.is_alive() for t in pool._threads)
pkt = b"\0" * mx.HDR_SIZE + b"x" * 1000
jobs = ([("req", "flow", 4064)] * 30 + [("srv", pkt, 8160, "ctx")] * 20
        + [("srv", None, 100, "c2")] * 10 + [("wr", pkt)] * 15)
sel = selectors.DefaultSelector()
sel.register(pool.wake_fd, selectors.EVENT_READ)
done, sent = [], 0
while len(done) < len(jobs):
    while sent < len(jobs) and pool.room():
        pool.submit(jobs[sent])
        sent += 1
    assert pool.pending <= pool.limit, "the queue outgrew its bound"
    assert sel.select(5.0), "no completion woke the loop"
    done += pool.completions()
kinds = collections.Counter(d[0] for d in done)
assert kinds == {"req": 30, "srv": 30, "wr": 15}, kinds
for d in done:
    if d[0] == "req":
        assert d[1] == "flow" and type(d[2]) is bytes and len(d[2]) == 4064, d
    elif d[0] == "srv":
        assert type(d[1]) is bytes and len(d[1]) in (8160, 100), d
        assert d[2] in ("ctx", "c2")
assert pool.pending == 0
assert sum(io.rd_ops for io in pool.ios) == 60, "30 requests + 30 replies read"
assert sum(io.wr_ops for io in pool.ios) == 35, "20 requests + 15 replies written"
assert sum(1 for io in pool.ios if io.rd_ops + io.wr_ops) > 1, \
    "the work was not shared between the threads"
pool.close()
assert not any(t.is_alive() for t in pool._threads), "a thread outlived close"
EOF
    assert_status 0 $? "every job kind, counted and bounded" || return 1
}

test_disk_depth_is_validated() {
    two_hosts --pps 100 --rx-size 4096 --disk replies || return 1
    run_mx agent --matrix matrix.csv --host alpha --duration 1 --disk-depth 0 \
        --disk-file d.dat --disk-size 1M
    assert_status 2 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "at least 1" || return 1
    run_mx agent --matrix matrix.csv --host alpha --duration 1 --disk-depth 1000 \
        --disk-file d.dat --disk-size 1M
    assert_status 2 "$RUN_RC" "an absurd depth is refused" || return 1
    # Every I/O slot is a file handle: a too-low hard limit names them.
    python3 - <<'EOF'
import contextlib, io, os, sys
sys.path.insert(0, os.environ["REPO_ROOT"] + "/matrix_orchestrator")
import mx, resource
resource.getrlimit = lambda what: (100, 100)
err = io.StringIO()
try:
    with contextlib.redirect_stderr(err):
        mx.raise_fd_limit(50, 40)
    raise AssertionError("over the hard limit and not refused")
except SystemExit:
    pass
assert "plus 40 disk handles" in err.getvalue(), err.getvalue()
EOF
    assert_status 0 $? "the descriptor budget counts the disk handles" || return 1
}

test_a_deeper_queue_keeps_a_slow_disk_busy() {
    # The point of --disk-depth. With 2 ms added to every I/O, one I/O at a
    # time caps a worker near 500 a second, short of a 1000/s target; eight
    # in flight carry it. Only beta serves, on one worker, so its disk reads
    # are exactly its replies.
    local p; p=$(pick_port)
    write_servers "$p" alpha beta > /dev/null
    run_mx gen --servers servers.txt --pps 1000 --tx-size 128 --rx-size 4096 \
        --disk replies
    awk -F, 'BEGIN{OFS=","} /^beta=/{$2=""} {print}' matrix.csv > m.new \
        && mv m.new matrix.csv
    local depth rd want _ shallow deep
    for depth in 1 8; do
        rm -rf rep
        MX_TEST_DISK_DELAY=0.002 run_disk_agents 7 alpha beta --workers 1 \
            --disk-depth "$depth"
        assert_not_contains "$(cat rep/alpha.log rep/beta.log)" "Traceback" || return 1
        read -r rd want _ _ < <(disk_io rep/beta.csv replies)
        assert_io "$rd" "$want" "depth $depth: one read per reply" || return 1
        if [ "$depth" = 1 ]; then shallow=$rd; else deep=$rd; fi
    done
    assert_contains "$(cat rep/beta.log)" "8 I/Os in flight per worker" || return 1
    assert_contains "$(grep -m1 'disk=rd' rep/beta.log)" "depth=8" \
        "the status line shows the depth" || return 1
    assert_eq "8" "$(host_col rep/beta.csv disk_depth | cut -d. -f1)" \
        "the report records it" || return 1
    assert_between 1.5 100 "$(python3 -c "print($deep / max($shallow, 1))")" \
        "8 in flight move more I/O than 1 on a slow disk ($shallow -> $deep/s)" || return 1
}

test_disk_depth_runs_every_path_through_the_pool() {
    # Both directions on disk at depth 4: requests read before they go,
    # written before they are answered, replies read before they go and
    # written when they land -- all four through the I/O threads, and the
    # accounting is the same as inline.
    two_hosts --pps 200 --tx-size 1024 --rx-size 1024 --disk both || return 1
    run_disk_agents 7 alpha beta --workers 2 --disk-depth 4
    assert_not_contains "$(cat rep/alpha.log rep/beta.log)" "Traceback" || return 1
    assert_contains "$(cat rep/alpha.log)" "4 I/Os in flight per worker (--disk-depth), 8 across the host" || return 1
    local h rd rd_want wr wr_want req_rd rep_rd _
    for h in alpha beta; do
        read -r rd rd_want wr wr_want < <(disk_io "rep/$h.csv" both)
        assert_io "$rd" "$rd_want" "$h reads every payload it sends" || return 1
        assert_io "$wr" "$wr_want" "$h writes every payload it receives" || return 1
        read -r _ req_rd _ _ < <(disk_io "rep/$h.csv" requests)
        read -r _ rep_rd _ _ < <(disk_io "rep/$h.csv" replies)
        assert_between 0.3 0.7 "$(python3 -c "print($req_rd / $rd_want)")" \
            "$h's request reads are a real share" || return 1
        assert_between 0.3 0.7 "$(python3 -c "print($rep_rd / $rd_want)")" \
            "$h's reply reads are a real share" || return 1
    done
    # And the network half is answered as it always was.
    assert_between 20 300 "$(host_col rep/alpha.csv rep_pps)" "replies come back" || return 1
}

test_a_layered_rotation_with_a_deep_disk_queue() {
    # Layer switches close flows while their payloads may still be being
    # read: those reads finish, are counted, and send nothing on a flow that
    # has ended -- no crash, and the rotation still covers every peer.
    local p; p=$(pick_port)
    write_servers "$p" a b c d > /dev/null
    run_mx gen --servers servers.txt --pps 300 --peers 1 --dwell 4 --seed 7 \
        --tx-size 1024 --disk requests
    assert_status 0 "$RUN_RC" || return 1
    mkdir -p rep
    local h pids=()
    for h in a b c d; do
        python3 "$MX" agent --matrix matrix.csv --host "$h" \
            --report "rep/$h.csv" --interval 2 --duration 14 --workers 1 \
            --disk-depth 4 --disk-file "rep/$h.disk" --disk-size 4M \
            > "rep/$h.log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    assert_not_contains "$(cat rep/*.log)" "Traceback" || return 1
    assert_not_contains "$(cat rep/*.log)" "died" || return 1
    python3 - <<'EOF'
import csv
for h in "abcd":
    peers = set()
    for r in csv.DictReader(open("rep/%s.csv" % h, newline="")):
        if r["dir"] == "tx" and r.get("pps"):
            assert float(r["pps"]) >= 0, r
            peers.add(r["peer"])
    assert peers == set("abcd") - {h}, "%s covered %r" % (h, peers)
EOF
    assert_status 0 $? "the rotation completes with a deep disk queue" || return 1
}

# ---- the fleet -------------------------------------------------------------

setup_disk_fleet() {
    local port="$1"; shift
    install_fake_ssh
    printf 'alpha=127.0.0.1:%d\nbeta=127.0.0.2:%d\n' "$port" "$((port + 1))" \
        > servers.txt
    run_mx gen --servers servers.txt "$@"
    assert_status 0 "$RUN_RC" "gen" || return 1
}

host_dir() { echo "$FAKE_ROOT/$1$MX_REMOTE_DIR"; }

test_start_prepares_every_disk_before_any_agent_starts() {
    setup_disk_fleet "$(pick_port)" --pps 500 --rx-size 4096 --disk replies || return 1
    run_mx start --interval 2 --duration 30 --disk-size 2M --disk-depth 3
    assert_status 0 "$RUN_RC" "start" || return 1
    assert_contains "$RUN_OUT" "disk: preparing a 2.0 MiB data file" || return 1
    assert_contains "$RUN_OUT" "disk=replies" "start says the run is on disk" || return 1
    local d
    for d in 127.0.0.1 127.0.0.2; do
        assert_file_exists "$(host_dir $d)/disk.dat" "the data file is in --remote-dir" || return 1
        assert_eq "$((2 << 20))" "$(stat -c %s "$(host_dir $d)/disk.dat")" "its size" || return 1
    done
    # Every prepare ran before the first agent was launched.
    python3 - "$FAKE_ROOT/calls.log" <<'EOF'
import sys
lines = open(sys.argv[1]).read().splitlines()
prep = [i for i, l in enumerate(lines) if "--prepare-disk" in l]
launch = [i for i, l in enumerate(lines) if "nohup" in l]
assert len(prep) == 2 and launch, (prep, launch)
assert max(prep) < min(launch), "an agent started before every disk was ready"
assert all("--disk-size 2M" in lines[i] for i in prep + launch)
assert all("--disk-depth 3" in lines[i] for i in launch), "depth not passed on"
EOF
    assert_status 0 $? "prepare, then start" || return 1
    sleep 3
    # The agent found the file ready rather than writing it under load.
    assert_contains "$(cat "$(host_dir 127.0.0.1)/agent.log")" "reusing disk.dat" || return 1
    assert_contains "$(cat "$(host_dir 127.0.0.1)/agent.log")" "3 I/Os in flight per worker" \
        "the agent runs the depth it was started with" || return 1
    run_mx stop
    assert_contains "$RUN_OUT" "disk.dat data file" "stop says the file stays" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/disk.dat" "stop keeps it for the next run" || return 1
    run_mx clean --yes
    assert_status 0 "$RUN_RC" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/disk.dat" "clean removes it" || return 1
}

test_start_with_a_bad_disk_size_touches_nothing() {
    setup_disk_fleet "$(pick_port)" --pps 500 --disk replies || return 1
    run_mx start --disk-size lots
    assert_status 2 "$RUN_RC" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/mx.py" "refused before deploying" || return 1
    run_mx start --disk-depth 0
    assert_status 2 "$RUN_RC" "a bad depth is refused the same way" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/mx.py" "refused before deploying" || return 1
}

test_reload_brings_the_disk_in_on_a_running_fleet() {
    # Turning --disk on is a header edit, so every host restarts -- and each
    # has its data file written before its agent comes back.
    setup_disk_fleet "$(pick_port)" --pps 500 --rx-size 4096 || return 1
    run_mx start --interval 2 --duration 60 --disk-size 2M
    assert_status 0 "$RUN_RC" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/disk.dat" "no file until --disk" || return 1
    sed 's/port=\([0-9]*\)$/port=\1 disk=replies/' matrix.csv > m.new && mv m.new matrix.csv
    run_mx reload
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "matrix changed for 2 host" || return 1
    assert_file_exists "$(host_dir 127.0.0.1)/disk.dat" || return 1
    assert_eq "$((2 << 20))" "$(stat -c %s "$(host_dir 127.0.0.2)/disk.dat")" \
        "the size the fleet was started with" || return 1
    sleep 3
    assert_contains "$(cat "$(host_dir 127.0.0.1)/agent.log")" "disk=replies" || return 1
    run_mx stop
}

test_doctor_and_dry_run_with_disk() {
    setup_disk_fleet "$(pick_port)" --pps 500 --disk replies || return 1
    run_mx doctor
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "disk_free=" "doctor reports the space for the file" || return 1
    run_mx start --dry-run
    assert_status 0 "$RUN_RC" || return 1
    assert_contains "$RUN_OUT" "--prepare-disk" "dry-run shows the prepare step" || return 1
    assert_no_file "$(host_dir 127.0.0.1)/disk.dat" "--dry-run writes nothing" || return 1
}

run_test test_gen_records_the_disk_mode_in_the_header
run_test test_disk_mode_is_validated
run_test test_header_only_payloads_are_called_out
run_test test_check_sizes_every_hosts_disk
run_test test_fingerprint_is_unchanged_without_disk
run_test test_prepare_disk_writes_real_data_once_and_reuses_it
run_test test_a_small_filesystem_takes_a_file_that_fits
run_test test_disk_size_is_validated
run_test test_buffered_fallback_still_does_the_io
run_test test_memory_filesystem_is_called_out
run_test test_replies_are_read_by_the_responder_and_written_by_the_requester
run_test test_requests_are_read_by_the_requester_and_written_by_the_responder
run_test test_both_puts_every_payload_on_disk_at_both_ends
run_test test_header_only_packets_cost_no_io
run_test test_disk_columns_stay_blank_without_disk
run_test test_summarize_and_export_report_the_disks
run_test test_summarize_without_disk_says_nothing_about_disks
run_test test_a_disk_bound_run_names_the_disk_not_the_fabric
run_test test_disk_pool_runs_every_kind_of_job
run_test test_disk_depth_is_validated
run_test test_a_deeper_queue_keeps_a_slow_disk_busy
run_test test_disk_depth_runs_every_path_through_the_pool
run_test test_a_layered_rotation_with_a_deep_disk_queue
run_test test_start_prepares_every_disk_before_any_agent_starts
run_test test_start_with_a_bad_disk_size_touches_nothing
run_test test_reload_brings_the_disk_in_on_a_running_fleet
run_test test_doctor_and_dry_run_with_disk
report_tests
