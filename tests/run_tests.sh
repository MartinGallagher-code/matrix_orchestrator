#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
#
# Top-level test runner. Discovers every `test_*.sh` next to it and runs
# each as its own bash subprocess, so a fatal error in one file cannot
# take down the rest of the suite.
#
# Usage:
#   tests/run_tests.sh                  # everything
#   tests/run_tests.sh test_matrix.sh   # one file
#   tests/run_tests.sh -v               # show each file's full output

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MX="$REPO_ROOT/matrix_orchestrator/mx.py"
export REPO_ROOT MX

if [ ! -f "$MX" ]; then
    echo "ERROR: $MX not found" >&2
    exit 2
fi

VERBOSE=0
files=()
for arg in "$@"; do
    case "$arg" in
        -v|--verbose) VERBOSE=1 ;;
        *) files+=("$arg") ;;
    esac
done

if [ ${#files[@]} -eq 0 ]; then
    while IFS= read -r f; do
        files+=("$f")
    done < <(find "$SCRIPT_DIR" -maxdepth 1 -name 'test_*.sh' -type f | sort)
fi

if [ ${#files[@]} -eq 0 ]; then
    echo "No test_*.sh files found in $SCRIPT_DIR" >&2
    exit 2
fi

echo "mx test suite"
echo "  tool:       $MX"
echo "  python:     $(python3 -V 2>&1)"
echo "  test files: ${#files[@]}"
echo

file_pass=0
file_fail=0
failed_files=()

for f in "${files[@]}"; do
    [ -f "$f" ] || f="$SCRIPT_DIR/$f"
    name="$(basename "$f")"
    echo "==> $name"
    if [ "$VERBOSE" -eq 1 ]; then
        bash "$f"
        rc=$?
    else
        out=$(bash "$f" 2>&1)
        rc=$?
        printf '%s\n' "$out" | sed -n '/^    \(PASS\|FAIL\)/p; /^  ran:/,/^  failed:/p'
        if [ "$rc" -ne 0 ]; then
            echo "--- full output ---"
            printf '%s\n' "$out"
            echo "--- end ---"
        fi
    fi
    if [ "$rc" -eq 0 ]; then
        file_pass=$((file_pass + 1))
    else
        file_fail=$((file_fail + 1))
        failed_files+=("$name")
    fi
    echo
done

echo "========================================"
echo "Test files run:    ${#files[@]}"
echo "Test files passed: $file_pass"
echo "Test files failed: $file_fail"
if [ "$file_fail" -ne 0 ]; then
    printf '  %s\n' "${failed_files[@]}"
    exit 1
fi
exit 0
