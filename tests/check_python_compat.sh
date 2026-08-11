#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
#
# Guards the `requires-python = ">=3.6"` floor. The agent is copied to
# every server and run by whatever python3 is there, which on a long-lived
# fleet can be old -- so the floor is a real constraint, not paperwork.
#
# Run under any python to byte-compile (catches post-floor *syntax*); CI
# additionally runs vermin, which catches post-floor *stdlib APIs* that
# compile fine and then fail on the box that matters.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

fail=0

echo "byte-compiling with $(python3 -V 2>&1)"
for f in "$REPO_ROOT"/matrix_orchestrator/*.py; do
    if python3 -m py_compile "$f"; then
        echo "  OK   $(basename "$f")"
    else
        echo "  FAIL $(basename "$f")"
        fail=1
    fi
done
rm -rf "$REPO_ROOT/matrix_orchestrator/__pycache__"

# Cheap textual guard for the constructs most likely to sneak in, so a
# developer without vermin still gets told.
echo "scanning for constructs newer than the 3.6 floor"
if grep -nE "^\s*(from|import) (dataclasses|zoneinfo|graphlib)\b" \
        "$REPO_ROOT"/matrix_orchestrator/*.py; then
    echo "  FAIL: stdlib module newer than 3.6"
    fail=1
fi
if grep -nE ":=" "$REPO_ROOT"/matrix_orchestrator/*.py | grep -v "['\"].*:=" ; then
    echo "  FAIL: walrus operator needs 3.8"
    fail=1
fi
if grep -nE "subprocess\.run\(|capture_output=|text=True" \
        "$REPO_ROOT"/matrix_orchestrator/*.py; then
    echo "  FAIL: subprocess API newer than 3.6"
    fail=1
fi
[ "$fail" -eq 0 ] && echo "  OK"

exit "$fail"
