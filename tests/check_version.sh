#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 Martin J. Gallagher

# check_version.sh -- every change moves the version.
#
# Usage: bash tests/check_version.sh [BASE]
#
# With no argument: every place this repository writes its version says the
# same thing, and the changelog's first section is that version, numbered and
# dated -- "## X.Y.Z — YYYY-MM-DD", or Keep a Changelog's "## [X.Y.Z] -
# YYYY-MM-DD", either may run on after the date -- with no "Unreleased"
# heading anywhere in it.
#
# With BASE, a commit: all of that, and the version is strictly greater than
# the one BASE has in the first place listed below. CI passes the base
# branch's tip on a pull request, and what main was before a push to it.
# Release numbers compare part by part (1.10.0 > 1.9.0), and a pre-release
# sorts before its release (1.0.0.dev3 < 1.0.0rc1 < 1.0.0). A version that
# cannot be read, on either side, fails; it never passes.
#
# Exit status: 0 when every check holds, 1 when one does not, 2 on misuse.
#
# Copied from shared_tools' templates/check_version.sh (BN-002). The places
# and the changelog just below are this repository's own; everything under
# the line that says so is the same in every copy, so a fix there is copied
# over verbatim.

# This repository's places: a file, a space, then a sed BRE matched from the
# start of a line, whose first group is the version (no "|" in it, and no
# "$" at its end: the rest of the line is ignored). The first one listed is
# the one compared with BASE.
places() { cat <<'PLACES'
matrix_orchestrator/mx.py ^VERSION = "\([^"]*\)"
pyproject.toml ^version = "\([^"]*\)"
PLACES
}
CHANGELOG=CHANGELOG.md

# ---- below this line, every copy is the same as shared_tools' template ----

set -eu

case "${1-}" in
    -h|--help) sed -n '5,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "check_version.sh: unknown option: $1" >&2; exit 2 ;;
esac
[ $# -le 1 ] || { echo "usage: check_version.sh [BASE]" >&2; exit 2; }
base="${1-}"
cd "$(dirname "$0")/.."

# read_place FILE BRE [REV]: the version FILE says, at REV if given, or nothing.
read_place() {
    if [ $# -ge 3 ]; then git show "$3:$1" 2>/dev/null || true
    else cat "$1" 2>/dev/null || true
    fi | sed -n "s|$2.*|\1|p" | head -n 1
}

# The version's parts: release, then the pre-release kind's rank (dev < a <
# b < rc < none) and its number. Nothing when it is not a version.
VRE='^v?([0-9]+(\.[0-9]+)*)([.-]?(dev|a|b|rc)([0-9]*))?$'
parts() {
    [[ $1 =~ $VRE ]] || return 1
    local rank=4
    case "${BASH_REMATCH[4]}" in dev) rank=0 ;; a) rank=1 ;; b) rank=2 ;; rc) rank=3 ;; esac
    printf '%s %s %s\n' "${BASH_REMATCH[1]}" "$rank" "${BASH_REMATCH[5]:-0}"
}

# newer A B: true when A is strictly greater than B.
newer() {
    local a b rel1 rank1 num1 rel2 rank2 num2 ra rb i x y
    a=$(parts "$1") && b=$(parts "$2") || return 1
    read -r rel1 rank1 num1 <<< "$a"
    read -r rel2 rank2 num2 <<< "$b"
    IFS=. read -r -a ra <<< "$rel1"
    IFS=. read -r -a rb <<< "$rel2"
    for ((i = 0; i < ${#ra[@]} || i < ${#rb[@]}; i++)); do
        x=$((10#${ra[i]:-0})); y=$((10#${rb[i]:-0}))
        [ "$x" -eq "$y" ] || { [ "$x" -gt "$y" ]; return; }
    done
    [ "$rank1" -eq "$rank2" ] || { [ "$rank1" -gt "$rank2" ]; return; }
    [ $((10#$num1)) -gt $((10#$num2)) ]
}

fail=0
bad() { printf 'FAIL  %s\n' "$*"; fail=1; }

# 1. Every place says the same version.
version=""
first_file=""
first_re=""
n=0
listing=""
while IFS= read -r line; do
    [ -n "$line" ] || continue
    file=${line%% *}
    re=${line#* }
    v=$(read_place "$file" "$re")
    n=$((n + 1))
    listing=$(printf '%s\n        %-40s %s' "$listing" "$file" "${v:-(none)}")
    if [ -z "$first_file" ]; then first_file=$file; first_re=$re; fi
    if [ -z "$v" ]; then
        bad "$file: no version found (looked for: $re)"
    elif [ -z "$version" ]; then
        version=$v
    fi
done < <(places)
[ "$n" -gt 0 ] || bad "no places are listed in places()"

if [ "$fail" -eq 0 ]; then
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        [ "$(read_place "${line%% *}" "${line#* }")" = "$version" ] && continue
        bad "the places disagree:$listing"
        break
    done < <(places)
fi
if [ -n "$version" ] && ! parts "$version" >/dev/null; then
    bad "$first_file: \"$version\" is not a version this check can compare"
fi

# 2. The changelog opens with that version, numbered and dated, and holds no
#    "Unreleased" heading.
if [ -n "$CHANGELOG" ]; then
    if [ ! -f "$CHANGELOG" ]; then
        bad "$CHANGELOG: not found"
    else
        top=$(grep -m 1 '^## ' "$CHANGELOG" || true)
        logv=$(printf '%s\n' "$top" | sed -n -E \
            's/^## \[?([^] ]+)\]? (—|–|-) [0-9]{4}-[0-9]{2}-[0-9]{2}( .*)?$/\1/p')
        if [ -z "$logv" ]; then
            bad "$CHANGELOG: the first section is not \"## X.Y.Z — YYYY-MM-DD\": ${top:-(no ## heading)}"
        elif [ -n "$version" ] && [ "$logv" != "$version" ]; then
            bad "$CHANGELOG: the first section is $logv, and the version is $version"
        fi
        unrel=$(grep -n -i -E '^## \[?unreleased\]?( |$)|^## \[?[^] ]+\]? (—|–|-) unreleased' "$CHANGELOG" || true)
        [ -z "$unrel" ] || bad "$CHANGELOG: an \"Unreleased\" heading -- give it a number and a date: $unrel"
    fi
fi

# 3. The version moved past BASE's.
if [ -n "$base" ]; then
    if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
        bad "BASE $base: not a commit here"
    else
        was=$(read_place "$first_file" "$first_re" "$base")
        short=$(git rev-parse --short "$base")
        if [ -z "$was" ]; then
            bad "$first_file: no version found at $base ($short)"
        elif ! parts "$was" >/dev/null; then
            bad "$first_file at $base ($short): \"$was\" is not a version this check can compare"
        elif [ "$fail" -eq 0 ] && ! newer "$version" "$was"; then
            bad "the version is $version, and $base ($short) has $was -- every change moves the version"
        fi
    fi
fi

[ "$fail" -eq 0 ] || exit 1
msg="version $version in all $n place(s)"
[ -z "$CHANGELOG" ] || msg="$msg and at the top of $CHANGELOG"
[ -z "$base" ] || msg="$msg; $base ($short) has $was"
echo "$msg"
