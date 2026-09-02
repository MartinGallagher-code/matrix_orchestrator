<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# Notes for Claude

## Bundling this repo

When packing this repository into a text bundle for handover, use the canonical
scripts in
[`shared_tools`](https://github.com/MartinGallagher-code/shared_tools)
(`scripts/merge.sh`, `scripts/split.sh`, `scripts/b64bundle.sh`) and read that
repo's `CLAUDE.md` first.

**Never include git internals in the bundle.** `.git/` and everything under it
must never be bundled — it is the whole history when the bundle needs one
commit's tree, it inflates the packet count for nothing, and `.git/config` can
carry credentials or private remote URLs. Always export with
`git archive main | tar -x -C /tmp/tree` (which produces a clean tree with no
`.git/`); never bundle a working tree or a plain `cp -r` of a live checkout. If
you ever bundle a directory that was not produced by `git archive`, delete
`.git/` first and confirm `find /tmp/tree -name .git` returns nothing before
running `merge.sh`.

`bundle.txt` and `bundle.part*of*.txt` are git-ignored here (see `.gitignore`):
bundles are handover artifacts, not committed files.
