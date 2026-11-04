<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# Notes for Claude

## Every task is a ticket

Everything done here goes through a ticket in `MartinGallagher-code/shared_tools`,
which keeps every project's tickets as files and moves each one Backlog → In Progress
→ Review → Done (Adversarial Test only when the owner asks). Before any change:

1. Attach `MartinGallagher-code/shared_tools` with `add_repo` and push access, and
   clone it. If a clone is already here, pull it instead
   (`git -C <shared_tools clone> pull --ff-only`): a stale clone has a stale
   `CLAUDE.md` and a stale `ticket.py`.
2. Read the "Every task is a ticket" section of its `CLAUDE.md`. It sets out what each
   column requires, and none of it is optional.
3. Open the ticket with the shared_tools clone's script, not a path in this
   repository:

   ~~~bash
   <shared_tools clone>/scripts/ticket.py new matrix_orchestrator "Imperative title" --request "the request, in the user's own words"
   ~~~

The board: https://claude.ai/artifact/HrB3RpVpqGGQS7UZgZsMJm

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
