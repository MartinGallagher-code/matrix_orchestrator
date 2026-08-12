# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 Martin J. Gallagher
"""Sphinx configuration for matrix_orchestrator.

Two principles, both anti-drift:

- The prose pages *include* the repository's own markdown (README,
  CHANGELOG, PUBLISHING) rather than duplicating it, so there is one
  copy of every sentence.
- The CLI reference is generated at build time from the real argparse
  parsers -- the same trick `mx help` uses -- so a flag cannot exist
  without being documented here, nor linger here after it is removed.
"""

import os
import sys

DOCS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DOCS_DIR)
sys.path.insert(0, REPO_ROOT)

from matrix_orchestrator import mx  # noqa: E402

project = "matrix-orchestrator"
author = "Martin J. Gallagher"
copyright = "2026, Martin J. Gallagher"  # noqa: A001 - sphinx's name for it
version = mx.VERSION
release = mx.VERSION

extensions = ["myst_parser"]
myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 3

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
master_doc = "index"
exclude_patterns = ["_build"]

html_theme = "sphinx_rtd_theme"
html_title = "matrix-orchestrator %s" % mx.VERSION
html_theme_options = {"collapse_navigation": False}

# The included README carries repo-relative links (LICENSE, tests/...)
# that have no docs-site counterpart; MyST flags them as xref warnings.
# They are correct where the file lives, so quieten just that class.
suppress_warnings = ["myst.xref_missing"]


def _generate_cli_reference():
    """Write cli.md from the live parsers: one section per command, its
    real --help text in a literal block. Runs on every build."""
    ap = mx.build_parser()
    sub = next(a for a in ap._actions
               if isinstance(a, mx.argparse._SubParsersAction))
    out = [
        "# CLI reference",
        "",
        "Generated from the argparse parsers at build time -- this page",
        "cannot drift from what the tool accepts. The same reference is",
        "available offline as `mx help`.",
        "",
    ]
    for name, parser in sub.choices.items():
        if name == "help":
            continue
        out.append("## mx %s" % name)
        out.append("")
        out.append("```text")
        out.append(parser.format_help().rstrip())
        out.append("```")
        out.append("")
    out += [
        "## Environment variables",
        "",
        "Each is the default for the matching flag:",
        "`MX_MATRIX`, `MX_SERVERS`, `MX_REPORTS`, `MX_REMOTE_DIR`,",
        "`MX_USER` (falls back to `SSH_USER`), `MX_JOBS`, `MX_PYTHON`,",
        "and `IPERF_BIND` (the same variable iperf-orchestrator uses).",
        "",
    ]
    path = os.path.join(DOCS_DIR, "cli.md")
    with open(path, "w") as f:
        f.write("\n".join(out))


_generate_cli_reference()
