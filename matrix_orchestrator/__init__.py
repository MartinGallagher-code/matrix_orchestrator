# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
"""matrix_orchestrator: a request/response traffic matrix across a fleet.

The whole tool is :mod:`matrix_orchestrator.mx` -- a single stdlib-only
file that is both the controller you run and the agent it copies to every
host. See the README, or run ``mx hints``.
"""

from .mx import VERSION as __version__   # noqa: F401
from .mx import main                     # noqa: F401

__all__ = ["main", "__version__"]
