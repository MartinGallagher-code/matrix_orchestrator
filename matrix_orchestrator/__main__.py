# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Martin J. Gallagher
"""``python -m matrix_orchestrator``."""

import sys

from .mx import main

if __name__ == "__main__":
    sys.exit(main())
