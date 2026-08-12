<!--
SPDX-License-Identifier: GPL-3.0-or-later
SPDX-FileCopyrightText: 2026 Martin J. Gallagher
-->

# Publishing to PyPI

This project ships as the **`matrix-orchestrator`** distribution on PyPI
(the name was verified available). Installing it puts two commands on
PATH: **`mx`** (the one you actually type) and `matrix-orchestrator` (an
alias). The console script `mx` does not conflict with the old eGenix
`mx` PyPI distribution: that is a library whose *import* package is
`mx`, while ours is `matrix_orchestrator` — different namespace, no
collision either way.

## Prerequisites

- Python 3.9+ with `build` and `twine`:
  ```bash
  python -m pip install --upgrade build twine
  ```

  This is the **build host** requirement, deliberately higher than the
  package's own `requires-python` (3.6+): current `setuptools` (>=77,
  needed for the PEP 639 license metadata), `build`, and `twine` all
  require 3.9+. The wheel they produce is `py3`-generic and installs
  fine on 3.6.

## One-time setup: trusted publishing

The `publish.yml` workflow uses PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), so no API token lives in the repo secrets. On PyPI, under the
project (or as a *pending* publisher before the first upload), add a
trusted publisher with:

- Owner: `MartinGallagher-code`
- Repository: `matrix_orchestrator`
- Workflow: `publish.yml`
- Environment: `pypi`

Then create the matching `pypi` environment in the GitHub repository
settings (Settings → Environments), optionally with required reviewers
as a release gate.

## Cut a release

1. **Bump the version** in `matrix_orchestrator/mx.py` (`VERSION`) *and*
   `pyproject.toml` (`[project].version`), and add a matching entry to
   `CHANGELOG.md`. Follow [SemVer](https://semver.org/). CI enforces
   that all three agree (`test_version_is_bumped_and_consistent_everywhere`),
   so a mismatch cannot reach a release unnoticed.

2. **Build the distributions** (sdist + wheel) into `dist/`:
   ```bash
   rm -rf dist
   python -m build
   ```

3. **Validate the metadata** — this must pass before uploading:
   ```bash
   python -m twine check dist/*
   ```

4. **Smoke-test the built wheel** in a throwaway virtualenv:
   ```bash
   python -m venv /tmp/mx-check
   /tmp/mx-check/bin/pip install dist/*.whl
   /tmp/mx-check/bin/mx --version
   /tmp/mx-check/bin/mx help >/dev/null
   ```

5. **Publish** — either path:
   - **Via GitHub (preferred):** push a tag and publish a Release; the
     `publish.yml` workflow builds, checks, smoke-tests and uploads via
     trusted publishing:
     ```bash
     git tag v$(python3 -c "import matrix_orchestrator; print(matrix_orchestrator.__version__)")
     git push origin --tags
     # then: GitHub → Releases → Draft a new release → publish
     ```
   - **By hand:**
     ```bash
     python -m twine upload dist/*
     ```
     (Optionally rehearse on TestPyPI first:
     `python -m twine upload --repository testpypi dist/*`.)

6. **Verify**: `pip install matrix-orchestrator` in a clean venv, run
   `mx --version`, and check the project page renders the README:
   <https://pypi.org/project/matrix-orchestrator/>.

## What the sdist and wheel contain

- **Wheel**: the `matrix_orchestrator` package (`mx.py` and the two
  wrappers) plus entry points. That is the whole runtime — stdlib only.
- **sdist**: the above plus `LICENSE`, `LICENSES/`, `README.md`,
  `CHANGELOG.md`, `REUSE.toml`, and the full test suite (`tests/`), so
  distro packagers can run the tests against exactly what they
  unpacked (`tests/run_tests.sh`; needs bash and python3, no network
  beyond loopback).
