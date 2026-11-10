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

1. **Nothing to bump: publish the version `main` already has.** Every
   pull request moves the version itself, however small the change
   (shared_tools BN-002): `matrix_orchestrator/mx.py` (`VERSION`),
   `pyproject.toml` (`[project].version`) and a matching
   `## [X.Y.Z] - YYYY-MM-DD` entry in `CHANGELOG.md`, following
   [SemVer](https://semver.org/). CI enforces that all three agree
   (`tests/check_version.sh`, and
   `test_version_is_bumped_and_consistent_everywhere`) and that the
   version moved past the base branch's (the **version** job), so a
   mismatch or a repeated number cannot reach a release unnoticed.
   Releasing is a separate step, and a person's call; a version that is
   never published is fine.

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
   rm -rf /tmp/mx-check
   python -m venv /tmp/mx-check
   /tmp/mx-check/bin/pip install dist/*.whl
   /tmp/mx-check/bin/mx --version
   /tmp/mx-check/bin/mx help >/dev/null
   ```

5. **Publish** — either path. A version number can never be reused on
   PyPI, so tag the commit that is on `main` after the version's pull
   request merged, not a branch:
   - **Via GitHub (preferred):** tag that commit, push **that one tag**,
     and publish a Release for it; the `publish.yml` workflow builds,
     checks, smoke-tests and uploads via trusted publishing:
     ```bash
     git fetch origin main && git checkout --detach origin/main
     v=$(python3 -c "import matrix_orchestrator; print(matrix_orchestrator.__version__)")
     git tag -a "v$v" -m "matrix-orchestrator $v"
     git push origin "v$v"
     # then: GitHub → Releases → Draft a new release → tag v$v → publish
     ```
     Push the tag by name: `git push origin --tags` sends every tag in
     the clone, including any made locally by mistake. The workflow
     refuses a Release whose tag is not `v` plus the version in
     `mx.py`, so a mistyped tag fails before anything is uploaded.
     The Release notes are the `CHANGELOG.md` entries for every version
     since the last one on PyPI, not only the newest, since those are
     the changes a `pip install` user has not seen.
   - **By hand:**
     ```bash
     python -m twine upload dist/*
     ```
     (Optionally rehearse on TestPyPI first:
     `python -m twine upload --repository testpypi dist/*`.)
     Tag the commit as above all the same, so the version on PyPI can be
     traced to the commit it was built from.

6. **Verify**: `pip install matrix-orchestrator==<version>` in a clean
   venv, run `mx --version`, and check the project page renders the
   README: <https://pypi.org/project/matrix-orchestrator/>.

## Is every version published?

**Not so far.** PyPI holds 1.2.0, 1.4.2 and 1.9.0 only, each uploaded
without a git tag, while `main` went through every version in
`CHANGELOG.md`, and no version had a git tag before 1.11.2. Whether every
version bump is published from now on, or only some, is the owner's
decision, and this section records it once it is made.

Either way the gap is visible: on every push to `main`, CI's wheel job
warns when the version in `mx.py` has no `v<version>` tag on origin.
To see where things stand by hand:

```bash
curl -s https://pypi.org/pypi/matrix-orchestrator/json \
    | python3 -c "import json,sys; print(sorted(json.load(sys.stdin)['releases']))"
git ls-remote --tags origin
```

## What the sdist and wheel contain

- **Wheel**: the `matrix_orchestrator` package (`mx.py` and the two
  wrappers) plus entry points. That is the whole runtime — stdlib only.
- **sdist**: the above plus `LICENSE`, `LICENSES/`, `README.md`,
  `CHANGELOG.md`, `REUSE.toml`, and the full test suite (`tests/`), so
  distro packagers can run the tests against exactly what they
  unpacked (`tests/run_tests.sh`; needs bash and python3, no network
  beyond loopback).
