# Contributing to Yesterwind XYZ-Modem

Thank you for taking the time to contribute!

## Getting started

```bash
git clone https://github.com/ehwio/yesterwind-xyzmodem
cd yesterwind-xyzmodem
uv sync --extra dev   # installs the package + all test/lint dependencies
uv run pytest         # should pass with 100% coverage
```

## Branch naming

Branch off `main` and open a PR back to `main`.

| Prefix | Use for |
|---|---|
| `feature/<slug>` | New functionality |
| `fix/<slug>` | Bug fixes |
| `docs/<slug>` | Documentation-only changes |

Examples: `fix/zskip-handling`, `feature/xmodem-1k`, `docs/readme-demos`.

## Before every push

Run the same three checks that CI runs, in this order:

```bash
uv run ruff check src/ tests/          # lint
uv run ruff format --check src/ tests/ # formatting
uv run pytest                          # tests + coverage
```

All three must exit 0 before you push. Pushing with a failing check
wastes a CI run and delays review.

## Coverage rule

The test suite enforces **100% branch coverage** on all library code
(`src/yesterwind_xyzmodem/`, excluding `demos/`). If you add a new code
path, add a test that exercises it. The `# pragma: no cover` marker is
reserved for genuinely untestable lines (e.g. `TYPE_CHECKING` guards);
don't use it to hide missing tests.

## Python version matrix

CI tests against:

- 3.9
- 3.10
- 3.11
- 3.12

Keep compatibility with all four. Avoid features that require 3.10+
without a 3.9-compatible fallback.

## Commit style

- Imperative subject line, 72 characters or fewer
  (`Fix ZSKIP: treat receiver skip as success`, not `Fixed the ZSKIP bug`)
- Blank line, then a body that explains *why*, not *what*
- Reference the issue number in the body (`Closes #1`)

## Pull request checklist

- [ ] Branch is off `main` and up to date
- [ ] All three checks pass locally (lint, format, tests)
- [ ] Coverage stays at 100%
- [ ] PR description explains what changed and why
- [ ] Related issue linked (`Closes #N`)

## Releasing

Releases are handled by maintainers. The workflow is:

1. Bump `version` in `pyproject.toml` on `main`
2. Push a tag `vX.Y.Z` — CI creates the GitHub Release and publishes to PyPI automatically
