# Yesterwind XYZ-Modem

This repository uses the simplified GitFlow development workflow.

- Features and Bugfixes happen in `feature/*` and `bugfix/*` branches 
- Releases are kept in branches, created from the `main` branch as
  `release-${version}`
- GitHub workflows detect pushes of tagged versions such as `v0.1.0`

## TDD

This repo requires 100% code coverage in testing.  The testing matrix covers
the following python versions:

- 3.9
- 3.10
- 3.11
- 3.12

