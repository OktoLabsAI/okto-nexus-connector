# Overlapping Pi discovery adoption

The Core .54 automatic Windows Pi layout could overlap with an executor's explicit
Pi release configuration, yielding two identical candidates. Two directed Core
tests failed before the correction. Core .55 consolidates identical observations
by physical installation reference and refuses conflicting evidence with
PROFILE_DRIFT. Distinct installations with identical bytes remain distinct.

Connector now pins and vendors Core .55 (commit `beed295`). No Connector application
code changed. Installed discovery/configuration/observation tests passed 31 cases
on Windows Python 3.13.1 and 31 on WSL Linux Python 3.12.13. Evidence:
`evidence/discovery-055-windows.xml` and `evidence/discovery-055-linux.xml`.
Core's own installed evidence is in `plans/DISCOVERY_055.md` in its repository.

Core wheel SHA-256:
`b1a389d247a470571c485e27adfa7277b11b4ae39382a52ebefc0ef0cc76453f`.
Connector wheel SHA-256:
`1c799154b2998035617bcfe2301cd9f4be7f3982dd7e797d7fd33b8dff6b252a`.
Packaged Python files were compared with source before Nexus CI adoption.
The real same-machine Pi runtime campaign is tracked in Nexus; these directed
passes do not qualify it or the complete final-artifact/OS/Python matrix.
