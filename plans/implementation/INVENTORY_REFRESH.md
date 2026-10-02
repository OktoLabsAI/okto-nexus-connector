# Passive inventory refresh HTTP extension

The daemon now consumes Server refresh requests after reconciliation when public
protocol metadata explicitly advertises `inventory_refresh_supported: true`.
Missing capability means the existing periodic publication behavior; a malformed
capability is refused. No Core snapshot or WSS frame format changes.

The current control ticket claims a delivery using
`POST /v1/runtime/executors/{id}/inventory:claim-refresh`. The response is checked
against the Server, executor and connection. The daemon claims before passive
discovery, checks its local configuration again, reserves the durable next
sequence, and publishes the correlation in `X-Nexus-Inventory-Refresh`.
Initial reconciliation claims pending work; subsequent claims occur every five
seconds. Periodic publication continues independently. A lost reply follows the
existing reconnect/reconciliation path and never reuses a publication sequence.
No refresh launches a version probe or grants execution authority.

## Verification on 2026-10-02

Source: 48 directed checks passed. Installed package: 79 checks passed on
Windows/Python 3.13.1 (34.98 seconds) and WSL Linux/Python 3.12.13 (29.75 seconds).
The installed runs use `-I` and `pythonpath=.` for test helpers, without adding
`src`. They cover strict HTTP capability/scope checks, publication correlation,
claim-before-discovery ordering, lost reply/reconnect, existing daemon control,
Core inventory projection, durable publications and lease renewal.

The companion Nexus campaign uses actual HTTP/WSS/IPC and the daemon lifecycle
from the installed packages: an offline request is completed at startup; another
is completed after reconnect while online. It creates no runtime operation and
uses empty passive discovery. This is same-host transport evidence, not two-host
acceptance or a real-provider journey. Nexus verifies all three installed package
trees against their wheel bytes before and after its campaigns.

Wheel SHA-256:
`46991823a500fef326917edeb6063fb6e570126731e35d8c9c349104a1740b8f`.
Sdist SHA-256:
`dcc79f39d709a6510babe44564a7461d15a531ff581eb54a9bab72be67350be2`.
All wheel Python module bytes were compared with source. The artifacts are a
development build from the reviewed worktree, not a final commit freeze.

[Artifact report](evidence/inventory-refresh-artifacts.json),
[source tests](evidence/inventory-refresh-source-final.xml),
[Windows installed tests](evidence/inventory-refresh-installed-windows.xml),
[Linux installed tests](evidence/inventory-refresh-installed-linux.xml).

Reproduce in an environment containing the built wheel and pinned Core:

```powershell
python -I -m pytest -q -o pythonpath=. tests/unit/test_inventory_refresh.py tests/unit/test_r4_daemon_control.py tests/unit/test_executor_inventory_r4.py tests/unit/test_r4_publications.py tests/unit/test_r4_lease_renewal.py
```

## Acceptance still open

UI, independent-host faults/provider matrix and final release qualification remain
pending. The preceding hosted run at `d2e240e` is not a green full-suite proof:
[Windows/Python 3.13 job](https://github.com/OktoLabsAI/okto-nexus-connector/actions/runs/36988580026/job/110779196138)
recorded 699 passes, two skips and two timeouts waiting for renewal entry in the
`[True]` variants of `test_automatic_renewal_keeps_the_same_runtime_past_its_initial_deadline`
and `test_renewal_failure_does_not_extend_deadline_or_repeat_native_open`.
Both pass in this directed installed campaign; their hosted cause remains
unconfirmed. No assertion, lease duration or observation timeout was relaxed.
