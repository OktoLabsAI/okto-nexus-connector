# M4d4e4: Connector half of the audit handshake

Connector state schema 15 retains the Core store identity and a matching
audit nonce, revision and digest in its independently locked atomic JSON
snapshot. The daemon starts the audit worker after IPC and transports are
ready, then joins it on shutdown. A failed state commit leaves the Core token
unpaired and is retried; it never blocks daemon readiness. A later shared
journal registration advances the Core head and triggers a new token on the
next audit pass.

The adapter checks the expected shared journal path and sidecars and converts
an older Core registry head in bounded pages before staging. A full snapshot
`StateStore.save` cannot erase a stored audit token. The token is not consumed
for admission; this checkpoint does not remove any startup safety check.

Source validation: 39 combined state, binding, journal, capacity, daemon and
recovery cases passed. One earlier combined run stalled without a test result;
the same 39 cases passed on a clean rerun, and the individual suites passed.
The same 39 cases passed against the installed wheel with Python isolated
imports. One quiet installed run stalled during the daemon lifecycle case;
the verbose rerun passed all 39 in 44 seconds. The cause of the stall is not
established, so this checkpoint does not claim a load or no-hang guarantee.
The disposable wheel is `okto_nexus_connector-0.0.10-py3-none-any.whl`,
SHA-256 `fae32af21020101875a96f2430399f4ba8d45b111d61a3d5f0fda3e96d305f82`.

M4d4e5 adds a pre-readiness rollback check using the shared Core rule. A
same-identity Core ledger restored behind the independent Connector state is
refused before IPC readiness. Ten focused source and ten installed binding
cases passed. The final disposable Connector wheel SHA-256 is
`a2288bf73f11caf008cfb1bf52427da326d714d0e35d2321ed3b19c9528452b6`.

M4d4e6 converts Core's sparse digest index in bounded retained work before
renewing a Connector token. An ahead Core head must extend the digest stored
in Connector state. Eleven source and eleven installed binding cases passed,
including deliberate prefix divergence. The final disposable Connector wheel
SHA-256 is `de7e9f695bc8dc7a672fb33ace5eb79789f575c5f6dd2fb03ee5743ebe92e1ca`.
