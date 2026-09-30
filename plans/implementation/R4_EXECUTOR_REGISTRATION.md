# Durable R4 executor registration — partial M02/M06

State schema 5 adds `execution_executors`: the selected Server, Connector ID,
registration agent, immutable client intent/body, canonical executor ID and
local registration status. It stores no bootstrap ticket. Existing schema 4
bindings/realizations remain intact and do not imply an executor registration.

`ExecutorRegistrationService.register` persists the intent before network I/O.
It uses exactly the selected imported identity and approved Server profile,
checks `/me`, and verifies the bootstrap response's Server/Connector/agent and
canonical credential/authorization revisions. The returned executor is committed
with CAS against the original request, identity and profile. Concurrent changes,
ambiguous identities and conflicting results cannot overwrite the registration.

After a lost response, the next call replays the same persisted intent. A daemon
can call `bootstrap(server_id=...)` after restarting to obtain a fresh derivative
through that registration. No identity is selected implicitly from a list. The
ticket remains process-local and is omitted from the bootstrap object's repr.
Its local deadline starts before the HTTP request; slow response/persistence
cannot create a new lifetime. Registration creates no binding, lane or lease.

## CLI

After importing an identity, use:

```text
okto-nexus-connector --json --non-interactive executor register --identity my-agent --label "Workstation"
okto-nexus-connector --json executor list
okto-nexus-connector --json executor show SERVER_ID
```

`--client-intent-id` optionally supplies an explicit recovery ID; otherwise the
first attempt generates and persists one. Repeating the command retains its
body. A changed label, actor or explicit intent is a conflict, not an implicit
replacement. Public output contains registration metadata and no ticket/key.

`identity add` now remembers the explicitly selected, authenticated Server using
the same profile path as `connect`. Previously it imported the credential but
left no profile for executor registration or later credential replacement.

## Evidence and remaining work

Directed tests cover lost response/replay, state reload, secret exclusion,
cross-scope responses, changed canonical revisions, concurrent local changes,
conflicting executor IDs, expired replies and schema migration. The Nexus
integration invokes the real CLI entry point against a real loopback HTTP
Server for identity import, register/replay/list/show and fresh bootstrap.
It observes one canonical executor and one registration intent, with no socket
owner. Results and hashes are in `evidence/r4-registration-report.json`.

The daemon's automatic startup must still call this service, negotiate R4,
publish inventory, attach approved bindings and compose runtime environment.
Tickets are not yet renewed by a live R4 daemon loop; lane/lease renewal,
reconnect, reconciliation, explicit revision refresh and final product gates
remain pending. This change makes those steps possible using persisted identity
rather than test-supplied bootstrap data; it does not claim that they are done.

Older binaries reject schema 5. Full upgrade/backup/rollback qualification stays
in M12. The Core wheel, runtime wire and executable/readiness flags are unchanged.
