# Connector architecture

The Connector runs on the execution host and uses the same pinned Core artifact
as Nexus. Productive remote execution uses authenticated R4 HTTP management and
outbound NXL R4 WSS. Core owns native process protocols, qualification, containment,
leases and its durable journal. The Connector contains no MCP server or proxy:
Codex/Claude MCP clients call Nexus directly over HTTP; Pi uses the native bridge.

```text
CLI -> protected local state / authenticated daemon IPC
                    |
                    v
daemon -> R4 HTTP management + outbound WSS -> Nexus canonical authority
   |
   +-> Core -> owned provider processes
                 |
                 +-> direct HTTP tools/native domain bridge -> Nexus
```

## Authority and ownership

One daemon owns a state directory under the OS account's permissions and a lock
with process birth identity. Server/executor/binding/session namespaces remain
separate. The CLI submits durable intents and observes results; it does not own
the productive runtime or the control socket. Closing a CLI observer does not
cancel an admitted operation.

The canonical agent key authenticates identity and management requests. Short-lived
executor tickets authenticate inventory/control setup; the WSS handshake does not
send the canonical key as its transport credential. A binding records reviewed
configuration and consent. Execution still requires current Server authorization,
an applied Core lease and the exact qualified installation. Session MCP/native
capabilities have separate scopes and cannot replace any of these authorities.

The Server persists resolution and admission before dispatch. The daemon keeps
one control reader, uses bounded lanes and asks Core for the current operation
context. It installs a correlated lease grant before sending `lease.applied`;
receiving a grant alone cannot extend local authority. Lease renewal, revocation,
connection generations and scope changes are fenced by Core and the connection
owner. Reconnection requires reconciliation before productive dispatch.

## Installation and workspace selection

Discovery is passive and constrained by locally approved roots. The Server stores
path-free catalog/availability evidence and does not inspect remote files or
requalify a remote installation using the Server's operating system.

`executor probe` is an explicit exception to passive discovery: it requests only
Core's contained version observation for the selected candidate and inventory
revision. Schema 13 retains observations bound to complete passive candidate
evidence, Core version and platform. Drift prevents reuse. A recorded version can
remain unqualified and grants no runtime authority. The daemon publishes the new
inventory; probe completion is not publication acknowledgment.

Local launch consent binds the approved workspace, provider home and protected
secret references. Realization publication exposes opaque references and digests.
Binding prepare/apply uses the exact reviewed diff and required operator proof.
Replacement preserves canonical identity only after the Server validates the
current revision and idle/reconciled session state.

## Native work, publication and recovery

Approved launch configuration composes session tool capabilities. Core executes
the provider; the Connector publishes correlated receipts and events, retaining
publication obligations through retries and shutdown. Unknown outcomes preserve
their original intent/operation identity. A lost response does not authorize a
new operation or imply that the effect failed.

Native domain calls preserve action/claim identities and use scoped capabilities.
The bridge validates current authority around asynchronous work, refuses redirects
and bounds payloads. Closing a runtime fences native ingress and retains stores
while owned producers settle. Canonical domain completion and native process
observations remain separate facts.

Capability issuance persists its request and protected vault handle before HTTP.
The owner retains in-flight producers when an observer cancels. Restore requires
current metadata and an applied matching Core lease; it cannot reconstruct a
missing secret or reanchor a persisted TTL. General automatic capability recovery,
renewal scheduling and terminal reservation cleanup must not be inferred from
this restore port. Those remaining lifecycle requirements need their own evidence.

## Boundaries and current acceptance

| Area | Implementation boundary |
|---|---|
| CLI/services | Explicit selection, protected input, durable management/runtime intents |
| Daemon/control | Exclusive connection ownership, reconciliation, dispatch and publication |
| Core | Native adapters/processes, technical qualification, journal and lease enforcement |
| Vault/state | Credential material in protected storage; ordinary state carries references |
| Nexus | Agent policy, consent/proposals, admission, grants and canonical work |

Core 0.2.53.dev0 exports an executable R4 bundle. That fact does not qualify every
provider/platform or prove independent-host operation. Windows and Linux have
containment backends; macOS managed execution is unsupported. Attach is not
advertised as qualified managed execution. The complete provider/fault/platform
matrix, two-Server topology, independent hosts and final artifact freeze remain
under acceptance.

Operational commands are in the [runbook](runbook.md) and
[R4 onboarding guide](../README.md#quickstart). Historical increment reports live
under [implementation evidence](../plans/implementation/); their old disabled-gate
statements describe their original commits, not the current executable bundle.
