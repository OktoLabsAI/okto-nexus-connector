# Threat model — okto-nexus-connector (plan C00.4)

Assets, boundaries and the honest limits of what this application can
and cannot protect against. There is **no Nexus user entity** anywhere:
the OS account running a service is only a local permission boundary.

## Trust boundaries

```text
Nexus Server ─(WSS/HTTPS, canonical key)─ daemon ─(native stdio)─ harness
     │                                        └─(IPC, per-user token)─ CLI/TUI
     └─(direct HTTP, capability)─ harness MCP client (no connector in path)
```

| Boundary | Mechanism | Residual risk (stated honestly) |
|---|---|---|
| OS account → daemon | Per-user state dir (0o700 / user-profile ACL), lock with process birth token, IPC hello-token | A malicious process under the **same OS account** can read the vault and control the daemon; this is the trust domain, not a bug we can fix in-process |
| Daemon → Server | TLS (no silent verify=False), canonical-key auth, exact contract revision, cross-origin redirect refusal | A compromised Server can send authorized intents; the Core still gates realization to approved roots/binaries |
| Daemon → harness child | Core-owned argv (no shell), whitelisted environment, secret refs resolved per launch, Job Object/guardian containment | Provider sandbox ≠ account isolation; hooks in an approved provider home run with that consent only |
| CLI → daemon | Loopback/unix IPC with 256-bit token in a user-only file; unauthenticated peers are refused before any dispatch (TC-08) | Same-account local attackers are inside the domain |
| Journal/state at rest | User-only permissions; secrets only as references; redaction on every IPC/log/export surface | Disk compromise of the account equals domain compromise |

## Threats considered

- **Credential scavenging** — no scanning of files/keychains; import is
  explicit; entries from MCP configs require origin match + explicit
  selection (TC-04/TC-05).
- **Hint/payload spoofing** — the Server derives `agent_id` from the key;
  client hints are compared, never trusted (AGENT_ID_MISMATCH abort).
- **Lane ticket theft** — tickets are short-lived, binding-scoped, never
  in URLs/logs/journal; attach frames are redacted as sensitive.
- **Revocation races** — revocation invalidates tickets/lanes; the local
  lease fence (monotonic, rollback-fenced) bounds offline authority; no
  auto-renewal can resurrect a rotated canonical key.
- **Hostile config** — path traversal, foreign MCP entries and hooks fail
  closed; owned-entry edits require exact predecessor match (CAS);
  concurrent edits produce drift errors, not overwrites.
- **Resource exhaustion** — bounded IPC frames/connections, finite event
  queues with an urgent budget, journal quotas with critical reserve;
  saturation reports honestly instead of dropping terminals silently.
- **Uncertain-effect replay** — operation IDs are durable before effects;
  duplicates return known receipts; conflicts raise; unknowns never
  auto-retry (A.9).
- **Process spoofing** — PID alone is never identity: readiness requires
  the process birth token and a live authenticated IPC ping (C02.1).

## Out of scope / not promised

- Protection against the OS account itself (same-user malware).
- Hardware attestation or clone discrimination beyond journal CAS and
  generations (A.12: no host identity claims).
- Exactly-once external effects; MCP termination or proxying (there is
  none); surviving logout/boot without a qualified service manager.
