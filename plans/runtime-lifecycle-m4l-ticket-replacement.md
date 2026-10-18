# M4l: ticket recovery after a Nexus restart

## Failure reproduced through the Nexus UI

On the disposable 0.3.1 Nexus at `127.0.0.1:8204`, a local Connector 0.0.10/Pi binding received and answered `M4K_REMOTE_BEFORE_RESTART_OK` through Meta-harness. The operator message reached `NATIVE_ACCEPTED` and `read`; the exact reply was recorded. A Nexus-only restart left the Connector at `CREDENTIAL_REPLACEMENT_REQUIRED` even though its canonical key and execution grant were valid. The Server still had an active, unbound derivative ticket. Its expiry had been extended by Server-side renewal, while the Connector's ticket-intent age exceeded 600 seconds. On a material-unavailable replay the Connector guessed the ticket had expired and omitted `replaces_ticket_id`; the Server correctly refused a second active ticket.

## Correction

The Connector now strictly validates the Server's `CREDENTIAL_REPLACEMENT_REQUIRED` metadata and persists a new request for the reported active ticket under the same durable client intent. It retries at most three times. The local age remains a hint for material-unavailable recovery; an explicit Server statement of an active ticket overrides that hint. No secret or ticket material is persisted in the intent store.

## Verification

- Built and installed the corrected Connector 0.0.10 wheel in the disposable Connector environment with Core 0.0.12. The installed wheel passed 53 ticket/HTTP tests and 66 adjacent daemon-control/execution tests. One timing-sensitive native-claim test failed during the first adjacent run, then passed individually and in the complete rerun (66/66); its failure was unrelated to ticket acquisition.
- After installing the corrected Connector, it reached `CONTROL_READY` and the remote Pi reached `CONNECTED` without rotating the key or changing the binding. A Meta-harness message after the first restart reached `NATIVE_ACCEPTED` and `read`; the Pi returned exactly `M4L_REMOTE_AFTER_RESTART_OK` in about 86 seconds. The UI showed the reply and 1/1 acknowledgement.
- Restarted only the disposable Nexus again while the current binding ticket was still valid (expiry 13:36:19 UTC). The Connector automatically rejoined at 13:31:11 UTC, issued a successor ticket referencing the still-active predecessor, and returned to `CONTROL_READY`/`CONNECTED` before expiry. Agents UI showed the remote Pi Online. No operator action was required.
- The earlier first restart demonstrated the age-versus-Server-renewal failure. The corrected recovery branch is covered directly by the installed ticket tests; the second real restart verifies end-to-end automatic rejoin with an active ticket.

The disposable fixture used a private Nexus home, workspace and Connector state. The global Nexus installation was not started or modified.
