# R4 connection owner — partial M06 integration

`R4Connection` owns the socket reader after control reconciliation. The public
`connect_r4_connection` factory negotiates WSS and transfers ownership once.
It routes lease and attach replies by immutable request IDs while admitting
operation envelopes into separate bounded productive and control inboxes.

The reader validates namespace and connection generation. Core validates the
attach ACK, its original revisions, and its deadline anchored before send.
Rotation invalidates the previous lane before waiting; a delayed ACK cannot
restore it. Consumers retain immutable encoded input and the exact item/byte
reservation until their producer completes. They call `require_current` after
each wait before obtaining a Core context. Releasing twice is an error.

Defaults are 32 productive operations / 256 KiB and eight control operations /
128 KiB, including reservations already taken by consumers. There are at most
16 request producers and 256 lane slots. Capacity is checked before creating
request tasks or accepting operation reservations. Saturation closes/fences
the connection rather than inventing receipts or retrying native work.

Lease exchange owns installation and ACK independently of its waiter. After
the Core installation, it sends `lease.applied` and replays the **same** lease
request. The identical reply establishes that the Server processed the ACK
on that ordered socket. This prevents an HTTP receipt from overtaking the
Server lease commit. It does not install a second lease, change the request,
increment the serial, or reanchor the Core deadline. Missing or changed
confirmation fences the connection and requires reconciliation.

Disconnect fences lanes and wakes requests/consumers. Queued operations are
released without executing; reservations already taken remain with their
producer until explicitly released. Closing cancels only the reader and
heartbeat observers, then awaits shielded request/install producers. Cancelling
the close waiter does not abandon an installation already in progress.

## Evidence

The contract suite uses the actual installed Core lease implementation and
controlled socket ordering. It covers interleaved operations, reversed lease
replies, stale attach ACKs, item/byte saturation with independent control
capacity, cancelled lease/close waiters, wrong-scope replies, timeout and the
post-ACK commit barrier. The Nexus integration test runs a real loopback WSS
Server and this owner through public onboarding, five actions, Core and HTTPS
receipts. Its provider and readiness qualification are synthetic.

The final source regression passed 256 cases with two skips; 13 directed
contracts and 28 coordinated installed cases passed, with overlap. One skip
requires a fuller MCP lab peer; the other is the Windows-excluded shim case.
Neither skip is counted as a pass.

Results and hashes are in `evidence/r4-connection-report.json`; coordinated
installed evidence is in the Nexus `plans/r4_execution/` directory. Initial
collection errors from an old global Core and a test-helper import were
preserved, as were fixture corrections for the strict grant shape/context API.
The installed integration exposed the cross-channel lease/receipt race and
passed after adding the ordered commit barrier. This is a product correction,
not a timing delay in the test.

## Remaining product integration

The daemon still uses its legacy transport. It must persist and load R4
executor/binding identities, compose this connection owner with the physical
realization resolver and Core dispatcher, renew/rotate credentials, and drive
productive/control consumers through the same authority. This owner does not
replace those lifecycle responsibilities or claim readiness by itself.

Nonempty reconciliation, notification/event handling, durable receipt replay,
governance, total shutdown budget, stalled storage supervision and provider /
multi-host qualification remain open. Unsupported frames fence this preview
connection; they are not silently accepted. Existing product readiness gates
remain disabled. No Core wire or wheel change is introduced here.
