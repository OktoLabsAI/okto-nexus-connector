# R4 binding consent DTO

The R4 HTTP proposal DTO now retains nullable `profile_id`,
`diff.requires_operator`, and `diff.fields_changed`, alongside the existing
approval references. Invalid field types are rejected as `VERSION_INCOMPATIBLE`.
The existing `apply_r4_binding` transports `operator_proof_ref` unchanged.

The Nexus approval queue owns the operator decision. The Connector does not
generate proofs, approve itself or apply an execution effect while binding.
Runtime grant issuance remains separate from binding approval.

The six contract cases in `tests/contract/test_r4_binding_consent.py` cover
nullable profiles, complete consent projection, exact proof transport, and
invalid consent shapes. Campaign results are recorded under
`plans/implementation/evidence/r4-binding-consent*.xml`. The coordinated Nexus
campaign also exercises the real HTTP client with an actual Nexus approval
decision and the same Core wheel.

This increment contributes to CON-R4-03/05. It does not close daemon, CLI,
provider or multi-host acceptance. Core remains pinned at `0.2.28.dev0` with
SHA-256 `27df75100dea033ca5456f2d571eb41b6311fa3ce530a723ecd6c606d257953c`.

## Results

Six new contract cases passed. The full source campaign recorded 246 passes,
two skips and one packaging isolation failure. The packaging subprocesses now
exclude inherited source imports; both packaging cases passed on rerun.
The failure and rerun remain in the evidence, with provenance in
`evidence/r4-binding-consent-summary.json`. The coordinated installed
Nexus/Connector campaign passed 27 overlapping cases.
