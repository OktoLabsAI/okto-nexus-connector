# Diagnostics v1 qualification

`doctor --diagnostic-format v1` emits the shared `okto.diagnostics` contract with
separate proof status, stable codes, controlled explanations, scoped subjects
and complete counts. `legacy` remains the default. A confirmed stop, missing
evidence and an excluded topology are distinct. Binary validation and transport
readiness do not certify provider login or completed work.

The existing explicit Server probe now also compares the returned Server ID
with the saved selection, in both formats. A matching agent on another Server
is refused. The new projection adds no network, credential or process checks;
the existing `--probe` boundary and CLI exit conventions remain intact.

The final shared artifact digest is
`06dbbea77eb0870058bc2fe4c9afc81023ea99f857612984321d1f8e6d20c422`.
The JSON fixtures and pin are preserved byte-for-byte by `.gitattributes`.

Qualification on Windows/Python 3.13:

- 93 distinct source cases passed; the final contract-only rerun overlaps them.
- The same 93 cases passed against non-editable Connector 0.0.12/Core 0.0.14
  wheels installed outside either checkout. Import guards checked package paths
  for every test; Nexus application imports were forbidden.
- All 119 Connector package files (118 Python files and `py.typed`) match source,
  detached build, wheel and installation bytes.
- Schema checks cover 67 Connector findings and seven synthetic cross-component
  reports. These fixtures do not claim a real host or provider campaign.

The Connector wheel SHA-256 is
`580ddc45266e8659abda9eaa834c4e46c95937d6a274769b4c716331e9960703`.
The unchanged Core wheel SHA-256 is
`5721e462add21d65380f070a68db07a653a498a78d4be9ea0cf949909d3cfecb`.

Vault, HTTP, inventory and containment observations were synthetic. Tests used
isolated profiles and a guard against unintended real credential/preflight
access. No provider, real Server, personal vault or native service was qualified.
An initial test fixture used positional identity fields in the wrong order;
its original failed report is retained alongside the corrected runs.

Exact reports, drivers, package/import proofs and overlap counts are retained in
[the evidence manifest](evidence/diagnostics-v1/manifest.json). No wheel, database
or private credential file is included. Source and installed counts overlap and
must not be added together. Installed Nexus diagnostics and the remaining R3
campaigns are qualified separately.
