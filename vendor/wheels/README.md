# One-shot development dependency

Connector 0.0.8 requires Core 0.0.10. Local wheel SHA-256:
`d66b60df1db82fafd0f9f9da1a786f229a41e4fe15eacbf158ad836651fad73e`.
Includes nonblocking Pi launch validation and safe stream-loss diagnostics.
This development artifact is not published.

# Current release dependency

Core 0.0.8 is required by Nexus 0.2.2 and Connector 0.0.6.
Shared wheel SHA-256: `7d8003095343da7dbcb8b924abf1623b155677002b0acc435299a13504db323f`.
Built from Core commit `74430e7`: preserve confirmed Codex initialization-failure containment.
The entries below document historical artifacts.

Earlier release notes follow.

# Current release dependency

Core 0.0.3 is required by Nexus 0.2.2 and Connector 0.0.2.
The shared wheel SHA-256 is `504e744002f91158995bc9bf4d6f1d6c982462abbe9c864b575865fda9c0aff7`.

Current PyPI release candidate: `okto_nexus_connector_core-0.0.1-py3-none-any.whl`
SHA-256: 1547d389913f6de6b9060f03b9492b38be660d876ab1d6a5298442720cf28421.
Python imports remain `nexus_connector_core`.

## Current development artifact: 0.2.53.dev0

nexus_connector_core-0.2.53.dev0-py3-none-any.whl

SHA-256: cc873031378793d374a9bbc00572c7a525f99c4246324a941713d866cc62c6b1

Executable R4 contract with an explicit revision identity; historical R3 bytes
remain unchanged. Provider and final release acceptance remain separate.

## Previous development artifact: 0.2.52.dev0

nexus_connector_core-0.2.52.dev0-py3-none-any.whl

SHA-256: 470eb23b28d917a3a154c2ef7cd9fdddf972ca6402c0668961b4c01909f1b732

Lease limits compare absolute deadlines, accepting the exact maximum without floating-point subtraction drift. The next representable deadline remains rejected before native effects. Both consumers use identical bytes; final release gates remain open.

Earlier entries below describe historical artifacts, not the current dependency.

## Previous development artifact: 0.2.51.dev0

nexus_connector_core-0.2.51.dev0-py3-none-any.whl

SHA-256: 4c4c0c58d25b93f4f08ba8515d8476ffc29d1f0a27cc577c9fccb610716d1d1c

Lost R4 revocation acknowledgements recover only from the exact durable fence row, without another CAS. Mismatched rows do not authorize an ACK or native work. Release gates remain open.

## Previous development artifact: 0.2.50.dev0

nexus_connector_core-0.2.50.dev0-py3-none-any.whl

SHA-256: 797e28d34ba43800e5ad800b0fb344ce53a4160389ac817b4687505187c25215

Public memory-only shutdown_resources distinguishes observed STOPPED from pending durable release. It does not authorize disposal or replace shutdown outcomes. R4 release gates remain open.

## Previous development artifact: 0.2.35.dev0

nexus_connector_core-0.2.35.dev0-py3-none-any.whl, SHA-256
20013ae173652ea3726f5b7ae5d7c37e66eac8f9bc319ae3c36f84e70247e760.

Retained policy-close receipt completion.

## Previous development artifact: 0.2.33.dev0

nexus_connector_core-0.2.33.dev0-py3-none-any.whl, SHA-256
1f119d5626e9de4c8dc2589b0941289d300e995b6a422ebc16b627be5b61fee1.

Durable R4 terminal close receipts.

## Previous development artifact: 0.2.32.dev0

nexus_connector_core-0.2.32.dev0-py3-none-any.whl, SHA-256
a32d49400c0e22ccedd1fbe7f26b8f74ff0b3bb2d55f21eac469dd0ae8a8784f.

Confirmed native close outcomes.

## Previous development artifact: 0.2.31.dev0

nexus_connector_core-0.2.31.dev0-py3-none-any.whl, SHA-256
6e3e6e08acee45c8c220bb87d8f79b9697cc95c177b73f37414aa601880d6339.

Exact Windows Codex 0.159.0 conversation and control qualification.

## Previous development artifact: 0.2.30.dev0

nexus_connector_core-0.2.30.dev0-py3-none-any.whl, SHA-256
e060e033be05fdd4c9aff5c90902d191483d400b3f6778fa7bb5395b66f561e6.
The same bytes are used by Nexus and Connector. Pi native action ingress
retains backend producers through timeout and shutdown. Both hosts compose
the public Pi launch owner and retain stores while native domain work remains
pending. Automatic approved configuration and real provider acceptance remain
pending; R4 readiness remains false.

## Historical development artifact: 0.2.29.dev0

`nexus_connector_core-0.2.29.dev0-py3-none-any.whl`, SHA-256
`a465c1ec1aaba9814872b21984a436bbf4cac5f28c2b4bed776f1a7042426cba`.
The same wheel is used by Nexus and Connector. Native domain capabilities
are fenced by the current installed R4 session authority, independently of
the seven runtime operations. Host lifecycle integration and provider
qualification remain pending. R4 execution readiness remains false.

# Wheel Core fixado para R4

Current: `nexus_connector_core-0.2.14.dev0-py3-none-any.whl`

SHA-256: `759cdee946037ed5e215f901cdfb09b31baf350c7fde69e4d5f79bd41f515bee`

ÃƒÆ’Ã¢â‚¬Â° o mesmo artefato usado pelo Nexus. Para instalar o Connector via pip antes
de publicar o Core em um ÃƒÆ’Ã‚Â­ndice, use `pip install --find-links vendor/wheels .`
na raiz deste repositÃƒÆ’Ã‚Â³rio. O NXL ainda ÃƒÆ’Ã‚Â© R3; o artefato nÃƒÆ’Ã‚Â£o habilita execuÃƒÆ’Ã‚Â§ÃƒÆ’Ã‚Â£o
remota R4. Os wheels 0.2.12 e 0.2.13 permanecem apenas para rastreabilidade.

Current pinned artifact: `nexus_connector_core-0.2.18.dev0-py3-none-any.whl`,
SHA-256 `547ab7dfde09adff79f38f5cdf688e5b9d0d7ed3caddf4de445766afdefde0da`.
It is byte-identical to the wheel in Nexus and Core. R4 remote execution
remains disabled until the host gates are qualified.

Current pinned artifact: `nexus_connector_core-0.2.19.dev0-py3-none-any.whl`,
SHA-256 `3b1b334f61f8ad5a2a59c63dcd127ced26a9d426dde7bff68085e4081d01c072`.
Projection failures after a Core receipt preserve possible effect and refuse
safe retry. R4 remote execution remains disabled.

Current pinned artifact: `nexus_connector_core-0.2.20.dev0-py3-none-any.whl`,
SHA-256 `7e9addcb72c52aefe35ea136b4c706721b104f6f9ce4a804a0071d2e829ec4c1`.
The Connector can publish verified steer receipts through the Core projection.
R4 remote execution remains disabled.

Current pinned artifact: `nexus_connector_core-0.2.21.dev0-py3-none-any.whl`,
SHA-256 `6cf55425acc44b4ead9bfd1abd6e216d2c9ed00c7e137d76800b1a42f2f065ed`.
The HTTPS client publishes verified interrupt and close receipts as well.
R4 remote execution remains disabled.

Current pinned artifact: `nexus_connector_core-0.0.1-py3-none-any.whl`, built from the first-release Core source (version reset from `0.2.63.dev0` to `0.0.1`).
SHA-256: `19a5bb4bf1e43bc2d523682a1aa4b1f14305e7ce21c8a5a5704b98efc5b8d600`.
SHA-256 `6af120edf1279cc07c026ba86ce1a2bc856d6982e3acd82fa1b63c951a39c25c`. The same wheel is used by Nexus and Connector regression tests.

The current 0.0.1 artifact includes the exact Okto Nexus LICENSE and SaaS/Branding Addendum.
# Current release dependency

The Connector pins `okto-nexus-connector-core==0.0.1`. Its matching artifact is
`okto_nexus_connector_core-0.0.1-py3-none-any.whl`, built from Core main commit
`a5dd65cf21d8628685a7932c8e701c06d20a9d09`.

SHA-256: `3445c41ce295b8d5b872d5ff720fc55471de87228bd0eb5f97c0a1db2127f64d`.

This artifact includes current connection configuration, native protocol probes,
remote HTTP MCP support and reconnect automation. The older entries below are
historical and are not the dependency selected by the current package metadata.

