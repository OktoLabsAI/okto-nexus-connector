# Executable Core R4 adoption

Connector pins Core 0.2.53.dev0 with the identical Nexus wheel:
`cc873031378793d374a9bbc00572c7a525f99c4246324a941713d866cc62c6b1`.
The existing explicit R4 codec/transport uses the retained preview alias,
which equals the promoted `R4_CONTRACT_REVISION`. The legacy WSS transport
continues to use the unchanged historical R3 `CONTRACT_REVISION`.

Fresh installed development Connector wheel SHA-256:
`68df39616fd8567306f2de8c63fe7a41380ae7603f3e03b244705266083d75cd`.
Directed runtime, renewal, publication and approved-launch regressions:
**66 passed in 43.90 seconds** with the new Core and Connector installed.
See `tests.xml`. Nexus is separately running actual negotiated local/remote
consumer conformance against the same packages. This is not final freeze,
native-provider or independent-machine acceptance.
