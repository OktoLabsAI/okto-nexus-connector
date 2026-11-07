# Remote project setup qualification

Connector 0.0.12 is qualified with Nexus 0.3.3 and unchanged Core 0.0.14.
The installed wheel passed 276 tests with no failure/error/skip; 123 package files match source, detached build, wheel and installation byte for byte.

Wheel: `999048fe9e157ec7b146c762ade8238059c6b52ab8335762a4d07a2dfed6c60d`. Nexus integration wheel: `3a103f923a0b35fdf9a2a504714392b382a9cfae3171bfccbcfd5b65212b279c`. Core: `5721e462add21d65380f070a68db07a653a498a78d4be9ea0cf949909d3cfecb`.

Host drafts, current authenticated handoffs, exact identity import, durable child maps, explicit local re-review, delayed-response CAS and racing-Finish recovery reuse the existing R4 services. No administrative credential enters the host. Retention and concrete-startup-plan limits are described in [connection configuration](../../connection-configuration.md).

Source qualification has 276 unique latest passing identities across the final regression/focal reports; reports overlap. The single broad-regression failure came from an older background fixture without the optional remote_setup field and is retained alongside its correction. Installed tests rerun the complete declared node list against the final wheel outside the checkout with import guards for all three packages.

The new test file received formatting-only changes after this installed run; its Python syntax tree was verified unchanged. Product source and wheel bytes did not change. Lint comparison finds no newly introduced diagnostics; 69 existing diagnostics remain in the changed files.

The Nexus campaign also covers six joint real TCP/WSS cases: ordinary setup, lost decision/apply responses, Server/local re-review and a prior Finish arriving during local amendment. Native/provider behavior and vault facilities are explicitly simulated or isolated. This is not a two-host deployment, real-provider, native-service or M1/M2 acceptance claim. No package was published to a registry.

[Reports](reports.json), [package proof](package-proof.json), [raw artifact manifest](artifact-manifest.json).
