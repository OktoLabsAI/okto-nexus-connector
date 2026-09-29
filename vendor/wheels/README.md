# Wheel Core fixado para R4

Current: `nexus_connector_core-0.2.14.dev0-py3-none-any.whl`

SHA-256: `759cdee946037ed5e215f901cdfb09b31baf350c7fde69e4d5f79bd41f515bee`

É o mesmo artefato usado pelo Nexus. Para instalar o Connector via pip antes
de publicar o Core em um índice, use `pip install --find-links vendor/wheels .`
na raiz deste repositório. O NXL ainda é R3; o artefato não habilita execução
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
