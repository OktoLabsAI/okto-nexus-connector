"""Retain exact diagnostic reports; no profiles, credentials, databases or wheels."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
REPO = Path('D:/Projetos/Techridy/okto-nexus-connector')
OUT = REPO / 'docs/evidence/diagnostics-v1'
SOURCE = Path('C:/Users/jpamb/AppData/Local/Temp/dx0714-diagnostics-proposal-yv2lnouo')
sha = lambda value: hashlib.sha256(value).hexdigest()
manifest_path = OUT / 'manifest.json'

if sys.argv[1] == 'collect':
    assert not manifest_path.exists()
    OUT.mkdir(parents=True, exist_ok=True)
    pairs = [(SOURCE / name, name) for name in (
        'connector-source-initial.xml', 'connector-source-second.xml', 'connector-source-final.xml',
        'connector-source-final-v2.xml', 'connector-contract-final-v2.xml', 'connector-contract-final-v3.xml',
        'connector-schema-conformance.json', 'connector-schema-conformance-final-v2.json',
        'connector-schema-conformance-final-v3.json', 'connector_doctor_test_guard.py')]
    pairs += [(ROOT / name, name) for name in ('build_connector.py', 'qualify_connector.py', 'retain_connector_evidence.py')]
    pairs += [(ROOT / 'qualified-connector' / name, name) for name in
              ('package-proof.json', 'installed.xml', 'installed-qualification.json')]
    rows = []
    for source, name in pairs:
        data, target = source.read_bytes(), OUT / name
        if target.exists():
            assert target.read_bytes() == data
        else:
            target.write_bytes(data)
        rows.append(dict(path=target.relative_to(REPO).as_posix(), original_path=str(source),
            bytes=len(data), sha256=sha(data), original_sha256=sha(data), exact_original_bytes=True))
    reports = {}
    identities = {}
    for row in rows:
        if row['path'].endswith('.xml'):
            cases = list(ET.parse(REPO / row['path']).getroot().iter('testcase'))
            name = Path(row['path']).name
            failures = sum(c.find('failure') is not None for c in cases)
            errors = sum(c.find('error') is not None for c in cases)
            skipped = sum(c.find('skipped') is not None for c in cases)
            reports[name] = dict(tests=len(cases), passed=len(cases)-failures-errors-skipped,
                                 failures=failures, errors=errors, skipped=skipped)
            identities[name] = {(c.get('classname'), c.get('name')) for c in cases if c.find('failure') is None}
    assert reports['installed.xml'] == dict(tests=93, passed=93, failures=0, errors=0, skipped=0)
    assert reports['connector-source-final-v2.xml'] == reports['installed.xml']
    assert identities['installed.xml'] == identities['connector-source-final-v2.xml']
    assert identities['connector-contract-final-v3.xml'] <= identities['installed.xml']
    package = json.loads((OUT / 'package-proof.json').read_text(encoding='utf-8'))
    assert package['wheel_sha256'] == '580ddc45266e8659abda9eaa834c4e46c95937d6a274769b4c716331e9960703'
    for name, values in package['files'].items():
        assert sha((REPO / 'src/okto_nexus_connector' / name).read_bytes()) == values['source_sha256']
        assert sha((Path(package['installed_package']) / name).read_bytes()) == values['installed_sha256']
    proof = json.loads((OUT / 'installed-qualification.json').read_text(encoding='utf-8'))
    assert proof['exit_code'] == 0 and proof['xml_sha256'] == sha((OUT / 'installed.xml').read_bytes())
    manifest = dict(schema_version=1, artifacts=rows, reports=reports,
        distinct_final_source_cases=93, distinct_installed_cases=93, source_installed_overlap=93,
        reruns_not_summed=True,
        common_contract_sha256='06dbbea77eb0870058bc2fe4c9afc81023ea99f857612984321d1f8e6d20c422',
        qualification='Installed non-editable Connector/Core; synthetic vault, HTTP, inventory and containment observations only.',
        initial_failure='A synthetic IdentityRecord used alias/agent positional arguments in the wrong order. Corrected to keyword arguments; original failure XML retained. No product fix was inferred from it.',
        excluded=['wheel files', 'databases', 'private credential material', 'isolated test profiles'])
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
else:
    assert sys.argv[1] in ('verify', 'staged')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))

for row in manifest['artifacts']:
    assert sha((REPO / row['path']).read_bytes()) == row['sha256'], row['path']
if sys.argv[1] == 'staged':
    rows = [*manifest['artifacts'], dict(path=manifest_path.relative_to(REPO).as_posix(), sha256=sha(manifest_path.read_bytes()))]
    rows += [dict(path=p.relative_to(REPO).as_posix(), sha256=sha(p.read_bytes()))
             for p in (REPO / 'tests/fixtures/diagnostics_v1').iterdir() if p.is_file()]
    raw = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=REPO,
        input=''.join(':' + row['path'] + '\n' for row in rows).encode())
    offset = 0
    for row in rows:
        end = raw.index(b'\n', offset)
        header = raw[offset:end].split()
        assert len(header) == 3 and header[1] == b'blob'
        size = int(header[2])
        assert sha(raw[end+1:end+1+size]) == row['sha256'], row['path']
        offset = end + size + 2
    assert offset == len(raw)
print(json.dumps(dict(mode=sys.argv[1], artifacts=len(manifest['artifacts']), source=93, installed=93, overlap=93)))
