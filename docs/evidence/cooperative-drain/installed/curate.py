"""Copy only explicit public proof artifacts, never fixture homes/databases."""
import hashlib
import json
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

root=Path(__file__).parent
public=root/'public'
public.mkdir(exist_ok=True)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
read=lambda name:json.loads((root/name).read_text(encoding='utf-8'))
write=lambda name,data:(public/name).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
before,after=read('inputs-before.json'),read('inputs-after.json')
proof0,proof1=read('package-proof-before.json'),read('package-proof-after.json')
imports=read('test-imports.json')
assert after['unchanged'] and after['core_wheel_unchanged']
assert before['files']==after['files']==after['snapshot']
assert proof0['packages']==proof1['packages']
assert imports['isolated']==1 and imports['pytest_exit_code']==0
assert not any('src' in [part.casefold() for part in Path(path).parts] for path in imports['sys_path'])
snapshot=(root/'snapshot').resolve()
assert all(Path(path).resolve()==snapshot or Path(path).resolve().is_relative_to(snapshot/'tests')
    for path in imports['sys_path'] if Path(path).resolve().is_relative_to(snapshot))
rows=list(ET.parse(root/'installed.xml').iter('testcase'))
passed=sum(all(row.find(k) is None for k in ('failure','error','skipped')) for row in rows)
skipped=[dict(name=row.get('name'),classname=row.get('classname'),reason=row.find('skipped').get('message'))
         for row in rows if row.find('skipped') is not None]
assert len(rows)==195 and passed==194 and len(skipped)==1
stages=['venv','dependencies','pip-check-core','build','install','pip-check-final','proof-before','tests','proof-after']
allowlist=['qualify.py','package_proof.py','installed_tests.py','curate.py','third-party.txt',
    'inputs-before.json','inputs-after.json','package-proof-before.json','package-proof-after.json','test-imports.json','installed.xml']
for stage in stages:
    assert read(f'{stage}.invocation.json')['exit_code']==0
    allowlist.extend(f'{stage}.{suffix}' for suffix in ('invocation.json','stdout.txt','stderr.txt'))
for name in allowlist:
    shutil.copy2(root/name,public/name)
wheel=next((root/'wheels').glob('okto_nexus_connector-*.whl'))
write('reports.json',dict(scope='Connector SVC-07 installed wheel, Windows/local synthetic',
    wheel=dict(path=str(wheel),sha256=sha(wheel)),core_wheel=before['core_wheel'],
    tests=dict(report='installed.xml',collected=195,passed=194,failed=0,errors=0,skipped=skipped,
        new_cases=sum(row.get('classname','').endswith(('test_operator_drain','test_r4_operator_drain')) for row in rows)),
    package_proof=dict(connector_source_wheel_install_equal=124,core_wheel_install_equal=166,
        input_files_unchanged=332,checked_before_and_after=True,installed_module_origins=len(imports['origins'])),
    isolation=dict(python_isolated=True,source_paths_on_sys_path=False,fixture_paths='snapshot root/tests only',
        environment='Explicit OS-tool allowlist, synthetic Temp homes, no inherited credentials or PYTHONPATH',
        shared_venv_modified=False,nexus_wheel_used=False),
    dependencies=dict(versions=proof1['installed_versions'],third_party_sha256=sha(root/'third-party.txt'),
        pip_check_core='PASS',pip_check_final='PASS'),
    preparation_failures=0,all_preparation_stdout_stderr_retained=True,
    limits=['Synthetic native peers and supervisor adapter; no personal provider/vault/native-service installation.',
        'This selection does not qualify productive M1/M2 or Nexus SVC-07 (not installed in this environment).',
        'Source and installed campaigns overlap; do not add their testcase counts.',
        'No checkout, vendor or shared environment changes by this qualification. No commit/push.']))
(public/'README.md').write_text('''# Installed Connector SVC-07

The exact Connector wheel in `reports.json` was built from a frozen copy, installed into a new Temp venv with Core wheel `1407014…`, pinned third-party dependencies, setuptools 84.0.0 and wheel 0.45.1. Both pip checks passed. No Nexus wheel was needed.

The installed run used Python `-I`; only snapshot root/tests were added for fixtures. All loaded Connector/Core modules came from this venv's site-packages. Source paths were absent from sys.path. The 124 Connector package files matched source → wheel → install, and all 166 Core package files matched wheel → install, before and after tests. All 332 source/snapshot inputs retained their hashes.

`installed.xml`: 194 passed, one preexisting conditional non-Windows-only skip, zero failures/errors; 26 new drain cases are included. The source run and this installed run overlap and must not be summed. All preparation/test logs are retained; no preparation failed.

Scope is Windows/local synthetic: real installed packages/Core/SQLite and fake native/HTTP/WSS peers. No personal provider, credential vault, native service installation, productive M1/M2, or Nexus SVC-07 campaign is claimed. The allowlist excludes all fixture homes, databases, venv files and caches. The wheel remains separately in the adjacent wheels directory with its hash pinned in reports.json.
''',encoding='utf-8')
allowlist.extend(['reports.json','README.md'])
write('manifest.json',{name:dict(sha256=sha(public/name),bytes=(public/name).stat().st_size) for name in sorted(allowlist)})
assert {p.name for p in public.iterdir() if p.is_file()}==set(allowlist)|{'manifest.json'}
print(json.dumps(dict(public=str(public),artifacts=len(allowlist)+1,manifest_sha256=sha(public/'manifest.json'),
    reports_sha256=sha(public/'reports.json'),loaded_modules=len(imports['origins']),wheel_sha256=sha(wheel)),indent=2))
