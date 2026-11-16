import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import sys
import zipfile

root,core,stage=Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3]
sha=lambda value:hashlib.sha256(value).hexdigest()
site=(root/'venv/Lib/site-packages').resolve()
report=dict(python=sys.version,isolated=sys.flags.isolated,packages={},sys_path=list(sys.path))
assert sys.flags.isolated and not any(str(root/'snapshot/src').casefold() in p.casefold() for p in sys.path)
for module,wheel in [('okto_nexus_connector',next((root/'wheels').glob('okto_nexus_connector-*.whl'))),
                     ('nexus_connector_core',core)]:
    loaded=importlib.import_module(module)
    path=Path(loaded.__file__).resolve()
    assert path.is_relative_to(site)
    with zipfile.ZipFile(wheel) as archive:
        data={name:archive.read(name) for name in archive.namelist() if name.startswith(module+'/') and not name.endswith('/')}
    installed={p.relative_to(site).as_posix():p.read_bytes() for p in (site/module).rglob('*')
        if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc'}
    assert data==installed,(module,'wheel/install differ')
    source_match=None
    if module=='okto_nexus_connector':
        source={p.relative_to(root/'snapshot/src').as_posix():p.read_bytes() for p in (root/'snapshot/src'/module).rglob('*')
            if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc'}
        assert data==source,'snapshot/wheel differ'
        source_match=True
    report['packages'][module]=dict(module_file=str(path),package_files=len(data),source_equal=source_match,
        wheel_install_equal=True,wheel=str(wheel),wheel_sha256=sha(wheel.read_bytes()),
        files={name:sha(value) for name,value in data.items()})
report['installed_versions']={name:importlib.metadata.version(name) for name in
    ('okto-nexus-connector','okto-nexus-connector-core','mcp','setuptools','wheel','pytest','pip')}
(root/f'package-proof-{stage}.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps({name:value['package_files'] for name,value in report['packages'].items()}))
