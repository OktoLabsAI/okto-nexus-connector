"""Exercise the installed Connector doctor contracts with synthetic dependencies."""
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent / 'qualified-connector'
REPO = Path('D:/Projetos/Techridy/okto-nexus-connector').resolve()
SITE = ROOT / 'venv/Lib/site-packages'
for key in list(os.environ):
    if key.upper().startswith(('PYTHON', 'PYTEST', 'OKTO_NEXUS', 'NEXUS_', 'CONNECTOR_')):
        os.environ.pop(key, None)
sys.dont_write_bytecode = True
sys.path[:] = [p for p in sys.path if p and not Path(p).resolve().is_relative_to(REPO.parent)]
profile = ROOT / 'test-home'
profile.mkdir()
for key, value in dict(USERPROFILE=profile, APPDATA=profile/'appdata', LOCALAPPDATA=profile/'localappdata').items():
    os.environ[key] = str(value)
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
os.chdir(ROOT)
sys.path.append('C:/Users/jpamb/AppData/Roaming/Python/Python313/site-packages')

for name, version in [('okto-nexus-connector', '0.0.12'), ('okto-nexus-connector-core', '0.0.14')]:
    assert importlib.metadata.version(name) == version
    direct = json.loads(importlib.metadata.distribution(name).read_text('direct_url.json'))
    assert not direct.get('dir_info', {}).get('editable') and direct['url'].endswith('.whl')


def guard():
    paths = {}
    for name, module in list(sys.modules.items()):
        assert name != 'okto_nexus' and not name.startswith('okto_nexus.'), name
        if name == 'okto_nexus_connector' or name.startswith('okto_nexus_connector.') or name == 'nexus_connector_core' or name.startswith('nexus_connector_core.'):
            value = getattr(module, '__file__', None)
            if value:
                path = Path(value).resolve()
                assert path.is_relative_to(SITE), (name, str(path))
                paths[name] = str(path)
    assert not any(Path(p).resolve().is_relative_to(REPO.parent) for p in sys.path if p)
    return paths


tests = ROOT / 'tests'
shutil.copytree(REPO / 'tests', tests, ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.pytest_cache'))
copied = {}
for path in sorted(p for p in tests.rglob('*') if p.is_file()):
    relative = path.relative_to(tests)
    source = REPO / 'tests' / relative
    assert path.read_bytes() == source.read_bytes()
    copied[relative.as_posix()] = dict(source=str(source), copy=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
source_guard = Path('C:/Users/jpamb/AppData/Local/Temp/dx0714-diagnostics-proposal-yv2lnouo/connector_doctor_test_guard.py')
guard_path = ROOT / 'connector_doctor_test_guard.py'
shutil.copy2(source_guard, guard_path)
spec = importlib.util.spec_from_file_location('connector_doctor_test_guard', guard_path)
plugin = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plugin)
config = ROOT / 'pytest.ini'
config.write_text('[pytest]\npythonpath =\nasyncio_mode = auto\nasyncio_default_fixture_loop_scope = function\n', encoding='utf-8')
nodes = ['test_diagnostic_report_contract.py', 'test_doctor_live_probe.py', 'test_doctor_contract.py',
         'test_doctor_platform_support.py', 'test_passive_diagnostics.py', 'test_cli_presentation.py']

import pytest
import pytest_asyncio.plugin


class InstalledGuard:
    def pytest_runtest_setup(self, item):
        guard()

    def pytest_sessionstart(self, session):
        guard()

    def pytest_sessionfinish(self, session, exitstatus):
        guard()


xml = ROOT / 'installed.xml'
code = pytest.main(['-q', '-c', str(config), '--rootdir', str(ROOT), '--junitxml=' + str(xml),
    *[str(tests / 'unit' / node) for node in nodes]], plugins=[InstalledGuard(), plugin, pytest_asyncio.plugin])
proof = dict(exit_code=int(code), python=sys.executable, cwd=str(Path.cwd()), nodes=nodes,
    test_files=copied, isolated_userprofile=str(profile), pytest_config=str(config),
    guard_plugin=str(guard_path), guard_sha256=hashlib.sha256(guard_path.read_bytes()).hexdigest(),
    loaded_package_modules=guard(), sys_path=sys.path, xml=str(xml), xml_sha256=hashlib.sha256(xml.read_bytes()).hexdigest(),
    counts=[dict(s.attrib) for s in ET.parse(xml).getroot().iter('testsuite')])
(ROOT / 'installed-qualification.json').write_text(json.dumps(proof, indent=2), encoding='utf-8')
print(json.dumps({key: proof[key] for key in ('exit_code', 'python', 'xml', 'counts')}, indent=2))
raise SystemExit(code)
