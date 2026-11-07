import json
from pathlib import Path
import sys

root=Path(sys.argv[1])
snapshot=root/'snapshot'
site=(root/'venv/Lib/site-packages').resolve()
assert sys.flags.isolated and not any('PYTHONPATH'==p for p in sys.path)
# Only the snapshot root for tests/fixtures. The src layout stays inaccessible.
sys.path.insert(0,str(snapshot))
import okto_nexus_connector
import nexus_connector_core
import pytest
for module in (okto_nexus_connector,nexus_connector_core):
    assert Path(module.__file__).resolve().is_relative_to(site)
selection=['tests/unit/test_operator_drain.py','tests/unit/test_r4_operator_drain.py',
    'tests/unit/test_r4_execution.py','tests/unit/test_r4_daemon_execution.py','tests/unit/test_r4_daemon_control.py',
    'tests/unit/test_user_service_lifecycle.py','tests/unit/test_service_readiness.py','tests/unit/test_ipc_protocol.py',
    'tests/integration/test_daemon_app.py','tests/unit/test_core02_integration.py','tests/unit/test_r4_events.py']
code=pytest.main(['-q','-o',f'cache_dir={root}/pytest-cache',f'--basetemp={root}/fixtures',
    f'--junitxml={root}/installed.xml',*selection])
origins={name:str(Path(module.__file__).resolve()) for name,module in sys.modules.items()
    if name.startswith(('okto_nexus_connector','nexus_connector_core')) and getattr(module,'__file__',None)}
assert all(Path(path).is_relative_to(site) for path in origins.values())
assert str(snapshot/'src') not in sys.path
(root/'test-imports.json').write_text(json.dumps(dict(isolated=sys.flags.isolated,origins=origins,
    sys_path=list(sys.path),pytest_exit_code=code),indent=2)+'\n',encoding='utf-8')
raise SystemExit(code)
