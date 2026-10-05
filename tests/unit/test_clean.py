import io
import json
from types import SimpleNamespace as NS
import pytest
from okto_nexus_connector.cli.commands import clean
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault


@pytest.fixture
def environment(tmp_path,monkeypatch):
    root=tmp_path/'connector'
    root.mkdir()
    (root/'state.json').write_text(json.dumps({'identities':[{'secret_handle':'vault:server/agent'}]}))
    (root/'vault').mkdir()
    vault=RestrictedFileVault(root/'vault',approved=True)
    vault.store('server/agent','synthetic-key')
    (root/'logs').mkdir()
    (root/'logs'/'daemon.log').write_text('test')
    (root/'project.txt').write_text('preserve')
    monkeypatch.setattr(clean,'open_vault',lambda *a,**k:vault)
    monkeypatch.setattr(clean.service_install,'status',lambda r:{'installed':False})
    monkeypatch.setattr(clean.manager,'stop',lambda *a,**k:{'was_running':False})
    return root


async def test_abort_keeps_everything(environment,monkeypatch):
    monkeypatch.setattr(clean,'confirm',lambda *a,**k:False)
    result=await clean.run_clean(build_parser().parse_args(['clean']),Output(json_mode=False,stream=io.StringIO()),environment)
    assert result['aborted'] and (environment/'state.json').exists()


@pytest.mark.parametrize('flag',['--json','--non-interactive'])
async def test_automation_requires_explicit_yes(environment,flag):
    with pytest.raises(ConnectorError,match='confirmation'):
        await clean.run_clean(build_parser().parse_args([flag,'clean']),Output(json_mode=True),environment)
    assert (environment/'state.json').exists()


def test_clean_removes_state_but_preserves_unowned_files(environment):
    result=clean.cleanup(environment)
    assert result['cleaned'] and result['credentials_removed']==1
    assert not (environment/'state.json').exists()
    assert not (environment/'vault').exists()
    assert not (environment/'logs').exists()
    assert (environment/'project.txt').read_text()=='preserve'
    assert clean.cleanup(environment)['cleaned']


def test_failed_shutdown_preserves_state(environment,monkeypatch):
    monkeypatch.setattr(clean.manager,'stop',lambda *a,**k:{'was_running':True,'stopped':False})
    with pytest.raises(ConnectorError,match='shutdown'):clean.cleanup(environment)
    assert (environment/'state.json').exists()


def test_home_directory_is_rejected(monkeypatch,tmp_path):
    monkeypatch.setattr(clean.Path,'home',lambda:tmp_path)
    with pytest.raises(ConnectorError,match='dedicated'):clean.targets(tmp_path)


def test_service_uninstall_failure_preserves_state(environment,monkeypatch):
    monkeypatch.setattr(clean.service_install,'status',lambda r:{'installed':True})
    monkeypatch.setattr(clean.service_install,'uninstall',lambda *a,**k:None)
    with pytest.raises(ConnectorError,match='autostart'):clean.cleanup(environment)
    assert (environment/'state.json').exists()


def test_locked_keyring_preserves_state(environment,monkeypatch):
    class Locked:
        def get_password(self,*a):raise RuntimeError('locked')
    monkeypatch.setattr(clean,'open_vault',lambda *a,**k:clean.KeyringVault(Locked()))
    with pytest.raises(ConnectorError,match='Unlock'):clean.cleanup(environment)
    assert (environment/'state.json').exists()


def test_symlink_does_not_delete_target(environment,tmp_path):
    target=tmp_path/'outside'
    target.mkdir()
    (target/'keep').write_text('safe')
    try:(environment/'runtime').symlink_to(target,target_is_directory=True)
    except OSError:pytest.skip('symlink privilege unavailable')
    with pytest.raises(ConnectorError,match='redirected'):clean.cleanup(environment)
    assert (target/'keep').exists()
