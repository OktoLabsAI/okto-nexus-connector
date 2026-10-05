import io
from types import SimpleNamespace

import pytest

from okto_nexus_connector.cli.commands import reconnect
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError


@pytest.mark.parametrize('running', [True, False])
async def test_reconnect_restarts_all_without_erasing_configuration(tmp_path, monkeypatch, running):
    saved = tmp_path / 'state.json'
    saved.write_text('preserved')
    calls = []
    def stop(root, timeout):
        calls.append(('stop', root, timeout))
        return dict(was_running=running, stopped=running)
    def start(root):
        calls.append(('start', root))
        return {'started': True}
    monkeypatch.setattr(reconnect.manager, 'stop', stop)
    monkeypatch.setattr(reconnect.manager, 'start', start)
    result = await reconnect.run_reconnect(build_parser().parse_args(['reconnect']), Output(json_mode=True, stream=io.StringIO()), tmp_path)
    assert result['reconnect_requested'] and result['scope'] == 'all'
    assert calls == [('stop', tmp_path, 30), ('start', tmp_path)]
    assert saved.read_text() == 'preserved'


async def test_reconnect_does_not_start_second_daemon_during_drain(tmp_path, monkeypatch):
    monkeypatch.setattr(reconnect.manager, 'stop', lambda *a, **k: dict(was_running=True, stopped=False))
    def start(*args):
        raise AssertionError('A second daemon must not start')
    monkeypatch.setattr(reconnect.manager, 'start', start)
    with pytest.raises(ConnectorError, match='still draining'):
        await reconnect.run_reconnect(SimpleNamespace(), Output(json_mode=True, stream=io.StringIO()), tmp_path)
