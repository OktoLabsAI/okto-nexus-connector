from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_nexus_connector.cli.commands import doctor


@pytest.mark.asyncio
async def test_probe_uses_selected_state_directory(tmp_path, monkeypatch):
    used = []
    def vault(path, **kwargs):
        used.append(path)
        return SimpleNamespace(resolve=lambda handle: 'test-key')
    monkeypatch.setattr(doctor, 'open_vault', vault)
    http = AsyncMock()
    http.__aenter__.return_value = http
    http.me.return_value = SimpleNamespace(agent_id='agent')
    monkeypatch.setattr(doctor, 'NexusHTTPClient', lambda url: http)
    state = SimpleNamespace(identities=[SimpleNamespace(
        server_id='server', agent_id='agent', secret_handle='test-handle')])
    result = await doctor._probe_server(
        SimpleNamespace(server_id='server', base_url='http://localhost'), state, tmp_path)
    assert result == ('ok', '/me agent', '')
    assert used == [doctor.paths.vault_dir(tmp_path)]
