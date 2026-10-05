from types import SimpleNamespace

import pytest

from okto_nexus_connector.identity.import_flow import import_identity, replace_credential
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.storage.state_store import StateStore
from okto_nexus_connector.transport.https_client import MeInfo


async def test_import_reimport_and_rotation_use_server_epoch(tmp_path):
    store = StateStore(tmp_path / 'state.json')
    (tmp_path / 'vault').mkdir()
    vault = RestrictedFileVault(tmp_path / 'vault', approved=True)
    epoch = 7
    async def me(key):
        return MeInfo('server', 'agent', 'Agent', (), 3, 2, epoch)
    http = SimpleNamespace(me=me)
    result = await import_identity(http, vault, store, key='test-key', alias='local', agent_hint='agent')
    assert result.identity.credential_epoch == 7
    # Reimport repairs an earlier stale local value, using authenticated /me.
    store.update(lambda state: setattr(state.identities[0], 'credential_epoch', 1))
    result = await import_identity(http, vault, store, key='test-key', alias='local', agent_hint='agent')
    assert result.identity.credential_epoch == store.load().identities[0].credential_epoch == 7
    epoch = 12
    result = await replace_credential(http, vault, store, alias='local', key='new-key')
    assert result.identity.credential_epoch == store.load().identities[0].credential_epoch == 12
    result = await replace_credential(http, vault, store, alias='local', key='new-key')
    assert result.identity.credential_epoch == 12
