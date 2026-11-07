"""Source campaign guard: doctor metadata dependencies are synthetic only.

This does not replace tests' own explicit fakes. It prevents legacy doctor tests
from consulting the host's actual keyring or containment platform. No product
module imports happen until pytest establishes its isolated test fixtures.
"""
from types import SimpleNamespace
import pytest


@pytest.fixture(autouse=True)
def synthetic_doctor_metadata(monkeypatch):
    from okto_nexus_connector.cli.commands import doctor
    from okto_nexus_connector.services import discovery_service
    def forbidden_resolve(*args, **kwargs):
        pytest.fail('Unstubbed credential resolution in diagnostic campaign')
    monkeypatch.setattr(doctor, 'vault_backend_names', lambda: ['synthetic-keyring'])
    monkeypatch.setattr(doctor, 'open_vault', lambda *a, **k: SimpleNamespace(
        backend_name=lambda: 'synthetic-keyring', resolve=forbidden_resolve,
        list_namespaces=lambda: []))
    monkeypatch.setattr(discovery_service, 'containment_status', lambda: {'synthetic': 'ok'})
