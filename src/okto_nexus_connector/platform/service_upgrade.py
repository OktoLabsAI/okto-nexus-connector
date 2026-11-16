"""Upgrade effects scoped to the selected Connector state directory."""
from contextlib import contextmanager

from ..daemon.manager import _process_status, _lock
from ..service_upgrade import ServiceUpgrade, UpgradeHost, fail
from .service_context import (CONTEXT, INSTALLATION, CONTROL, LAUNCH, read_document,
    write_document, digest, validate_context, launch_exclusion)
from .user_service import UserService, windows_logon_session


class ConnectorUpgradeHost(UpgradeHost):
    component = 'connector'
    read, write, digest = staticmethod(read_document), staticmethod(write_document), staticmethod(digest)
    logon_session = staticmethod(windows_logon_session)

    def __init__(self, root, *, installation=None):
        self.root = root.resolve()
        self.installation = installation or UserService(self.root)
        self.instance_id = self.installation.instance_id
        self.context_path = self.root / CONTEXT
        self.installation_path = self.root / INSTALLATION
        self.control_path = self.root / CONTROL
        self.launch_path = self.root / 'run' / LAUNCH
        self.run_path = self.root / 'service-run.json'
        self.completed_path = self.root / 'service-upgrade.json'
        self.shutdown_path = self.root / 'run' / 'shutdown.json'

    def validate(self, context, *, require_paths=True):
        return validate_context(self.root, context, require_paths=require_paths)

    def status(self):
        return _process_status(self.root).to_json()

    def runtime_exists(self):
        from . import paths
        return (paths.journal_path(self.root).exists() or paths.owned_slot_ledger_path(self.root).exists()
            or self.run_path.exists())

    def claim_snapshot(self, receipt):
        return None

    def check_claim(self, body, confirmed, *, committed=False):
        if body.get('source_claim') is not None:
            fail('UPGRADE_INSTANCE_MISMATCH', 'Connector upgrades do not own a Server database claim.')

    def publish_claim(self, body, confirmed):
        self.check_claim(body, confirmed)

    @contextmanager
    def exclusion(self):
        with launch_exclusion(self.root):
            if self.status()['state'] != 'stopped':
                fail('UPGRADE_OWNER_UNCONFIRMED', 'Drain the source owner before applying; no process was stopped.')
            owner = _lock(self.root)
            if not owner.acquire():
                fail('UPGRADE_OWNER_UNCONFIRMED', 'Another process owns this state directory.')
            try:
                yield
            finally:
                owner.release()


def for_instance(root):
    return ServiceUpgrade(ConnectorUpgradeHost(root))
