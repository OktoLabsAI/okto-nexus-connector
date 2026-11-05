"""Per-instance Windows task ownership; callers hold host launch exclusion."""
from __future__ import annotations

import os
from pathlib import Path
import secrets

from nexus_connector_core.user_service_definition import UserServiceDefinition, windows_logon_task

from ..errors import ConnectorError
from .service_context import (CONTEXT, CONTROL, INSTALLATION, digest, read_document,
                              saved_context, validate_context, write_document)
from .service_identity import effective_principal, service_instance_id
from .windows_tasks import WindowsTasks

SUPERVISOR = 'windows-task-scheduler'
LEGACY_TASK_NAME = 'okto-nexus-connector'


def conflict(reason, action):
    return ConnectorError(reason, 'user_service', 'The selected service integration requires review.', action=action)


def windows_logon_session():
    import win32api
    import win32security
    token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32security.TOKEN_QUERY)
    try:
        authentication = win32security.GetTokenInformation(token, win32security.TokenStatistics)['AuthenticationId']
        session = win32security.LsaGetLogonSessionData(authentication)
        return f"{authentication}:{int(session['LogonTime'].timestamp() * 1000000)}"
    finally:
        token.Close()


class UserService:
    def __init__(self, root: Path, *, tasks=None):
        self.root = root.resolve()
        self.instance_id = service_instance_id(self.root)
        self.principal = effective_principal()
        self.name = f'OktoNexus-connector-{self.instance_id[4:]}'
        self.tasks = tasks if tasks is not None else WindowsTasks()

    def receipt(self):
        try:
            value = read_document(self.root / INSTALLATION)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as error:
            raise conflict('INSTALLATION_RECEIPT_UNAVAILABLE', 'Inspect the retained service installation receipt.') from error
        if (value.get('revision') != 1 or value.get('instance_id') != self.instance_id
                or value.get('principal') != self.principal or value.get('name') != self.name
                or value.get('supervisor') != SUPERVISOR
                or value.get('state') not in {'installing', 'installed', 'removing'}
                or not isinstance(value.get('token'), str) or len(value['token']) != 32):
            raise conflict('INSTALLATION_RECEIPT_INVALID', 'Reconcile the selected user and state directory; no task was changed.')
        return value

    def _platform(self):
        if os.name != 'nt':
            raise conflict('CAPABILITY_UNSUPPORTED', 'Use the existing foreground supervisor on this platform; this installer targets Windows user logon tasks.')

    def _legacy(self):
        if self.tasks.read(LEGACY_TASK_NAME) is not None:
            raise conflict('LEGACY_SERVICE_REVIEW_REQUIRED',
                'A legacy global Connector task exists. Review its state directory and migrate it explicitly before selecting another supervisor. It was not modified.')

    def observe(self):
        self._platform()
        receipt = self.receipt()
        task = self.tasks.read(self.name)
        if receipt is None:
            if task is not None:
                raise conflict('FOREIGN_SERVICE_DEFINITION', 'The task exists without this instance ownership receipt; inspect it in Task Scheduler.')
            self._legacy()
            return None, None
        if receipt['state'] != 'installed':
            raise conflict('INSTALLATION_PENDING', 'Repeat install/uninstall to reconcile the retained operation. Unconfirmed existing tasks require explicit review.')
        if task is None:
            raise conflict('SERVICE_DEFINITION_MISSING', 'Repeat uninstall to remove the retained receipt, then install explicitly.')
        if task.digest != receipt.get('native_digest'):
            raise conflict('SERVICE_DEFINITION_CHANGED', 'Compare the task with native_xml in its receipt; restore or reconcile user edits before retrying.')
        context = validate_context(self.root, read_document(self.root / CONTEXT))
        if digest(context) != receipt.get('context_digest'):
            raise conflict('SERVICE_CONTEXT_CHANGED', 'Restore the recorded context or uninstall before selecting a different environment.')
        return receipt, task

    def definition(self, context, token):
        executable = Path(context['executable'])
        if executable.name.lower() == 'python.exe':
            executable = executable.with_name('pythonw.exe')
        return UserServiceDefinition('connector', self.instance_id, self.principal.removeprefix('sid:'),
            str(executable), ('-I', '-m', 'okto_nexus_connector.daemon.service_process',
            '--state-dir', str(self.root), '--supervised', '--token', token), str(self.root))

    def prepare(self):
        self._platform()
        context = validate_context(self.root, saved_context(self.root))
        receipt, task = self.receipt(), self.tasks.read(self.name)
        if receipt is None:
            if task is not None:
                raise conflict('FOREIGN_SERVICE_DEFINITION', 'The task exists without a matching receipt and was preserved.')
            self._legacy()
        else:
            if digest(context) != receipt.get('context_digest'):
                raise conflict('SERVICE_CONTEXT_CHANGED', 'Reconcile the retained execution context before installing.')
            if receipt['state'] == 'installing' and (task is None or task.digest == receipt.get('native_digest')):
                return context
            self.observe()
        definition = self.definition(context, '0' * 32)
        if not Path(definition.executable).is_file():
            raise conflict('SERVICE_EXECUTABLE_MISSING', 'A complete recorded Python environment with pythonw.exe is required.')
        self.tasks.validate(self.name, windows_logon_task(definition))
        return context

    def install(self):
        context = self.prepare()
        receipt = self.receipt()
        if receipt and receipt['state'] == 'installed':
            return receipt
        if receipt and receipt.get('native_digest'):
            task = self.tasks.read(self.name)
            if task and task.digest == receipt['native_digest']:
                receipt = {**receipt, 'state': 'installed'}
                write_document(self.root / INSTALLATION, receipt)
                return receipt
        token = receipt['token'] if receipt else secrets.token_hex(16)
        definition = self.definition(context, token)
        receipt = {'revision': 1, 'instance_id': self.instance_id, 'principal': self.principal,
            'name': self.name, 'supervisor': SUPERVISOR, 'token': token, 'state': 'installing',
            'context_digest': digest(context), 'definition_digest': definition.revision_digest}
        write_document(self.root / CONTEXT, context)
        write_document(self.root / INSTALLATION, receipt)
        write_document(self.root / CONTROL, {'revision': 1, 'token': token, 'desired': 'running'})
        created = self.tasks.create(self.name, windows_logon_task(definition))
        receipt = {**receipt, 'native_digest': created.digest, 'native_xml': created.xml}
        write_document(self.root / INSTALLATION, receipt)
        current = self.tasks.read(self.name)
        if not current or current.digest != created.digest:
            raise conflict('INSTALLATION_UNCONFIRMED', 'Inspect the retained native response before retrying; no task was adopted or overwritten.')
        receipt = {**receipt, 'state': 'installed'}
        write_document(self.root / INSTALLATION, receipt)
        return receipt

    def control(self, receipt):
        value = read_document(self.root / CONTROL)
        if (value.get('token') != receipt['token'] or value.get('revision') != 1
                or value.get('desired') not in {'running', 'draining'}):
            raise conflict('SERVICE_CONTROL_INVALID', 'Reconcile the control receipt with the selected installation.')
        return value

    def start(self):
        receipt, task = self.observe()
        if receipt is None:
            return False
        control = self.control(receipt)
        if task.active or control.get('launch_requested'):
            return True
        if not task.enabled or task.state == 0:
            raise conflict('SUPERVISOR_UNCONFIRMED', 'Inspect the selected task state; it was not re-enabled or replaced.')
        write_document(self.root / CONTROL, {'revision': 1, 'token': receipt['token'],
            'desired': 'running', 'launch_requested': True})
        self.tasks.start(self.name, receipt['native_digest'])
        return True

    def begin_stop(self, generation):
        receipt, _ = self.observe()
        if receipt:
            control = self.control(receipt)
            write_document(self.root / CONTROL, {'revision': 1, 'token': receipt['token'],
                'desired': 'draining', 'generation': generation,
                'logon_session': control.get('logon_session') or windows_logon_session()})

    def uninstall(self):
        self._platform()
        receipt, task = self.receipt(), self.tasks.read(self.name)
        if receipt is None:
            if task:
                raise conflict('FOREIGN_SERVICE_DEFINITION', 'The unowned task was preserved.')
            return
        if task:
            if task.digest != receipt.get('native_digest'):
                raise conflict('SERVICE_DEFINITION_CHANGED', 'Reconcile the selected task before removing it; the retained native_xml is available for comparison.')
            if task.active or task.state == 0:
                raise conflict('SUPERVISOR_STILL_ACTIVE', 'Wait for the application drain and task exit; no forced stop was requested.')
            write_document(self.root / INSTALLATION, {**receipt, 'state': 'removing'})
            self.tasks.delete(self.name, receipt['native_digest'])
            if self.tasks.read(self.name):
                raise conflict('UNINSTALL_UNCONFIRMED', 'Repeat uninstall to observe this same removal.')
        (self.root / INSTALLATION).unlink()
        (self.root / CONTROL).unlink(missing_ok=True)
