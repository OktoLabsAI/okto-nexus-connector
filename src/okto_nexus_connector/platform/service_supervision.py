"""Identity-bound receipt for the one bounded user-service supervisor."""
from __future__ import annotations

import ctypes
from ctypes import wintypes
import os

from nexus_connector_core.user_service_supervision import RetryState

from ..daemon.lock import InstanceLock
from .service_context import read_document, write_document

MODE = 'bounded-parent-v1'
RECEIPT = 'service-supervisor.json'
GUARD = 'service-supervisor.guard'
TERMINAL = {'stopped', 'exhausted'}


def guard(root):
    return InstanceLock(root / 'run', lock_name=GUARD)


def _kernel():
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(wintypes.FILETIME)] * 4)]
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    return kernel


def _birth(kernel, handle):
    fields = [wintypes.FILETIME() for _ in range(4)]
    if not kernel.GetProcessTimes(handle, *[ctypes.byref(field) for field in fields]):
        raise OSError('Process birth unavailable')
    return 'filetime:' + str(fields[0].dwLowDateTime | fields[0].dwHighDateTime << 32)


def valid_identity(value):
    return (isinstance(value, dict) and type(value.get('pid')) is int and value['pid'] > 0
        and isinstance(value.get('birth_token'), str) and value['birth_token'].startswith('filetime:')
        and value['birth_token'][9:].isdigit() and int(value['birth_token'][9:]) > 0)


def process_state(identity):
    """Positive same-birth liveness/exit; denied access remains unknown."""
    if not valid_identity(identity) or os.name != 'nt':
        return 'unknown'
    kernel = _kernel()
    handle = kernel.OpenProcess(0x1000 | 0x100000, False, identity['pid'])
    if not handle:
        return 'exited' if ctypes.get_last_error() == 87 else 'unknown'
    try:
        if _birth(kernel, handle) != identity['birth_token']:
            return 'exited'  # PID reused; the recorded process is gone.
        state = kernel.WaitForSingleObject(handle, 0)
        return {0: 'exited', 258: 'alive'}.get(state, 'unknown')
    except OSError:
        return 'unknown'
    finally:
        kernel.CloseHandle(handle)


def child_identity(process):
    """Popen retains the actual child handle even when startup exits quickly."""
    if os.name != 'nt' or not hasattr(process, '_handle'):
        raise OSError('Child identity unavailable')
    return {'pid': process.pid, 'birth_token': _birth(_kernel(), int(process._handle))}


class ProcessHandle:
    """Retain query/synchronize rights to the actual runtime child, never kill."""
    def __init__(self, identity):
        if not valid_identity(identity):
            raise ValueError('Invalid runtime identity')
        self.kernel = _kernel()
        self.handle = self.kernel.OpenProcess(0x1000 | 0x100000, False, identity['pid'])
        if not self.handle:
            raise OSError('Runtime handle unavailable')
        self.identity = dict(identity)
        try:
            if _birth(self.kernel, self.handle) != identity['birth_token'] or self.exited():
                raise OSError('Runtime identity changed')
        except BaseException:
            self.close()
            raise

    def exited(self):
        result = self.kernel.WaitForSingleObject(self.handle, 0)
        if result not in (0, 258):
            raise OSError('Runtime exit unconfirmed')
        return result == 0

    def wait(self):
        if self.kernel.WaitForSingleObject(self.handle, 0xFFFFFFFF) != 0:
            raise OSError('Runtime physical exit unconfirmed')
        self.kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        self.kernel.GetExitCodeProcess.restype = wintypes.BOOL
        code = wintypes.DWORD()
        if not self.kernel.GetExitCodeProcess(self.handle, ctypes.byref(code)):
            raise OSError('Runtime exit code unavailable')
        return int(code.value)

    def parent_pid(self):
        class Entry(ctypes.Structure):
            _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD),
                ('th32ProcessID', wintypes.DWORD), ('th32DefaultHeapID', ctypes.c_size_t),
                ('th32ModuleID', wintypes.DWORD), ('cntThreads', wintypes.DWORD),
                ('th32ParentProcessID', wintypes.DWORD), ('pcPriClassBase', wintypes.LONG),
                ('dwFlags', wintypes.DWORD), ('szExeFile', wintypes.WCHAR * 260)]
        kernel = self.kernel
        kernel.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
        kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(Entry)]
        kernel.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(Entry)]
        snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
        if snapshot in (None, ctypes.c_void_p(-1).value):
            raise OSError('Process ancestry unavailable')
        try:
            entry = Entry()
            entry.dwSize = ctypes.sizeof(entry)
            found = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
            while found:
                if entry.th32ProcessID == self.identity['pid']:
                    if self.exited():
                        raise OSError('Runtime exited during ancestry check')
                    return int(entry.th32ParentProcessID)
                found = kernel.Process32NextW(snapshot, ctypes.byref(entry))
            raise OSError('Runtime ancestry unconfirmed')
        finally:
            kernel.CloseHandle(snapshot)

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def handshake_path(root, cycle):
    return root / 'run' / f"service-supervisor-handshake-{cycle['cycle_id']}-{cycle['attempts_started']}.json"


def read_cycle(root, installation):
    try:
        value = read_document(root / 'run' / RECEIPT)
    except FileNotFoundError:
        return None
    if (value.get('revision') != 1
            or value.get('instance_id') != installation['instance_id']
            or not isinstance(value.get('token'), str) or len(value['token']) != 32
            or not isinstance(value.get('cycle_id'), str) or len(value['cycle_id']) != 32
            or any(char not in '0123456789abcdef' for char in value['cycle_id'])
            or not valid_identity(value.get('parent'))
            or type(value.get('attempts_started')) is not int or not 0 <= value['attempts_started'] <= 4
            or value.get('phase') not in {'initial', 'running', 'backoff', 'exhausted', 'stopped', 'unknown'}
            or type(value.get('child_exit_confirmed')) is not bool
            or type(value.get('launcher_exit_confirmed')) is not bool
            or type(value.get('child_acknowledged')) is not bool
            or value.get('launcher') is not None and not valid_identity(value['launcher'])
            or value.get('child') is not None and not valid_identity(value['child'])
            or value.get('attempts_started', 0) > 0 and value.get('child') is None and value.get('child_exit_confirmed')
            or value.get('phase') in TERMINAL and not value.get('child_exit_confirmed')
            or value.get('phase') in TERMINAL and not value.get('launcher_exit_confirmed')
            or value.get('attempts_started', 0) > 0 and value.get('phase') in TERMINAL and not value.get('child_acknowledged')
            or value.get('phase') == 'exhausted' and value.get('attempts_started') != 4):
        raise ValueError('Invalid supervisor receipt')
    RetryState(**{key: value.get(key) for key in ('attempts_started', 'phase', 'next_retry_monotonic', 'last_exit_code')})
    if value['attempts_started'] and (not isinstance(value.get('attempt_nonce'), str)
            or len(value['attempt_nonce']) != 32 or any(char not in '0123456789abcdef' for char in value['attempt_nonce'])):
        raise ValueError('Invalid supervisor attempt nonce')
    if value.get('token') != installation['token']:
        # Explicit uninstall/install or a completed upgrade may rotate the
        # installation token. Retain old evidence, and accept no ambiguous child.
        if (value['phase'] not in TERMINAL or not value['child_exit_confirmed']
                or process_state(value['parent']) != 'exited'
                or value.get('launcher') and process_state(value['launcher']) != 'exited'
                or value.get('child') and process_state(value['child']) != 'exited'):
            raise ValueError('Prior installation supervisor is unconfirmed')
        return None
    return value


def observation(root, installation, task, control, *, probe=None):
    probe = probe or process_state
    cycle = read_cycle(root, installation)
    if cycle is None:
        return None
    parent = probe(cycle['parent'])
    launcher = probe(cycle['launcher']) if cycle['launcher'] else ('exited' if cycle['launcher_exit_confirmed'] else 'unknown')
    child = probe(cycle['child']) if cycle['child'] else ('exited' if cycle['child_exit_confirmed'] else 'unknown')
    phase = cycle['phase']
    coherent = control.get('cycle_id') == cycle['cycle_id']
    if (not coherent and control.get('launch_requested') is True and phase in TERMINAL
            and cycle['child_exit_confirmed'] and parent == launcher == child == 'exited'):
        state = 'initial'
    elif coherent and parent == launcher == 'alive' and cycle['phase'] == 'running' and not cycle['child_acknowledged']:
        state = 'handshaking'
    elif not coherent or parent == 'unknown' or launcher == 'unknown' or child == 'unknown':
        state = 'unknown'
    elif phase in TERMINAL and cycle['child_exit_confirmed'] and launcher == child == 'exited':
        if parent == 'exited' and not task.active and task.state not in {0, 2, 4}:
            state = 'stopped' if control['desired'] == 'draining' else phase
        else:
            state = 'exiting'
    elif parent != 'alive' or guard(root).locked() is not True or phase == 'unknown':
        state = 'unknown'
    else:
        state = phase
    return {'state': state, 'phase': phase, 'cycle_id': cycle['cycle_id'],
        'attempts_started': cycle['attempts_started'], 'max_attempts': 4,
        'next_retry_monotonic': cycle.get('next_retry_monotonic'), 'last_exit_code': cycle.get('last_exit_code'),
        'parent_state': parent, 'launcher_state': launcher, 'child_state': child,
        'child_acknowledged': cycle['child_acknowledged'], 'child_exit_confirmed': cycle['child_exit_confirmed']}


def validate_child(root, installation, control, cycle_id, nonce):
    from .sysinfo import current_process_identity
    from ..errors import ConnectorError
    cycle = read_cycle(root, installation)
    identity = current_process_identity().to_json()
    if (not cycle or cycle['cycle_id'] != cycle_id or control.get('cycle_id') != cycle_id
            or cycle['phase'] != 'running' or cycle['child_exit_confirmed']
            or cycle.get('attempt_nonce') != nonce or not isinstance(nonce, str) or len(nonce) != 32
            or not valid_identity(identity) or not valid_identity(cycle.get('launcher'))
            or process_state(cycle['parent']) != 'alive' or guard(root).locked() is not True):
        raise ValueError('Supervisor child authority is unconfirmed')
    # The parent independently verifies this declaration through the OS and a
    # retained exact runtime handle before acknowledging it. It is not authority.
    declaration = {'revision': 1, 'instance_id': installation['instance_id'], 'cycle_id': cycle_id,
        'attempts_started': cycle['attempts_started'], 'attempt_nonce': nonce,
        **identity, 'parent_pid': os.getppid()}
    path = handshake_path(root, cycle)
    try:
        existing = read_document(path)
    except FileNotFoundError:
        write_document(path, declaration)
    else:
        if existing != declaration:
            raise ValueError('Another runtime claimed this handshake')
    if not cycle['child_acknowledged']:
        raise ConnectorError('LIFECYCLE_BUSY', 'supervision', 'Waiting for the parent identity acknowledgement.')
    if cycle['child'] != identity:
        raise ValueError('Parent acknowledged another runtime')


def require_no_native_retry(task):
    from xml.etree import ElementTree as ET
    if any(element.tag.rsplit('}', 1)[-1] == 'RestartOnFailure' for element in ET.fromstring(task.xml).iter()):
        from .user_service import conflict
        raise conflict('SERVICE_SUPERVISION_POLICY_CHANGED',
            'Stop, uninstall and install this user service explicitly to select bounded foreground supervision. '
            'The previous task and retained data were not changed.')
