import ctypes
import os
from types import SimpleNamespace

import pytest

from okto_nexus_connector.platform import sysinfo


def test_current_process_has_stable_native_identity():
    identity = sysinfo.current_process_identity()
    assert identity.pid == os.getpid()
    assert identity.birth_token != 'unknown'
    assert sysinfo.same_process(identity)
    assert not sysinfo.same_process(sysinfo.ProcessIdentity(identity.pid, 'different-birth'))
    assert sysinfo.process_identity(-1) is None


@pytest.mark.parametrize('fault', [None, 'short', 'wrong_pid', 'zero_start', 'bad_usec'])
def test_darwin_birth_validates_native_response(monkeypatch, fault):
    def query(pid, flavor, arg, pointer, size):
        assert (pid, flavor, arg) == (42, 3, 0)
        info = pointer._obj
        assert size == ctypes.sizeof(info) == 136
        assert type(info).start_sec.offset == 120
        assert type(info).start_usec.offset == 128
        info.pid = 43 if fault == 'wrong_pid' else pid
        info.start_sec = 0 if fault == 'zero_start' else 123456
        info.start_usec = 1000000 if fault == 'bad_usec' else 7
        return size - 1 if fault == 'short' else size
    def library(path, *, use_errno):
        assert path == '/usr/lib/libproc.dylib' and use_errno
        return SimpleNamespace(proc_pidinfo=query)
    monkeypatch.setattr(sysinfo.ctypes, 'CDLL', library)
    monkeypatch.setattr(sysinfo.sys, 'platform', 'darwin')
    assert sysinfo._birth_of(42) == ('start:123456.000007' if fault is None else None)


def test_darwin_unavailable_library_fails_closed(monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError('unavailable')
    monkeypatch.setattr(sysinfo.ctypes, 'CDLL', unavailable)
    monkeypatch.setattr(sysinfo.sys, 'platform', 'darwin')
    assert sysinfo.process_identity(42) is None
