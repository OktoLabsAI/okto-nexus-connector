"""Restricted Windows supervisors must not produce false detached readiness."""
import json
import os
import subprocess
import sys
import uuid

import pytest

from okto_nexus_connector.daemon import manager
from okto_nexus_connector.daemon.lock import InstanceLock, wait_for_readiness
from okto_nexus_connector.platform import paths


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job Object policy")
@pytest.mark.parametrize("mode", ["detached", "foreground"])
def test_daemon_under_restrictive_job(tmp_path, mode):
    import win32api
    import win32job

    root = paths.state_dir(tmp_path)
    env = dict(os.environ, OKTO_NEXUS_CONNECTOR_STATE=str(root),
               OKTO_NEXUS_CONNECTOR_VAULT="file")
    job_name = "okto-daemon-test-" + uuid.uuid4().hex
    job = win32job.CreateJobObject(None, job_name)
    limits = win32job.QueryInformationJobObject(
        job, win32job.JobObjectExtendedLimitInformation)
    limits["BasicLimitInformation"]["LimitFlags"] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, limits)
    script = '''import json, pathlib, sys
sys.stdin.readline()
import win32api, win32job
job = win32job.OpenJobObject(win32job.JOB_OBJECT_ALL_ACCESS, False, sys.argv[2])
win32job.AssignProcessToJobObject(job, win32api.GetCurrentProcess())
from okto_nexus_connector.daemon import manager
from okto_nexus_connector.errors import ConnectorError
if sys.argv[3] == 'foreground':
    import asyncio
    raise SystemExit(asyncio.run(manager.run_foreground(pathlib.Path(sys.argv[1]))))
try:
    manager.start(pathlib.Path(sys.argv[1]))
except ConnectorError as exc:
    print(json.dumps(dict(code=exc.code, message=str(exc), action=exc.action)), flush=True)
else:
    raise AssertionError('restricted job must not claim detached startup')
'''
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", script, str(root), job_name, mode], cwd=tmp_path,
        env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        # The handshake prevents daemon creation before the restrictive job
        # owns the starter. No breakaway permission is granted to this job.
        if mode == "foreground":
            process.stdin.write("start\n")
            process.stdin.flush()
            wait_for_readiness(InstanceLock(paths.pid_dir(root)), timeout=30)
            assert manager.status(root).running
            assert manager.stop(root, timeout=30)["stopped"] is True
            stdout, stderr = process.communicate(timeout=10)
        else:
            stdout, stderr = process.communicate("start\n", timeout=60)
        assert process.returncode == 0, stderr
        if mode == "detached":
            error = json.loads(stdout)
            assert error["code"] == "DAEMON_UNAVAILABLE"
            assert "daemon run" in error["action"]
        assert not manager.status(root).running
    finally:
        win32api.CloseHandle(job)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
