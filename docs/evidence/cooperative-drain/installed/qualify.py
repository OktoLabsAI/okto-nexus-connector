"""Installed-wheel qualification; source is copied, never placed on sys.path."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

root=Path(__file__).parent
repo=Path('D:/Projetos/Techridy/okto-nexus-connector')
snapshot=root/'snapshot'
core=Path('C:/Users/jpamb/AppData/Local/Temp/r3-cooperative-drain-09ucmgsr/core-v2-wheels/okto_nexus_connector_core-0.0.14-py3-none-any.whl')
requirements=Path('C:/Users/jpamb/AppData/Local/Temp/r3-cooperative-drain-09ucmgsr/third-party.txt')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
write=lambda name,data:(root/name).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
assert sha(core)=='1407014ea12f7e6f04ac70b9da8dbf8ebd9a57d8021bb5e74437bc22bd274f7a'
tracked=subprocess.check_output(['rtk','proxy','git','ls-files','-z'],cwd=repo).decode('utf-8').split('\0')
own=json.loads(Path('C:/Users/jpamb/AppData/Local/Temp/connector-svc07-zfwdu6r_/public/source-manifest.json').read_text(encoding='utf-8'))['owned_files']
files=sorted({p for p in tracked+list(own) if p and (p.startswith(('src/','tests/','docs/')) or
    p in {'pyproject.toml','README.md','LICENSE','MANIFEST.in','conftest.py','pytest.ini','setup.cfg','setup.py'})})
before={name:sha(repo/name) for name in files}
for name in files:
    target=snapshot/name
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(repo/name,target)
assert all(sha(snapshot/name)==value for name,value in before.items())
shutil.copy2(requirements,root/'third-party.txt')
write('inputs-before.json',dict(files=before,head=subprocess.check_output(['rtk','git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    core_wheel=dict(path=str(core),sha256=sha(core)),third_party_sha256=sha(requirements)))
home=root/'home'
home.mkdir()
env={k:v for k,v in os.environ.items() if k.upper() in {'SYSTEMROOT','WINDIR','PATH','PATHEXT','COMSPEC','TEMP','TMP'}}
env.update(HOME=str(home),USERPROFILE=str(home),APPDATA=str(home),LOCALAPPDATA=str(home),
    PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8',
    UV_CACHE_DIR='C:/Users/jpamb/AppData/Local/uv/cache')
def run(stage,command,cwd=root):
    with (root/f'{stage}.stdout.txt').open('w',encoding='utf-8') as out,(root/f'{stage}.stderr.txt').open('w',encoding='utf-8') as err:
        child=subprocess.Popen(['rtk','proxy',*map(str,command)],cwd=cwd,env=env,stdin=subprocess.DEVNULL,stdout=out,stderr=err)
        print(json.dumps(dict(stage=stage,pid=child.pid)),flush=True)
        code=child.wait()
    write(f'{stage}.invocation.json',dict(command=list(map(str,command)),cwd=str(cwd),exit_code=code,
        environment='Explicit OS-tool allowlist; all homes in this Temp; no inherited credential variables or PYTHONPATH'))
    print(json.dumps(dict(stage=stage,exit_code=code)),flush=True)
    if code:
        print((root/f'{stage}.stderr.txt').read_text(encoding='utf-8')[-3000:],flush=True)
        print((root/f'{stage}.stdout.txt').read_text(encoding='utf-8')[-5000:],flush=True)
        raise SystemExit(code)
python=root/'venv/Scripts/python.exe'
run('venv',['uv','--no-config','venv','--python','3.13',root/'venv'])
run('dependencies',['uv','--no-config','pip','install','--python',python,'--no-deps','-r',root/'third-party.txt',
    'setuptools==84.0.0','wheel==0.45.1',core])
run('pip-check-core',[python,'-I','-m','pip','check'])
run('build',[python,'-I','-m','build','--wheel','--no-isolation','--outdir',root/'wheels'],cwd=snapshot)
wheel=next((root/'wheels').glob('okto_nexus_connector-*.whl'))
run('install',['uv','--no-config','pip','install','--python',python,'--no-deps',wheel])
run('pip-check-final',[python,'-I','-m','pip','check'])
run('proof-before',[python,'-I',root/'package_proof.py',root,core,'before'])
run('tests',[python,'-I',root/'installed_tests.py',root],cwd=snapshot)
run('proof-after',[python,'-I',root/'package_proof.py',root,core,'after'])
after={name:sha(repo/name) for name in files}
copied={name:sha(snapshot/name) for name in files}
write('inputs-after.json',dict(files=after,snapshot=copied,unchanged=after==before and copied==before,
    core_wheel_unchanged=sha(core)=='1407014ea12f7e6f04ac70b9da8dbf8ebd9a57d8021bb5e74437bc22bd274f7a'))
assert after==before and copied==before
print(json.dumps(dict(result='PASS',wheel=str(wheel),wheel_sha256=sha(wheel),inputs=len(files))),flush=True)
