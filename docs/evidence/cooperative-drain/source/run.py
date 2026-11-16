import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).parent
repo = Path('D:/Projetos/Techridy/okto-nexus-connector')
python = Path('C:/Users/jpamb/AppData/Local/Temp/nexus-ci-connector-refresh-la1tb1l6/venv/Scripts/python.exe')
name = sys.argv[1]
core_source = '--core-source' in sys.argv
tests = [arg for arg in sys.argv[2:] if arg != '--core-source']
env = {k:v for k,v in os.environ.items() if k.upper() in {'SYSTEMROOT','WINDIR','PATH','PATHEXT','COMSPEC','TEMP','TMP'}}
env.update(HOME=str(root/'home'),USERPROFILE=str(root/'home'),APPDATA=str(root/'home'),LOCALAPPDATA=str(root/'home'),
    PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8')
sources=[str(repo/'src')]
if core_source:
    sources.append('D:/Projetos/Techridy/okto-nexus-connector-core/src')
env['PYTHONPATH']=os.pathsep.join(sources)
command=['rtk','proxy',str(python),'-m','pytest','-q','-o',f'cache_dir={root/name}-cache',
    f'--basetemp={root/name}-fixtures',f'--junitxml={root/name}.xml',*tests]
with (root/f'{name}.stdout.txt').open('w',encoding='utf-8') as stdout, (root/f'{name}.stderr.txt').open('w',encoding='utf-8') as stderr:
    result=subprocess.run(command,cwd=repo,env=env,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr)
(root/f'{name}.invocation.json').write_text(json.dumps({'command':command,'exit_code':result.returncode,
    'core_source':core_source,'scope':'Synthetic tests; no provider/personal service or vault'},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'name':name,'exit_code':result.returncode,'report':str(root/f'{name}.xml')}))
print((root/f'{name}.stdout.txt').read_text(encoding='utf-8')[-12000:])
print((root/f'{name}.stderr.txt').read_text(encoding='utf-8')[-1000:])
