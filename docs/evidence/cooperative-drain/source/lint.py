from pathlib import Path
import json
import subprocess
import sys

root = Path(__file__).parent
repo = Path('D:/Projetos/Techridy/okto-nexus-connector')
changed = subprocess.check_output(['rtk','proxy','git','diff','--name-only'],cwd=repo,text=True).splitlines()
new = subprocess.check_output(['rtk','proxy','git','ls-files','--others','--exclude-standard'],cwd=repo,text=True).splitlines()
files=[p for p in changed+new if p.endswith('.py')]
baseline=root/'lint-baseline'
for name in changed:
    if not name.endswith('.py'):
        continue
    path=baseline/name
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(subprocess.check_output(['rtk','proxy','git','show',f'HEAD:{name}'],cwd=repo))
def run(cwd, selection, name):
    result=subprocess.run(['rtk','proxy',sys.executable,'-m','ruff','check','--output-format','json',*selection],
        cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (root/f'{name}.json').write_bytes(result.stdout)
    (root/f'{name}.stderr.txt').write_bytes(result.stderr)
    return json.loads(result.stdout)
label=sys.argv[1] if len(sys.argv)>1 else 'initial'
current=run(repo,files,'lint-current-'+label)
prior=run(baseline,[p for p in changed if p.endswith('.py')],'lint-baseline')
def keys(items,cwd):
    return {(str(Path(x['filename']).relative_to(cwd)),x['code'],x['message']) for x in items}
new_issues=sorted(keys(current,repo)-keys(prior,baseline))
report=dict(current_issues=len(current),baseline_issues=len(prior),new_issues=new_issues,
    all_new_files_pass=not any(Path(x['filename']).relative_to(repo).as_posix() in new for x in current))
(root/f'lint-comparison-{label}.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps(report,indent=2))
