"""Finite public evidence allowlist; never reads fixture homes or databases."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET

root=Path(__file__).parent
public=root/'public'
public.mkdir(exist_ok=True)
repo=Path('D:/Projetos/Techridy/okto-nexus-connector')
sources=[
    'src/okto_nexus_connector/cli/commands/daemon.py','src/okto_nexus_connector/cli/main.py',
    'src/okto_nexus_connector/daemon/app.py','src/okto_nexus_connector/daemon/drain.py',
    'src/okto_nexus_connector/daemon/lifecycle.py','src/okto_nexus_connector/daemon/manager.py',
    'src/okto_nexus_connector/daemon/r4_control.py','src/okto_nexus_connector/daemon/r4_execution.py',
    'src/okto_nexus_connector/services/core_host.py','src/okto_nexus_connector/services/r4_events.py',
    'src/okto_nexus_connector/services/r4_execution.py','src/okto_nexus_connector/transport/r4_connection.py',
    'tests/unit/test_operator_drain.py','tests/unit/test_r4_operator_drain.py']
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
write=lambda name,value:(public/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
stages=['initial-policy','policy-fixed','policy-additions','initial-core-r4','owner-latch','final-v2','fixes-v3','final-v3']
reports={}
for stage in stages:
    rows=list(ET.parse(root/f'{stage}.xml').iter('testcase'))
    reports[stage]=dict(tests=len(rows),passed=sum(all(row.find(k) is None for k in ('failure','error','skipped')) for row in rows),
        failed=sum(row.find('failure') is not None for row in rows),errors=sum(row.find('error') is not None for row in rows),
        skipped=[dict(name=row.get('name'),reason=row.find('skipped').get('message')) for row in rows if row.find('skipped') is not None])
    for suffix in ('xml','stdout.txt','stderr.txt','invocation.json'):
        shutil.copy2(root/f'{stage}.{suffix}',public/f'{stage}.{suffix}')
for name in ('run.py','lint.py','curate.py','lint-current-initial.json','lint-comparison.json',
             'lint-baseline.json','lint-current-freeze-v3.json','lint-comparison-freeze-v3.json'):
    shutil.copy2(root/name,public/name)
write('source-manifest.json',dict(base_head=subprocess.check_output(['rtk','git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    owned_files={name:dict(sha256=sha(repo/name),bytes=(repo/name).stat().st_size) for name in sources},
    excluded_other_owner_files=['docs/service-lifecycle.md'],
    core_dependency='Current source API: begin_drain, drain_status, drain_revision, combine_drain_status, SQLiteJournal.drain_snapshot; no new Core wheel installed by this agent.'))
write('reports.json',dict(scope='Connector SVC-07 source qualification, Windows/local synthetic',reports=reports,
    authoritative_final='final-v3.xml',new_test_cases=26,overlapping_campaigns_not_added=True,
    diagnostics={'initial-policy':'5 fixture failures: synthetic app lacked run/; fixture fixed without changing production.',
                 'final-v2':'1 fixture failure: omitted SUBMISSION_STARTED transition before SUBMITTED; fixed through public journal API.',
                 'lint-initial':'2 new fixture-reexport F811 errors fixed; 15 preexisting issues unchanged.'},
    reviewed_fixes=['Direct IPC latches supervisor before admission fence with owner generation proof and launch exclusion.',
        'Force reaches containment independently of a blocked owned status reader; stores/IPC remain until reader and admitted requests join.',
        'Unknown R4 controls remain owned across retries.',
        'Shared journal sibling claims aggregate only through public Core proofs/revisions; orphan or unopened history remains UNKNOWN.'],
    validation=['194 PASS, 1 existing non-Windows-only SKIP, 0 failures/errors in final source regression.',
        'Ruff: 15 current issues equal 15 baseline issues; zero new issues, all new files pass.'],
    limitations=['No personal provider, credential vault, native service installation or productive M1/M2 campaign.',
        'Native execution peers and supervisor adapter are synthetic; real Core/SQLite and existing fake HTTP/WSS/IPC integrations are exercised.',
        'Current source tested with isolated installed dependencies; final wheel/installed-product qualification belongs to root.',
        'No ledger, general documentation, existing evidence, commit or push changes by this agent.']))
(public/'README.md').write_text('''# Connector SVC-07

The operator stop path closes new admissions and preserves control, decisions, already admitted producers and publication. Its first deadline is immutable. Expiry alone never calls Core shutdown; force requires the same generation after expiry. Signals retain their separate safety path.

Direct IPC uses the same durable supervisor latch as CLI. Quiescence combines public Core proofs with host composition, operation reservations, receipt/event publication and native-tool obligations. A blocked status reader stays owned while explicit force begins containment; shared stores and IPC close only after retained readers/producers join. Unknown outcomes retain the same owner and unresolved control records.

The final source regression is `final-v3.xml`: 194 passed, one existing non-Windows-only skip, no failures/errors. It includes 26 new test cases. Earlier runs overlap and must not be summed. Initial fixture/lint failures remain in this allowlist. `source-manifest.json` pins the 14 owned files; other owners' documentation is excluded. Ruff adds no issues over the 15 baseline findings.

This qualifies Windows/local synthetic behavior. It does not qualify a personal provider, credential vault, native service install, productive M1/M2, or the future installed wheel. Root owns package qualification, ledger and commits. No fixture home, database, credential file or environment dump is included.
''',encoding='utf-8')
files=sorted(p.name for p in public.iterdir() if p.is_file() and p.name!='manifest.json')
write('manifest.json',{name:dict(sha256=sha(public/name),bytes=(public/name).stat().st_size) for name in files})
print(json.dumps(dict(public=str(public),files=len(files)+1,manifest_sha256=sha(public/'manifest.json'),
    final=reports['final-v3'],source_manifest_sha256=sha(public/'source-manifest.json')),indent=2))
