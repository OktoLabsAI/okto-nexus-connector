from pathlib import Path
import hashlib
import json
import shutil

repo=Path(r'D:/Projetos/Techridy/okto-nexus-connector')
root=Path(__file__).parent
dest=repo/'docs/evidence/cooperative-drain'
dest.mkdir(parents=True,exist_ok=False)
sources={
 'source':Path(r'C:/Users/jpamb/AppData/Local/Temp/connector-svc07-zfwdu6r_/public'),
 'installed':Path(r'C:/Users/jpamb/AppData/Local/Temp/connector-svc07-installed-_r091tbk/public')}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
for name,folder in sources.items():
    manifest=json.loads((folder/'manifest.json').read_text())
    for path,item in manifest.items():assert sha(folder/path)==item['sha256'],path
    shutil.copytree(folder,dest/name)
source=json.loads((sources['source']/'source-manifest.json').read_text())
for name,item in source['owned_files'].items():assert sha(repo/name)==item['sha256'],name
assert sha(repo/'vendor/wheels/okto_nexus_connector_core-0.0.14-py3-none-any.whl')=='1407014ea12f7e6f04ac70b9da8dbf8ebd9a57d8021bb5e74437bc22bd274f7a'
shutil.copyfile(__file__,dest/'curate_connector.py')
(dest/'milestone.md').write_text('''# SVC-07 — Connector cooperative shutdown

An operator stop now fences new productive admission and waits while existing
decisions, leases, results and acknowledgements continue. Timeout does not force
shutdown. Force requires an expired existing drain and the reviewed generation;
the CLI preserves explicit owner refusals. Direct authenticated IPC also persists
the selected supervisor stop intent before effects.

The host uses Core's public shared-journal drain reports and revisions. Historical
unobserved state stays UNKNOWN. A blocked observation cannot prevent an explicit
force decision from starting containment, but its owned reader must join before
stores/IPC/locks close. Cleanup owners remain tracked across unknown retries.

26 new cases are included in **194 passed /1 conditional non-Windows skip** in
both source and installed qualification; these totals are not additive. The
new wheel's124 Connector payload files match frozen source and installed bytes;
166 Core files match wheel1407014e… from Core commit3cdba87. All332 input hashes
were unchanged,173 imported modules resolved to site-packages, and pip check
passed. Source Ruff findings match15 preexisting items, with no new findings.
Earlier fixture and product-diagnostic attempts are retained in source/.

No wire-format change or new canonical authority. The technical run/drain.json
marker is additive; the existing supervisor CONTROL remains authoritative for
stop suppression. Runtime/journal data is retained. Rollback requires confirmed
drain and reverting this feature candidate with its prior Core dependency;
never remove an uncertain owner's lock or receipts to force rollback.

Tests use isolated homes, SQLite and synthetic native/HTTP/WSS peers. No provider,
personal vault, native service registration or actual login/logout was exercised.
Nexus integration and real M1/native service campaigns remain separate gates.
Commands, original outcomes, package/import proof and source hashes are retained
under their exact-byte manifests.
''',encoding='utf-8')
items=[{'path':p.relative_to(dest).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p)}
       for p in sorted(dest.rglob('*')) if p.is_file()]
(dest/'artifact-manifest.json').write_text(json.dumps({'artifacts':items},indent=2)+'\n')
print(json.dumps({'artifacts':len(items)+1,'owned_source_files':len(source['owned_files'])}))
