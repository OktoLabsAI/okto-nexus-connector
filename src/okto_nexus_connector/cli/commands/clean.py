"""Explicit reset of Connector-owned local state, never provider homes."""
import asyncio
import json
from pathlib import Path
import shutil

from ...daemon import manager
from ...daemon.lock import InstanceLock
from ...errors import ConnectorError
from ...identity.vault import open_vault, namespace_of, KeyringVault
from ...platform import service_install
from ..prompts import confirm


OWNED = ('state.json', 'state.json.lock', 'state.lock', 'vault', 'runtime', 'logs',
         'executor-journal.db', 'executor-journal.db-wal', 'executor-journal.db-shm',
         'owned-slots.db', 'owned-slots.db-wal', 'owned-slots.db-shm', '.user-private')


def targets(root):
    root = Path(root).absolute()
    if root.is_symlink() or root.resolve() in (Path(root.anchor), Path.home().resolve()):
        raise ConnectorError('VALIDATION_ERROR', 'clean', 'Select a dedicated Connector state directory.')
    resolved = root.resolve()
    selected = [root / name for name in OWNED if (root / name).exists() or (root / name).is_symlink()]
    # Reject redirects before any recursive removal, including Windows junctions.
    for path in selected + [root / 'run']:
        for item in [path, *(path.rglob('*') if path.is_dir() and not path.is_symlink() else ())]:
            if item.is_symlink() or getattr(item, 'is_junction', lambda:False)() or not item.resolve().is_relative_to(resolved):
                raise ConnectorError('VALIDATION_ERROR', 'clean', 'State contains a redirected path; cleanup refused.')
    return selected


def handles(value):
    if isinstance(value, dict):
        return set().union(*(handles(v) for v in value.values())) if value else set()
    if isinstance(value, list):
        return set().union(*(handles(v) for v in value)) if value else set()
    return {value} if isinstance(value,str) and value.startswith('vault:') else set()


def cleanup(root):
    selected = targets(root)
    # Disable autostart before draining, otherwise the service can respawn.
    if service_install.status(root)['installed']:
        service_install.uninstall(root, dry_run=False)
        if service_install.status(root)['installed']:
            raise ConnectorError('OPERATION_CONFLICT','clean','Could not disable the Connector autostart service.')
    stopped = manager.stop(root, timeout=30)
    if stopped.get('was_running') and not stopped.get('stopped'):
        raise ConnectorError('OPERATION_CONFLICT','clean','Daemon shutdown is incomplete; state was preserved.')
    lock = InstanceLock(root / 'run')
    if not lock.acquire():
        raise ConnectorError('OPERATION_CONFLICT','clean','The Connector is still running; state was preserved.')
    try:
        raw = json.loads((root / 'state.json').read_text(encoding='utf-8')) if (root / 'state.json').exists() else {}
        refs = handles(raw)
        if refs:
            vault = open_vault(root / 'vault', approved_fallback=True)
            for ref in refs:
                namespace = namespace_of(ref)
                if isinstance(vault, KeyringVault):
                    # KeyringVault.remove intentionally ignores backend errors
                    # for ordinary identity removal. A full reset must not.
                    try:
                        if vault._keyring.get_password(vault.SERVICE, namespace) is not None:
                            vault._keyring.delete_password(vault.SERVICE, namespace)
                        remaining = vault._keyring.get_password(vault.SERVICE, namespace)
                    except Exception:
                        raise ConnectorError('AGENT_AUTH_REQUIRED','clean','Unlock the OS keyring before cleanup; state was preserved.') from None
                    if remaining is not None:
                        raise ConnectorError('OPERATION_CONFLICT','clean','Could not remove a keyring credential; state was preserved.')
                    continue
                vault.remove(namespace)
                try:
                    vault.resolve(ref)
                except ConnectorError as error:
                    if error.code != 'AGENT_AUTH_REQUIRED': raise
                else:
                    raise ConnectorError('OPERATION_CONFLICT','clean','A stored credential could not be removed; state was preserved.')
        selected = targets(root)
        for path in selected:
            if path.is_dir(): shutil.rmtree(path)
            else: path.unlink(missing_ok=True)
        # Keep the lock inode so simultaneous starts cannot bypass this lock.
        for path in (root / 'run').iterdir():
            if path.name != 'daemon.lock':
                if path.is_dir(): shutil.rmtree(path)
                else: path.unlink(missing_ok=True)
    finally:
        lock.release()
    return {'cleaned':True, 'state_dir':str(root), 'credentials_removed':len(refs),
            'server_records_removed':False}


async def run_clean(args, output, root):
    targets(root)
    output.line(f'Clean Connector state: {root}')
    output.line('Stops the daemon and removes autostart, identities, stored keys, bindings, pending requests, settings, journals and logs. Projects and harness logins are preserved. Nexus records remain; reconnecting creates a new machine identity.')
    if not args.yes:
        if args.non_interactive or args.json:
            raise ConnectorError('APPROVAL_REQUIRED','clean','Cleanup requires confirmation. Pass clean --yes explicitly.')
        if not confirm('Permanently reset this local Connector environment?', non_interactive=False, default=False):
            return {'cleaned':False,'aborted':True}
    try:
        result = await asyncio.to_thread(cleanup, root)
    except (OSError, ValueError):
        raise ConnectorError('OPERATION_CONFLICT','clean','Cleanup could not finish. Check filesystem access and state integrity before retrying.') from None
    output.line('Local Connector environment reset. Run configure to start again.')
    return result
