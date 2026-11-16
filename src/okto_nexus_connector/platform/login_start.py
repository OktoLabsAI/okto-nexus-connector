"""Secret-free review of the existing host-local user-service operations."""
from nexus_connector_core.user_service_definition import windows_definition_digest, windows_with_enabled

from .service_context import digest, saved_context
from .user_service import SUPERVISOR, UserService, conflict


def scope(service):
    context = saved_context(service.root)
    return dict(component='connector', instance_id=service.instance_id, principal=service.principal,
        supervisor=SUPERVISOR, root=str(service.root), executable=context['executable'],
        cwd=context['cwd'], context_digest=digest(context))


def review(root):
    service = UserService(root)
    service.prepare()  # Ownership/context checks and TASK_VALIDATE_ONLY, no registration.
    receipt, task = service.observe() if service.receipt() else (None, None)
    action = 'install' if receipt is None else 'reuse' if task.enabled else 'enable'
    value = dict(format=1, scope=scope(service), action=action,
        native_digest=task.digest if task else None,
        enabled_digest=windows_definition_digest(windows_with_enabled(task.xml, True)) if task else None,
        effects=[
            'Start this Connector at login for the effective user, using the existing local credential context.',
            'No system service, elevation, availability before login or after logout is requested.',
            'Login startup applies to this Connector even while Nexus approval is pending.',
        ])
    if action == 'install':
        value['effects'].append('After this confirmation is saved, drain and stop this Connector, install its user logon task, and start it through that task. Active work may delay the transition; no forced termination is used.')
    elif action == 'enable':
        value['effects'].append('Enable the existing reviewed task without replacing its definition, then start or reuse the Connector.')
    else:
        value['effects'].append('Reuse the enabled user task without replacing it or restarting a ready Connector.')
    return dict(value, plan_revision=digest(value))


def validate(root, plan):
    service = UserService(root)
    if (plan.get('format') != 1 or plan.get('action') not in ('install', 'enable', 'reuse') or
            digest({k: v for k, v in plan.items() if k != 'plan_revision'}) != plan.get('plan_revision') or
            plan['scope'] != scope(service)):
        raise conflict('LOGIN_START_REVIEW_CHANGED', 'Restore the reviewed user, instance and execution context before resuming this request.')
    receipt = service.receipt()
    if plan['action'] == 'install':
        service.prepare()
        if receipt is not None and receipt['state'] == 'installed':
            _, task = service.observe()
            if not task.enabled:
                raise conflict('LOGIN_START_REVIEW_CHANGED', 'The installed task was disabled. Preserve that choice and review enablement explicitly.')
        return
    if receipt is None or receipt.get('context_digest') != plan['scope']['context_digest']:
        raise conflict('LOGIN_START_REVIEW_CHANGED', 'The reviewed task or its execution context changed.')
    pending = receipt.get('enablement')
    if pending:
        if (plan['action'] != 'enable' or pending.get('target') is not True or
                pending.get('from_digest') != plan['native_digest']):
            raise conflict('LOGIN_START_REVIEW_CHANGED', 'Another enablement transition is pending.')
        service._check_context(receipt)
        return  # The existing setter reconciles only this retained native change.
    _, task = service.observe()
    allowed = {plan['native_digest']}
    if plan['action'] == 'enable':
        allowed.add(plan['enabled_digest'])
    if task.digest not in allowed:
        raise conflict('LOGIN_START_REVIEW_CHANGED', 'The native task changed after review. Its current definition was preserved.')
