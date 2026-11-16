"""Local startup choice and checkpoint within the existing Connector wizard."""
import asyncio
import os

from ...daemon import manager
from ...errors import ConnectorError
from ...platform import login_start, service_install


def choose(args, output, root, choice):
    selected = getattr(args, 'start_at_login', None)
    if selected is None and os.name == 'nt':
        selected = choice('Start this Connector at user login?', [
            (False, 'Keep the current service mode'), (True, 'Start at user login')], False)
    if not selected:
        output.line('Startup: keep the current service mode; on-demand use remains available.')
        return None
    try:
        plan = login_start.review(root)
    except (OSError, ValueError) as error:
        raise ConnectorError('LOGIN_START_REVIEW_UNAVAILABLE', 'configure',
            'The local startup review could not be prepared.',
            action='Inspect this instance with service status before choosing startup.') from error
    scope = plan['scope']
    output.heading('Login startup')
    for label, value in [
        ('User', scope['principal']), ('Instance', scope['instance_id']),
        ('State directory', scope['root']), ('Interpreter', scope['executable']),
        ('Working directory', scope['cwd'])]:
        output.line(label + ': ' + value)
    for effect in plan['effects']:
        output.line('- ' + effect)
    return plan


async def resume(args, root, saved, save):
    plan = saved.get('login_start_plan')
    choice = getattr(args, 'start_at_login', None)
    if (choice is True and not plan) or (choice is False and plan):
        raise ConnectorError('OPERATION_CONFLICT', 'configure',
            'This request already has a confirmed startup choice.',
            action='Resume with the original choice. Use the explicit service commands to change an installed integration.')
    if not plan:
        return
    operation = saved.get('login_start_operation', {})
    if (operation.get('plan_revision') != plan.get('plan_revision') or
            operation.get('state') not in ('pending', 'installed', 'done')):
        raise ConnectorError('LOGIN_START_CHECKPOINT_INVALID', 'configure',
            'Inspect the retained startup intent before resuming this request.')
    if operation['state'] == 'done':
        return  # Never reenable/reinstall a later user change on reconnect.
    try:
        await asyncio.to_thread(login_start.validate, root, plan)
    except (OSError, ValueError) as error:
        raise ConnectorError('LOGIN_START_REVIEW_UNAVAILABLE', 'configure',
            'The retained startup context could not be revalidated.',
            action='Inspect this instance and restore the reviewed context before resuming.') from error
    try:
        if operation['state'] == 'pending':
            if plan['action'] == 'install':
                result = await asyncio.to_thread(service_install.install, root, dry_run=False)
                if not result.get('installed'):
                    raise ConnectorError('LOGIN_START_DRAIN_PENDING', 'configure',
                        'The Connector has not completed its controlled stop.',
                        action='Inspect daemon status and resume this same request after the drain completes.')
            elif plan['action'] == 'enable':
                await asyncio.to_thread(service_install.set_enabled, root, True)
            operation['state'] = 'installed'
            save()
        await asyncio.to_thread(manager.start, root)
        operation['state'] = 'done'
        save()
    except ConnectorError as error:
        raise ConnectorError(error.code, 'login_start', error.message, possible_effect=True,
            retry_safe=error.retry_safe, operation_id=args.request_id,
            action=error.action or 'Inspect this Connector and resume the same configuration request.') from error
    except (OSError, ValueError) as error:
        raise ConnectorError('LOGIN_START_UNCONFIRMED', 'login_start',
            'The native operation or its checkpoint was not confirmed.', possible_effect=True,
            operation_id=args.request_id,
            action='Inspect this Connector and resume the same request. The original onboarding intent is retained.') from error


async def result(root, saved, value):
    if not saved.get('login_start_plan'):
        return value
    current = await asyncio.to_thread(manager.status, root)
    return dict(value, login_start=dict(operation_state=saved['login_start_operation']['state'],
        current_user_service=current.user_service, service_state=current.state, reason=current.reason))
