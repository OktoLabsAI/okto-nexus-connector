"""Select a logical Server workspace independently of this host's folder."""
from ...errors import ConnectorError


async def select_workspace(args, output, http, key, *, choice, text):
    if args.workspace_id:
        return args.workspace_id, args.workspace_label
    items, cursor, seen = [], None, set()
    try:
        while True:
            page = await http.list_connection_workspaces(key, after_workspace_id=cursor)
            if not isinstance(page, dict):
                raise ConnectorError('PROTOCOL_ERROR', 'configure', 'Invalid workspace catalog response.')
            rows = page.get('items')
            if (not isinstance(rows, list) or len(rows) > 100 or type(page.get('has_more')) is not bool
                    or any(not isinstance(r, dict) or not isinstance(r.get('workspace_id'), str)
                           or not r['workspace_id'] or len(r['workspace_id']) > 160
                           or r.get('display_name') is not None and not isinstance(r['display_name'], str)
                           for r in rows)):
                raise ConnectorError('PROTOCOL_ERROR', 'configure', 'Invalid workspace catalog response.')
            for row in rows:
                if row['workspace_id'] not in seen:
                    items.append(row)
                    seen.add(row['workspace_id'])
            if not page['has_more']:
                break
            next_cursor = page.get('next_workspace_id')
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor <= (cursor or '') or len(items) >= 1000:
                raise ConnectorError('OPERATION_CONFLICT', 'configure',
                    'Workspace catalog is too large or changed. Retry with an explicit --workspace-id.')
            cursor = next_cursor
    except ConnectorError as error:
        if error.code not in {'PERMISSION_DENIED', 'NOT_FOUND'}:
            raise
        output.line('Workspace listing is unavailable for this identity or Server. You may enter a known workspace ID or create a new workspace; Nexus still checks connection approval.')
        use_id = choice('Nexus workspace', [(True, 'Enter existing workspace ID'), (False, 'Create new workspace')], False)
        return (text('Existing Nexus workspace ID', required=True), args.workspace_label) if use_id else (None, args.workspace_label)
    if not items:
        output.line('No existing workspaces are available. This connection will request a new workspace.')
        return None, args.workspace_label
    output.line('Select the logical workspace in Nexus. The folder on this machine is configured separately; other hosts may use different folders.')
    selected = choice('4. Nexus workspace', [
        (r['workspace_id'], f"{r.get('display_name') or 'Unnamed workspace'} ({r['workspace_id']})") for r in items
    ] + [(None, 'Create new workspace')], None)
    if selected is None:
        return None, args.workspace_label
    row = next((r for r in items if r['workspace_id'] == selected), None)
    if row is None:
        raise ConnectorError('VALIDATION_ERROR', 'configure', 'Choose a workspace from the list.')
    return selected, row.get('display_name') or args.workspace_label
