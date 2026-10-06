"""Terminal questions with retry, visible defaults and explicit cancellation."""
import sys
from ..errors import ConnectorError


def text(label, default='', *, required=False):
    while True:
        print(f'{label}' + (f' [{default}]' if default else '') + ': ', end='', file=sys.stderr, flush=True)
        try:
            value = input().strip() or str(default)
            if value == '-':value=''
        except EOFError:
            raise ConnectorError('VALIDATION_ERROR','configure','Configuration canceled: terminal input ended.') from None
        if value or not required:
            return value
        print('This field is required.', file=sys.stderr)


def choice(label, options, default=None):
    """Values are opaque keys, labels are human-readable (never credentials)."""
    if not options:
        raise ConnectorError('VALIDATION_ERROR','configure',f'No available choices: {label}')
    print(label, file=sys.stderr)
    for index, (key, caption) in enumerate(options, 1):
        print(f'  {index}. {caption}' + (' (current)' if key == default else ''), file=sys.stderr)
    default_index = next((str(i) for i,(key,_) in enumerate(options,1) if key == default),'')
    while True:
        value = text('Choice', default_index, required=True)
        if value.isdecimal() and 1 <= int(value) <= len(options):
            return options[int(value)-1][0]
        print('Select a number from the list.', file=sys.stderr)


def limit(label, default, maximum):
    """0 means unlimited; the caller sets the matching Server flag."""
    while True:
        value = text(label + ' (0 = unlimited)', str(default or 0), required=True)
        if value.isdecimal() and 0 <= int(value) <= maximum:
            return int(value)
        print(f'Enter 0 (unlimited) or 1–{maximum}.', file=sys.stderr)


def preferences(configuration, schema):
    current = configuration['harness_settings']
    result = {}
    model = text('Model (blank uses harness default)',current.get('model',''))
    if model: result['model']=model
    for field in schema['parameters']:
        if field['name']=='model' or not field['core_applies']:
            continue
        name=field['name']
        if field['type']=='enum':
            options=[('', 'Harness default')] + [(v,v) for v in field['values']]
            value=choice(field['label'],options,current.get(name,''))
        else:
            value=text(field['label']+' (optional)',current.get(name,''))
        if value:result[name]=value
    configuration['harness_settings']=result
    configuration['session_policy']=choice('Conversation context',[(None,'Use global setting'),('per_sender','One session per sender'),('per_sender_session','One session per sender + source session'),('shared','Shared')],configuration['session_policy'])
    configuration['automatic_reply']=True
    configuration['tool_access']=choice('Nexus tool approval',[('ask','Ask for approval'),('always_allow','Always allow')],configuration['tool_access'])
    minutes=limit('Authorization duration in minutes',configuration['authorization']['minutes'],1440)
    actions=limit('Action limit',configuration['authorization']['actions'],1000)
    configuration['authorization']={'minutes':minutes,'actions':actions,
        'no_expiry':minutes==0,'unlimited_actions':actions==0}
    return configuration
