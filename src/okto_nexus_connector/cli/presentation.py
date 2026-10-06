"""Dependency-free terminal presentation; pipes and NO_COLOR stay plain."""
import os
import re
import shutil
import textwrap

_ESCAPES = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]|[\x00-\x08\x0b-\x1f\x7f]')


def plain(value):
    return _ESCAPES.sub('', str(value if value is not None else '-')).replace('\t', '    ')


def supports_color(stream):
    if 'NO_COLOR' in os.environ or os.environ.get('TERM') == 'dumb' or not getattr(stream, 'isatty', lambda: False)():
        return False
    if os.name == 'nt':
        try:
            import ctypes
            import msvcrt
            handle = msvcrt.get_osfhandle(stream.fileno())
            mode = ctypes.c_ulong()
            kernel = ctypes.windll.kernel32
            if not kernel.GetConsoleMode(ctypes.c_void_p(handle), ctypes.byref(mode)):
                return False
            return bool(kernel.SetConsoleMode(ctypes.c_void_p(handle), mode.value | 4))
        except (OSError, ValueError, AttributeError):
            return False
    return True


def styled(text, style, stream):
    text = plain(text)
    code = {'heading': '1;36', 'label': '1', 'ok': '32', 'warning': '33', 'error': '31', 'muted': '2'}.get(style, '0')
    return f'\033[{code}m{text}\033[0m' if supports_color(stream) else text


def state_style(value):
    state = str(value).upper()
    if state in {'CONNECTED', 'CONTROL_READY', 'RUNNING', 'OK', 'READY', 'APPROVED', 'SUCCESS'}:
        return 'ok'
    if any(word in state for word in ('ERROR', 'FAIL', 'DENIED', 'REVOKED', 'REJECTED', 'UNREACHABLE')):
        return 'error'
    return 'warning'


def table(headers, rows, stream):
    """ASCII borders, wrapped cells and a stacked fallback for narrow terminals."""
    headers = [plain(h) for h in headers]
    rows = [[plain(cell) for cell in row] for row in rows]
    width = shutil.get_terminal_size((100, 24)).columns
    widths = [max(len(h), *(min(45, max(map(len, cell[i].splitlines()), default=1)) for cell in rows))
              for i, h in enumerate(headers)] if rows else [len(h) for h in headers]
    minimum = [max(10, len(h)) for h in headers]
    if sum(minimum) + 3 * (len(headers) - 1) > width:
        for row in rows:
            for h, cell in zip(headers, row):
                for line in textwrap.wrap(f'{h}: {cell}', max(10, width), subsequent_indent='  ') or ['']:
                    print(line, file=stream)
            print(file=stream)
        return
    while sum(widths) + 3 * (len(headers) - 1) > width:
        eligible = [i for i, w in enumerate(widths) if w > minimum[i]]
        if not eligible:
            break
        i = max(eligible, key=lambda i: widths[i])
        widths[i] -= 1
    print(styled(' | '.join(h.ljust(w) for h, w in zip(headers, widths)), 'label', stream), file=stream)
    print('-+-'.join('-' * w for w in widths), file=stream)
    for row in rows:
        cells = [textwrap.wrap(cell, w, replace_whitespace=True) or ['-'] for cell, w in zip(row, widths)]
        for n in range(max(map(len, cells))):
            parts = []
            for i, (cell, w) in enumerate(zip(cells, widths)):
                value = (cell[n] if n < len(cell) else '').ljust(w)
                parts.append(styled(value, state_style(row[i]), stream) if headers[i].lower() in {'status', 'state'} else value)
            print(' | '.join(parts).rstrip(), file=stream)
