"""Generate CLI syntax documentation directly from argparse."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from okto_nexus_connector.cli.main import build_parser


def render():
    lines = ['# CLI syntax reference', '',
        'Generated from the CLI parser. Regenerate with `python tools/generate_cli_reference.py`.', '',
        'Read the [usage guide](cli.md) for effects and prerequisites. --json and --verbose work before or after any command; other global options go before the command. Uppercase values are placeholders. Brackets mean optional; unbracketed flags are required. Legacy syntax is not proof of R4 support.', '']
    def walk(parser, name):
        lines.extend(['## ' + name, '', '```text', parser.format_help().rstrip(), '```', ''])
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for command, child in action.choices.items():
                    walk(child, name + ' ' + command)
    walk(build_parser(), 'okto-nexus-connector')
    return '\n'.join(lines)


if __name__ == '__main__':
    (ROOT / 'docs' / 'cli-reference.md').write_text(render(), encoding='utf-8')
