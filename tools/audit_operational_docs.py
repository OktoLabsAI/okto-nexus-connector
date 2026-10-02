"""Validate current operational examples with an installed Connector parser.

This parses examples but never dispatches them. Only --help subprocesses run.
It does not prove the operational scenario or close release acceptance gates.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import hashlib
from io import StringIO
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys

import okto_nexus_connector
from okto_nexus_connector.cli.main import build_parser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    package = Path(okto_nexus_connector.__file__).resolve().parent
    assert 'site-packages' in str(package) and not package.is_relative_to(root)
    files = [root/'README.md', *(root/'docs').glob('*.md')]
    cli = build_parser()
    commands, links, documents = [], [], {}
    for path in sorted(files):
        value = path.read_text(encoding='utf-8')
        documents[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        for target in re.findall(r'\]\(([^)]+)\)', value):
            if target.startswith(('https://', 'http://', '#', 'mailto:')):
                continue
            file = target.split('#', 1)[0]
            assert (path.parent/file).exists(), (path, target)
            links.append(dict(document=str(path.relative_to(root)), target=target))
        for number, line in enumerate(value.splitlines(), 1):
            if not line.startswith('okto-nexus-connector '):
                continue
            argv = shlex.split(line, comments=True)[1:]
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                cli.parse_args(argv)  # Syntax only; no credential, network or process effect.
            result = subprocess.run([sys.executable, '-I', '-m',
                'okto_nexus_connector.cli.main', *argv, '--help'], capture_output=True,
                text=True, encoding='utf-8', timeout=30)
            assert result.returncode == 0 and 'usage:' in result.stdout.lower(), (argv, result.stderr)
            commands.append(dict(document=str(path.relative_to(root)), line=number,
                argv=argv, exit_code=result.returncode,
                help_sha256=hashlib.sha256(result.stdout.encode()).hexdigest()))
    assert commands, 'No documented commands were audited'
    report = dict(status='PASS', scope='Installed parser/help and local-link audit; not scenario acceptance',
        package=str(package), documents=documents, commands=commands, links=links,
        command_count=len(commands), full_release_accepted=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(dict(status=report['status'], commands=len(commands), links=len(links))))


if __name__ == '__main__':
    main()
