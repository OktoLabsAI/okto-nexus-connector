"""Normalized local build: wheel + sdist into dist/ with hashes."""
from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path


def main() -> int:
    project = Path(__file__).parents[1]
    dist = project / "dist"
    dist.mkdir(exist_ok=True)
    result = subprocess.run(
        [sys.executable, "-m", "build", "--outdir", str(dist)],
        cwd=project)
    if result.returncode != 0:
        return result.returncode
    for artifact in sorted(dist.iterdir()):
        if artifact.is_file() and artifact.suffix in (".whl", ".gz"):
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            print(f"{artifact.name}  sha256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
