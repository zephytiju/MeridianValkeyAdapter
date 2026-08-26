#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail when a versioned text artifact lacks an Apache-2.0 SPDX marker."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUFFIXES = {".conf", ".json", ".md", ".py", ".sh", ".toml", ".yaml", ".yml"}
EXCLUDED = {"LICENSE", "NOTICE"}


def main() -> int:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required for the SPDX check")
    result = subprocess.run(  # noqa: S603 - resolved executable and fixed read-only argv
        [git, "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    missing: list[str] = []
    for relative in sorted(filter(None, result.stdout.splitlines())):
        path = ROOT / relative
        if not path.is_file() or path.name in EXCLUDED or path.suffix not in SUFFIXES:
            continue
        head = path.read_text(encoding="utf-8")[:4096]
        if "SPDX-License-Identifier: Apache-2.0" not in head:
            missing.append(relative)
    if missing:
        print("missing SPDX-License-Identifier: Apache-2.0:")
        print("\n".join(missing))
        return 1
    print("SPDX check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
