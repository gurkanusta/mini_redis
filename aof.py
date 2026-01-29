from __future__ import annotations
from pathlib import Path
from typing import Iterable

class AOF:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, line: str) -> None:

        with self.path.open("a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")

    def replay_lines(self) -> Iterable[str]:
        if not self.path.exists():
            return []

        def _gen():
            with self.path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield line
        return _gen()
