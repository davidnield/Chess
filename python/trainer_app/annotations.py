"""Annotation loader for the trainer app.

Reads trainer_data/annotations/annotations.parquet into a
{(rep, position_hash) -> {annotation, memory_rule, themes, flagged}} map,
hot-reloading when the file's mtime changes. Absent file = no annotations
(every lookup returns None); the app works unchanged without them.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


class Annotations:
    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "annotations" / "annotations.parquet"
        self._mtime = None
        self._map: dict[tuple[str, int], dict] = {}
        self.reload()

    def reload(self) -> None:
        if not self.path.exists():
            self._map = {}
            self._mtime = None
            return
        mt = self.path.stat().st_mtime
        if mt == self._mtime:
            return
        self._mtime = mt
        df = pl.read_parquet(self.path)
        self._map = {
            (r["color"], r["position_hash"]): {
                "annotation": r["annotation"],
                "memory_rule": r["memory_rule"],
                "themes": list(r["themes"] or []),
                "flagged": bool(r["flagged"]),
            }
            for r in df.iter_rows(named=True)
            if r["annotation"]           # skip empty/failed rows
        }

    def get(self, rep: str, position_hash: int) -> dict | None:
        self.reload()
        return self._map.get((rep, position_hash))
