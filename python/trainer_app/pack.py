"""TrainingPack: runtime loader for the pack parquets.

Loads each color's tree + cards into numpy arrays / lists once (sub-second,
tens of MB) and answers the queries the trainer and deviation scanner need:
line reconstruction by parent pointers, card lookups, children of a node, and
sorted-array membership probes for the full-book dev lookup.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import polars as pl

COLORS = ("white", "black")


class TrainingPack:
    def __init__(self, pack_dir: Path):
        self.pack_dir = Path(pack_dir)
        meta_path = self.pack_dir / "pack_meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(
                f"no training pack at {self.pack_dir} — run build_training_pack.py")
        self.meta = json.loads(meta_path.read_text(encoding="utf-8"))

        self.tree: dict[str, dict] = {}       # color -> column dict
        self.children: dict[str, dict[int, list[int]]] = {}
        self.cards: dict[str, pl.DataFrame] = {}
        self.card_by_hash: dict[str, dict[int, int]] = {}   # hash -> cards row idx
        self._dev_ph: dict[str, np.ndarray] = {}
        self._dev_san: dict[str, list[str]] = {}

        for color in COLORS:
            t = pl.read_parquet(self.pack_dir / f"{color}_tree.parquet")
            cols = {c: t[c].to_list() for c in
                    ("parent_id", "move_san", "position_hash", "epd", "ply",
                     "is_our_turn", "best_move", "path_reach", "reply_share")}
            self.tree[color] = cols
            kids: dict[int, list[int]] = defaultdict(list)
            for nid, pid in enumerate(cols["parent_id"]):
                if pid >= 0:
                    kids[pid].append(nid)
            self.children[color] = kids

            c = pl.read_parquet(self.pack_dir / f"{color}_cards.parquet")
            self.cards[color] = c
            self.card_by_hash[color] = {h: i for i, h in enumerate(c["position_hash"])}

            d = pl.read_parquet(self.pack_dir / f"dev_lookup_{color}.parquet")
            self._dev_ph[color] = d["position_hash"].to_numpy()   # sorted at build
            self._dev_san[color] = d["best_move"].to_list()

    # ── tree queries ────────────────────────────────────────────────────────

    def path_to_root(self, color: str, node_id: int) -> list[int]:
        """Node ids from root to node_id inclusive (root first)."""
        cols = self.tree[color]
        path = []
        nid = node_id
        while nid >= 0:
            path.append(nid)
            nid = cols["parent_id"][nid]
        path.reverse()
        return path

    def line_sans(self, color: str, node_ids: list[int]) -> list[str]:
        """SAN sequence along a root-first node path (root's None skipped)."""
        ms = self.tree[color]["move_san"]
        return [ms[n] for n in node_ids if ms[n] is not None]

    def node(self, color: str, node_id: int) -> dict:
        cols = self.tree[color]
        return {k: cols[k][node_id] for k in cols}

    # ── card / book queries ─────────────────────────────────────────────────

    def card(self, color: str, position_hash: int) -> dict | None:
        i = self.card_by_hash[color].get(position_hash)
        return self.cards[color].row(i, named=True) if i is not None else None

    def book_move(self, color: str, position_hash: int) -> str | None:
        """Full-book expected move (dev_lookup probe), or None if out of book."""
        ph = self._dev_ph[color]
        i = int(np.searchsorted(ph, position_hash))
        if i < len(ph) and int(ph[i]) == position_hash:
            return self._dev_san[color][i]
        return None
