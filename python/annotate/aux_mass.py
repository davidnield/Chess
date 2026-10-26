"""The games a pool's surviving edges don't show, per position (the aux sidecar).

A pool keeps an edge only above its min_games floor; the rest of a position's
games sit in the aux sidecar: replies below the floor (`other_total`, spread over
`other_edges` distinct moves), games that ended there (`term_*_total`) and games cut
by the ply cap (`horizon_total`). A reply's share must be taken over ALL games that
reached the position, or the one surviving reply of a thin position reads as 100%.

The sidecar sits next to its pool: position_stats_<tag>.parquet ->
position_stats_aux_<tag>.parquet.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def aux_path_for(stats_path: str) -> Path | None:
    p = Path(stats_path)
    if not p.name.startswith("position_stats_"):
        return None
    aux = p.with_name(p.name.replace("position_stats_", "position_stats_aux_", 1))
    return aux if aux.exists() else None


def load_aux_mass(stats_path: str, hashes) -> dict[int, dict]:
    """position_hash -> {other, other_moves, ended, horizon} games ({} without a sidecar)."""
    aux = aux_path_for(stats_path)
    if aux is None or not hashes:
        return {}
    df = (pl.scan_parquet(aux)
            .filter(pl.col("position_hash").is_in(list(hashes)))
            .select("position_hash", "other_total", "other_edges", "horizon_total",
                    (pl.col("term_normal_total") + pl.col("term_flag_total")
                     + pl.col("term_other_total")).alias("ended"))
            .collect())
    return {r["position_hash"]: {"other": int(r["other_total"] or 0),
                                 "other_moves": int(r["other_edges"] or 0),
                                 "ended": int(r["ended"] or 0),
                                 "horizon": int(r["horizon_total"] or 0)}
            for r in df.iter_rows(named=True)}


def unseen_games(m: dict | None) -> int:
    """Games at the position that no surviving edge counts."""
    return 0 if not m else m["other"] + m["ended"] + m["horizon"]
