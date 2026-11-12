"""One definition of the move menu a budget book was built from, for everything that re-derives it.

build_budget_books.py expands its graph from --stats, or from an uncut_own_moves.py cache when
--uncut-own-moves is given: every observed move at OUR decisions, the floored pool at the
opponent's. validate_book.py, cloud_feedback.py and rare_verify.py must rebuild the same candidate
sets from a book's .meta.json, or they audit a different book: a validator on the floored pool
calls every restored prescription 'outside the set' and never sees the restored rival that decides
a gate. Books built before the uncut lookup carry no 'uncut_own_moves' key and read as before.

    source, where = edge_source(meta, colour)       # parquet path, SQL filter on alias s
    rare_floor, verified = rare_policy(meta, frame)  # the builder's --rare-verification inputs
"""
from __future__ import annotations

from pathlib import Path

import budget_core as bc


def edge_source(meta: dict, colour: str) -> tuple[str, str]:
    """(parquet path, WHERE clause over alias `s`) for the edges this book's builder saw."""
    floor = int(meta.get("min_games", 50))          # pre-2026-10-31 metas: the old default
    cache = meta.get("uncut_own_moves")
    if not cache:
        return str(meta["stats"]).replace("\\", "/"), f"s.total >= {floor}"
    from uncut_own_moves import digest, load
    # The lookup's contract includes the opponent overlay the builder gave it (a Maia reply
    # prior); the book's meta records that overlay's file and hash. Hand the reader the same
    # one, and refuse a file that has changed since the build.
    prior = meta.get("opponent_prior")
    overlay = prior["file"] if prior else None
    if overlay and digest(overlay) != prior["sha256"]:
        raise ValueError("The opponent overlay this book was built with has changed on disk")
    ready = load(cache, meta["stats"], meta["aux_stats"], colour, overlay)
    if ready["recipe_sha256"] != meta.get("uncut_own_recipe"):
        raise ValueError("This book was built from another own-move lookup than the one on disk")
    turn = "w" if colour == "white" else "b"
    return ((Path(cache) / "graph.parquet").as_posix(),
            f"(s.total >= {floor} OR split_part(s.parent_epd, ' ', 2) = '{turn}')")


def edges_at(con, meta: dict, colour: str, parents) -> dict[int, list[dict]]:
    """parent hash -> its edge dicts (move_san, child_hash, total, white_score_avg), in the
    collector's order."""
    import polars as pl
    from duck_compat import duck_pl
    source, where = edge_source(meta, colour)
    con.register("book_graph_keys",
                 pl.DataFrame({"h": sorted(set(parents))}, schema={"h": pl.Int64}).to_arrow())
    e = duck_pl(con, f"""SELECT s.parent_hash, s.move_san, s.child_hash, s.total, s.white_score_avg
        FROM read_parquet('{source}') s SEMI JOIN book_graph_keys k ON k.h = s.parent_hash
        WHERE {where}""")
    con.unregister("book_graph_keys")
    e = e.sort("parent_hash", "move_san", "child_hash", nulls_last=True)
    out: dict[int, list[dict]] = {}
    for h, group in e.group_by("parent_hash", maintain_order=True):
        out[h[0]] = group.to_dicts()
    return out


def rare_policy(meta: dict, proposals=None) -> tuple[int | None, dict]:
    """(rare_floor, verified moves by parent) exactly as the build applied them."""
    floor = meta.get("rare_floor")
    return (None, {}) if floor is None else (int(floor), bc.verified_moves(proposals))
