"""Winpos crossing detection, computed inline with the extract replay.

Replaces the SECOND full replay that build_crush_winpos_phase2.py performs
(~47 h, peak 76 GB) with a pass over the buffers build_pooled_stats.py already
fills. build_crush_winpos.winpos_sql stays the definition of the event and the
oracle this is tested against — it is not being replaced, only precomputed.

The event, unchanged: per game per side, ONE event at the EARLIEST of
  (a) the first position strictly AFTER the edge whose eval crosses +thresh for
      that side, expressed in full moves as ply // 2, and
  (b) the decisive game end by mate/resignation (termination == 'Normal'),
      at move_count.
move_bucket = clip(event_full_move - (ply - 1) // 2, 1, 60); every edge also
contributes n = 1 at bucket 0, which is the denominator.

Two things differ from the SQL and both are deliberate:

  * NO `keys` INNER JOIN. That table is the set of edges that survived the
    pool's min_games floor, which is a GLOBAL merge decision and simply does not
    exist while a single file is being extracted. Rows are therefore emitted for
    every edge and semi-joined to the survivors at merge time — the pattern
    merge_crush already uses for the retired histogram.

  * Thresholds are a parameter, not a rebuild. The eval of each position is read
    once and compared against each threshold, so three thresholds cost three
    comparisons rather than three passes over the data.

MISSING must be masked explicitly. In SQL an uncovered position simply fails to
join win_w/win_b; here it is the sentinel -32768, which would satisfy
`eval <= -thresh` and manufacture a black crossing at every position the eval DB
has never seen.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_crush_winpos import SENTINEL
from eval_arrays import MISSING

MISSING_I = int(MISSING)


def game_events(plies, evals, thresh_cp: int, wwn: bool, bwn: bool,
                move_count) -> tuple[list, list]:
    """Per-edge (ev_w, ev_b) in FULL MOVES, SENTINEL where no event.

    `plies` must be ascending (replay order). One reverse scan gives every edge
    the smallest crossing ply strictly greater than its own: the crossing at an
    edge's OWN ply is excluded, which is guarantee 3 (a game already winning
    before the move must not credit the move).
    """
    n = len(plies)
    ev_w = [SENTINEL] * n
    ev_b = [SENTINEL] * n
    nxt_w = nxt_b = None
    for i in range(n - 1, -1, -1):
        if nxt_w is not None:
            ev_w[i] = nxt_w // 2
        if nxt_b is not None:
            ev_b[i] = nxt_b // 2
        e = evals[i]
        if e != MISSING_I:
            if e >= thresh_cp:
                nxt_w = plies[i]
            elif e <= -thresh_cp:
                nxt_b = plies[i]
    if wwn and move_count is not None:
        mc = int(move_count)
        ev_w = [mc if v > mc else v for v in ev_w]
    if bwn and move_count is not None:
        mc = int(move_count)
        ev_b = [mc if v > mc else v for v in ev_b]
    return ev_w, ev_b


def _bucket(ev: int, ply: int) -> int:
    b = ev - (ply - 1) // 2
    return 1 if b < 1 else (60 if b > 60 else b)


def winpos_batch(parent_hash, move_san, ply, evals, spans, facts,
                 thresh_cp: int) -> dict:
    """Raw (unaggregated) histogram rows for one read batch at one threshold.

    spans   [(start, end)] index ranges into the flat arrays, one per game
    facts   [(white_win_normal, black_win_normal, move_count)] aligned with spans

    The caller aggregates these exactly like _agg_crush does; emitting raw rows
    keeps this function free of any polars dependency and trivially checkable.
    """
    out = {k: [] for k in ("parent_hash", "move_san", "move_bucket",
                           "n", "white_wins", "black_wins")}
    ph_o, sa_o, mb_o = out["parent_hash"], out["move_san"], out["move_bucket"]
    n_o, w_o, b_o = out["n"], out["white_wins"], out["black_wins"]

    for (s, e), (wwn, bwn, mc) in zip(spans, facts):
        if e <= s:
            continue
        gp = ply[s:e]
        ev_w, ev_b = game_events(gp, evals[s:e], thresh_cp, bool(wwn), bool(bwn), mc)
        for j in range(e - s):
            i = s + j
            p, h, sn = gp[j], parent_hash[i], move_san[i]
            # denominator: every edge, once
            ph_o.append(h); sa_o.append(sn); mb_o.append(0)
            n_o.append(1); w_o.append(0); b_o.append(0)
            vw = ev_w[j]
            if vw < SENTINEL:
                ph_o.append(h); sa_o.append(sn); mb_o.append(_bucket(vw, p))
                n_o.append(0); w_o.append(1); b_o.append(0)
            vb = ev_b[j]
            if vb < SENTINEL:
                ph_o.append(h); sa_o.append(sn); mb_o.append(_bucket(vb, p))
                n_o.append(0); w_o.append(0); b_o.append(1)
    return out
