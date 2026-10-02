"""The winpos ("winning position achieved") crush event: its definition, as SQL, and the
reference replay that feeds it. Production computes the event inside the extract
(winpos_fused.py); this module is the independent oracle it is held equal to
(_test_winpos.py, _test_winpos_fused.py). Lifted verbatim from the retired second-pass
builders build_crush_winpos.py / build_crush_winpos_phase2.py (now in scratch/).

Event definition: per game per side, ONE event =

    EARLIEST of:
      (a) the first position reached strictly AFTER the edge whose Stockfish
          eval is >= +thresh_cp for our side (decisive evals are capped at
          +-2000 in the eval DB, so mate-class evals are included), and
      (b) the decisive game end by mate/resignation (termination='Normal').

    move_bucket = clip(event_full_move - (ply-1)//2, 1, 60)   [same as crush_hist_rel]
    All games through the edge contribute n=1 at bucket 0 (the denominator);
    Stage 3 only ever sums n across buckets, so where n lives is immaterial.

Guarantees (task #27; verified by _test_winpos.py against this exact SQL):
  1. No double counting — min() of the two event types: a game that crosses +3
     at move 10 and resigns at move 18 lands ONE count, in the move-10 bucket.
  2. Ever-achieved, not frontier-eval — ALL positions along the game's path are
     checked, so a +3 later given away still counts. (Backwards induction cannot
     provide this: propagated value is an expectation, not a first-passage.)
  3. Downstream-only — only positions strictly after the edge are checked
     (position at pm ply c = position after c-1 plies, hence "after our move at
     ply p" == c > p), so the edge's PARENT never credits it.
     That does NOT make the event a crossing. This guarantee used to claim "a
     game already winning BEFORE an edge does not credit it"; corrected
     2026-09-22. A game winning at the parent that STAYS winning makes the
     child a winning position too, so the edge is credited at bucket 1 —
     measured on the 2013-2026 t300 histogram, 81.7% of games through edges
     from >= +300 positions. The histogram records a STATE ("a winning
     position within b moves"). The EVENT semantics are applied by the
     consumer: stage3 --crush-won-cp makes already-winning positions
     crush-absorbing, which is exact because the eval is per position, so
     the histogram itself did not need a re-extract.
  4. Coverage — eval-crossings fire only where the eval DB covers the position;
     mate/resignation events fire for ALL games. Uncovered (rare) lines are
     structurally biased against, consistent with --crush-baseline zero.
  5. Crossings beyond the extraction depth (ply 30) are unobservable; the
     decisive-end component (which sees the true game length) covers that tail.
"""
from __future__ import annotations

import chess

from stage1_extract_positions import iter_san_moves
from zobrist import zobrist_int64

MIN_BAND = 1800    # pm-side elo_band floor == the >=1800 pool population
SENTINEL = 9999    # "no event" full-move sentinel (> any real move_count)


def winpos_sql(pm_path: str, crush_path: str, min_band: int = MIN_BAND) -> str:
    """The per-file event/histogram query. Expects three temp tables in the
    connection: keys(parent_hash, move_san), win_w(position_hash),
    win_b(position_hash). pm_path holds (game_id, ply, parent_hash, move_san,
    elo_band) rows; crush_path the per-game (game_id, white_win_normal,
    black_win_normal, move_count) facts.

    Per game side s: ev_s = LEAST(first winning position strictly after the
    edge (full move = crossing_ply // 2), decisive-normal end move). One event
    per game per side -> no double counting by construction. Bucket 0 rows
    carry n only.
    """
    q = lambda p: str(p).replace("\\", "/")
    return f"""
    WITH pm AS (
        SELECT game_id, ply, parent_hash, move_san
        FROM read_parquet('{q(pm_path)}')
        WHERE elo_band >= {int(min_band)}
    ),
    cw AS (  -- per game: sorted plies whose position is winning for White
        SELECT pm.game_id, list_sort(list(pm.ply)) AS cps
        FROM pm JOIN win_w w ON pm.parent_hash = w.position_hash
        GROUP BY pm.game_id
    ),
    cb AS (
        SELECT pm.game_id, list_sort(list(pm.ply)) AS cps
        FROM pm JOIN win_b w ON pm.parent_hash = w.position_hash
        GROUP BY pm.game_id
    ),
    g0 AS (
        SELECT e.parent_hash, e.move_san, e.ply,
               list_filter(cw.cps, x -> x > e.ply)[1] AS cw_ply,
               list_filter(cb.cps, x -> x > e.ply)[1] AS cb_ply,
               c.white_win_normal AS wwn, c.black_win_normal AS bwn,
               c.move_count AS mc
        FROM pm e
        JOIN keys k  ON e.parent_hash = k.parent_hash AND e.move_san = k.move_san
        JOIN read_parquet('{q(crush_path)}') c ON e.game_id = c.game_id
        LEFT JOIN cw ON e.game_id = cw.game_id
        LEFT JOIN cb ON e.game_id = cb.game_id
    ),
    g AS (
        SELECT parent_hash, move_san, ply,
               LEAST(COALESCE(cw_ply // 2, {SENTINEL}),
                     CASE WHEN wwn = 1 THEN mc ELSE {SENTINEL} END) AS ev_w,
               LEAST(COALESCE(cb_ply // 2, {SENTINEL}),
                     CASE WHEN bwn = 1 THEN mc ELSE {SENTINEL} END) AS ev_b
        FROM g0
    )
    SELECT parent_hash, move_san, move_bucket,
           SUM(n)::BIGINT AS n,
           SUM(w)::BIGINT AS white_wins,
           SUM(b)::BIGINT AS black_wins
    FROM (
        SELECT parent_hash, move_san, 0 AS move_bucket, 1 AS n, 0 AS w, 0 AS b
        FROM g
        UNION ALL
        SELECT parent_hash, move_san,
               LEAST(GREATEST(ev_w - (ply - 1) // 2, 1), 60), 0, 1, 0
        FROM g WHERE ev_w < {SENTINEL}
        UNION ALL
        SELECT parent_hash, move_san,
               LEAST(GREATEST(ev_b - (ply - 1) // 2, 1), 60), 0, 0, 1
        FROM g WHERE ev_b < {SENTINEL}
    )
    GROUP BY parent_hash, move_san, move_bucket
    """


def _walk_game_winpos(pm_buf: dict, game_id, movetext, elo_band, max_ply: int,
                      hasher=None) -> bool:
    """Replay one game to max_ply, appending its pm rows (game_id, ply,
    parent_hash, move_san, elo_band) to pm_buf. Same replay shape as
    build_pooled_stats._walk_game, but no crush-bucket computation here — that
    happens in winpos_sql from the crush-fact table. Returns True if a SAN
    failed to parse (the game is cut there).

    `hasher` is an optional reused IncrementalZobrist (see build_pooled_stats.
    _walk_game); omitting it reproduces the original full-rescan path exactly.
    No EPD memo here — this walk never materializes EPDs.
    """
    if not movetext:
        return False
    toks = list(iter_san_moves(movetext))
    if not toks:
        return False
    maxply = min(max_ply, len(toks))
    board = chess.Board()
    if hasher is None:
        get_hash = lambda: zobrist_int64(board)
        push = board.push
    else:
        hasher.reset(board)
        get_hash = lambda: hasher.current(board)
        push = lambda mv: hasher.push_move(board, mv)
    for ply in range(1, maxply + 1):
        san = toks[ply - 1]
        ph = get_hash()
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            return True
        pm_buf["game_id"].append(game_id)
        pm_buf["ply"].append(ply)
        pm_buf["parent_hash"].append(ph)
        pm_buf["move_san"].append(san)
        pm_buf["elo_band"].append(elo_band)
        push(move)
    return False
