"""
build_pooled_stats.py — fast, reusable combined position-stats + relative crush
histogram, pooled to a SINGLE slice (event='Pooled', elo_band=0).

This is the CANONICAL pooler for new datasets: it fuses extraction + elo/event
filtering + pooling straight from the source parquets. (build_combined_slice.py is the
OTHER pooling path — it re-pools stats that were already aggregated per (event,elo_band).
Use that only when you already have per-band position_stats and just want to combine them.)

Fused single-pass design (replaces the old Stage-1 raw-edge dump + Stage-2 tier
merge, whose tier/shard/fragment machinery was ~OOM-defensive scaffolding for the
1.56-billion-key multi-band slices — eliminated here by filtering + pooling):

  For each source parquet, BEFORE any move parsing, drop games with mean_elo
  below --min-elo (only ~1/3 of Blitz, ~1/5 of Rapid, ~1/6 of Classical qualify
  at 1800 -> ~75% of work skipped). Replay each surviving game ONCE and, per
  chunk, accumulate BOTH:
    - position-stats   : (parent_hash, move_san) -> child_hash + white/draw/black/total
      child_hash falls out of the replay for free (the position after ply p is
      the position before ply p+1), which is why the merge no longer re-derives
      it with python-chess in a single-threaded tail. See _walk_game.
    - relative crush   : (parent_hash, move_san, move_bucket) -> n/white/black
      move_bucket = clip(move_count - (ply-1)//2, 1, 60) for a decisive NORMAL
      win, else 0  (full moves from THIS position to the decisive end — same
      formula as build_crush_stats.py).
  Each chunk writes two SMALL pre-aggregated partials (never the multi-GB per-ply
  edge files). Two final single-pass DuckDB GROUP BYs merge the partials. No
  tiers — filtering + pooling keep cardinality bounded.

RETIRED OUTPUT: the resignation-proxy crush histogram is no longer merged unless
--crush-hist is passed. Nothing consumes it — build_sharp_reps.py reads the winpos
histogram (crush_hist_relwin_*, build_crush_winpos_phase2.py), whose win event is
the earliest of (eval >= +300cp, decisive-normal end) rather than terminations
alone. The extract still writes .crush.parquet partials, so it can be merged later
without re-extracting. The next full rebuild should fuse winpos into THIS script's
replay instead of running a second pass — see the task note in CLAUDE.md.

Reusable: parameterized by --start-year/--end-year/--months, --min-elo, --events,
--min-games, --max-ply. Resumable: per-chunk partial skip-gate + atomic .tmp
writes; re-run for a new month or a different filter = another invocation.

Usage:
  # full target build (2019-2025, Blitz+Rapid+Classical, mean_elo>=1800)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2019 --end-year 2025

  # single month, extract phase only (benchmarking)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2023 --end-year 2023 \
      --months 1 --phase extract --workers 11

  # merge phase only (after partials exist)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2019 --end-year 2025 --phase merge
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import chess
import numpy as np
import polars as pl
import pyarrow.parquet as pq

# Reuse the proven SAN tokenizer + signed-int64 Zobrist from Stage 1 (sibling module).
from stage1_extract_positions import iter_san_moves, zobrist_int64
from zobrist import IncrementalZobrist
from eval_arrays import (MISSING as _EVAL_MISSING, lookup_evals,
                         open_eval_arrays, verify_eval_arrays)
from winpos_fused import winpos_batch

# Winpos crossing thresholds, in centipawns. 300 is the canonical one every
# shipped repertoire was built against and must stay in the set; the others cost
# one extra comparison per position because the eval is already in hand, and
# having them means the threshold can be swept without another extract.
WINPOS_THRESHOLDS = (200, 300, 500)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")
STATS_DIR = Path("E:/chess/position-stats")
POOL_EVENT = "Pooled"
POOL_ELO = 0

# (year, month) partitions to skip. EMPTY as of 2026-08-05.
#
# 2024-10 was excluded because the HuggingFace copy ships broken movetext
# numbering (White's first ply labelled "0...", measured at 99.8% of games). That
# was a property of the HF export, not of the source: Lichess re-exported both
# 2018-03 and 2024-10 upstream on 2024-11-05 after fix bb54a07, and the served
# .pgn.zst is clean (0 failures in ~18K games each, with the detector validated
# to fire on 99.8% of the HF copies and stay silent on their neighbours). Both
# months have been re-fetched from source and passed the numbering gate.
#
# Left empty rather than deleted: a silent skip here is invisible in the output,
# so the next partition that needs excluding should be added WITH its reason.
SKIP_PARTITIONS: set[tuple[int, int]] = set()

# Source columns needed for the fused pass. No game_id needed (we aggregate, never
# join per-game crush facts). movetext is the heavyweight string.
SRC_COLUMNS = ["movetext", "white_score", "termination", "move_count", "mean_elo"]
READ_BATCH_GAMES = 50_000
# Compact the per-chunk accumulator every N batches to bound peak memory (the
# concat+regroup keeps the running frame near the chunk's unique-key count).
COMPACT_EVERY = 8


# ── per-chunk fused extraction ────────────────────────────────────────────────

def _agg_ps(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san").agg(
        pl.col("parent_epd").first().alias("parent_epd"),
        # child_hash is a FUNCTION of the group key (same position + same move
        # reaches the same position), so .first() is exact, not a representative.
        # child_eval is a function of child_hash, hence also of the key.
        pl.col("child_hash").first().alias("child_hash"),
        pl.col("child_eval").first().alias("child_eval"),
        pl.col("ply").first().alias("ply"),
        pl.col("ws").eq(1.0).sum().cast(pl.Int64).alias("white_wins"),
        pl.col("ws").eq(0.5).sum().cast(pl.Int64).alias("draws"),
        pl.col("ws").eq(0.0).sum().cast(pl.Int64).alias("black_wins"),
        pl.len().cast(pl.Int64).alias("total"),
    )


def _agg_ps_resum(df: pl.DataFrame) -> pl.DataFrame:
    # Re-aggregate already-aggregated ps frames (sum the counts; keep a representative epd/ply).
    return df.group_by("parent_hash", "move_san").agg(
        pl.col("parent_epd").first().alias("parent_epd"),
        pl.col("child_hash").first().alias("child_hash"),
        pl.col("child_eval").first().alias("child_eval"),
        pl.col("ply").first().alias("ply"),
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("draws").sum().alias("draws"),
        pl.col("black_wins").sum().alias("black_wins"),
        pl.col("total").sum().alias("total"),
    )


def _agg_crush(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san", "move_bucket").agg(
        pl.len().cast(pl.Int64).alias("n"),
        pl.col("wn").sum().cast(pl.Int64).alias("white_wins"),
        pl.col("bn").sum().cast(pl.Int64).alias("black_wins"),
    )


def _agg_crush_resum(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san", "move_bucket").agg(
        pl.col("n").sum().alias("n"),
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("black_wins").sum().alias("black_wins"),
    )


def _new_buf() -> dict:
    return {k: [] for k in
            ("parent_hash", "child_hash", "move_san", "parent_epd", "ply",
             "ws", "wn", "bn", "move_bucket")}


# ── terminal accounting (TERM / HORIZON) ──────────────────────────────────────
# Stage 3's opponent-node mean divides by the sum of a node's OUTGOING edges, so
# two populations are invisible to it:
#
#   TERM     the game ENDED here (resign / mate / flag / abandon). No outgoing
#            edge exists, so these games contribute nothing — and they are not
#            noise: measured reach-weighted, 7.01% of arriving mass, scoring
#            0.9015 for White against 0.5087 for the games that continue (Black
#            is to move and the game ended, so the mean deletes precisely our
#            wins). Understates winning lines by ~0.027, compounding up the tree.
#
#   HORIZON  the game was still going but the tier/--max-ply cap stopped the
#            replay. Not a termination, and valuing it like one would be wrong.
#
# Recorded per game as one row at the position where the game left the recorded
# tree. Keeping them apart is what lets the accounting identity close exactly:
#   in_mass - out_mass - other - term - horizon == 0
# and de-conflates the two things --reply-shrink's coverage c currently blurs.
TERM_ENDED, TERM_HORIZON = 0, 1

# WHY the reason is carried rather than collapsed. Measured on 2025-01 Blitz over
# the 7,510 games that end inside the ply-30 tree:
#
#   Normal (mate/resign)  86.4%   mean score of the SIDE TO MOVE  0.0953
#   Time forfeit          13.6%                                   0.0049
#   Rules infraction       0.0%                                   0.0000
#
# A flag is a fact about the CLOCK, not the position — you only ever flag on your
# own move, which is why it is even more one-sided than a resignation — yet it
# lands with the same sign and would read as "the position broke them". It is
# still counted by default, because the edge-level white_score_avg it gets
# averaged against already counts time forfeits and every other result in the
# pool; excluding flags from one half of a mean but not the other is a worse
# defect than the one it fixes. Carrying the dimension costs one small column and
# keeps the choice reversible — the extract is the ~90 h un-redoable step, so the
# alternative to carrying it is another full rebuild to change our minds.
TERM_NORMAL, TERM_FLAG, TERM_ABANDON, TERM_OTHER = 0, 1, 2, 3

_TERM_REASONS = {"Normal": TERM_NORMAL, "Time forfeit": TERM_FLAG,
                 "Abandoned": TERM_ABANDON}


def _term_reason(termination) -> int:
    return _TERM_REASONS.get(termination, TERM_OTHER)


_TERM_SCHEMA = {"position_hash": pl.Int64, "kind": pl.Int32, "reason": pl.Int32,
                "ws": pl.Float64}

# A game with no movetext at all ended before White's first move, so its result
# belongs at the start position rather than nowhere.
_START_HASH = zobrist_int64(chess.Board())


def _new_term_buf() -> dict:
    return {k: [] for k in ("position_hash", "kind", "reason", "ws")}


def _append_term(tb: dict, position_hash: int, kind: int, reason: int,
                 ws: float) -> None:
    tb["position_hash"].append(position_hash)
    tb["kind"].append(kind)
    tb["reason"].append(reason)
    tb["ws"].append(ws)


# W/D/B are recorded EMPIRICALLY and the result is never imputed from whose turn
# it is. That looks like an avoidable column — the side to move is the loser 91.2%
# of the time — but a player may resign while the OPPONENT is on move, measured at
# 7.8% of terminations (9.0% of Normal ones). Imputing "side to move lost" would
# bias every terminal node by ~9 points of score. It is also why the terminal mean
# is 0.905 for the other side rather than ~0.99.
def _agg_term(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("position_hash", "kind", "reason").agg(
        pl.col("ws").eq(1.0).sum().cast(pl.Int64).alias("white_wins"),
        pl.col("ws").eq(0.5).sum().cast(pl.Int64).alias("draws"),
        pl.col("ws").eq(0.0).sum().cast(pl.Int64).alias("black_wins"),
        pl.len().cast(pl.Int64).alias("total"),
    )


def _agg_term_resum(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("position_hash", "kind", "reason").agg(
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("draws").sum().alias("draws"),
        pl.col("black_wins").sum().alias("black_wins"),
        pl.col("total").sum().alias("total"),
    )


def classify_maxply(tiers: dict | None, w1: str | None, b1: str | None,
                    cap: int) -> int:
    """Asymmetric-depth tier for a game, keyed on its first two SAN moves.

    30/18/10-ply tiers (configurable) seeded from existing >=1800 frequency stats:
      - W1 in the main set {e4,d4,c4,Nf3} + a common Black reply (P(b1|w1) >= deep) -> deep_ply
      - W1 in main set + a mid reply (mid <= P(b1|w1) < deep)                       -> mid_ply
      - W1 an offbeat-but-common first move (P(w1) >= deep, not in main set)        -> mid_ply
      - everything else (rare first moves / rare replies)                          -> shallow_ply
    tiers=None disables pruning (full `cap`).
    """
    if tiers is None:
        return cap
    if w1 in tiers["four"]:
        if b1 is not None and b1 in tiers["deep_resp"].get(w1, ()):
            return tiers["deep_ply"]
        if b1 is not None and b1 in tiers["mid_resp"].get(w1, ()):
            return tiers["mid_ply"]
        return tiers["shallow_ply"]
    if w1 in tiers["common_white_other"]:
        return tiers["mid_ply"]
    return tiers["shallow_ply"]


def _walk_game(buf: dict, movetext, white_score, white_norm, black_norm,
               move_count, tiers, max_ply_cap, hasher=None, epd_memo=None,
               term_buf=None, reason: int = TERM_OTHER) -> bool:
    """Append one game's per-ply rows into the columnar buffer, capping depth by the
    asymmetric tier (classified from the first two SAN moves). Returns True on parse error.

    NOTE: move_bucket still uses the game's REAL move_count (full length from source),
    so the relative-crush horizon is unaffected by where we truncate extraction.

    `hasher` / `epd_memo` are the two extract-speed optimizations (2026-07 profiling:
    the position hash and board.epd() were 29% and 52% of replay time, against 19%
    for the actual parse_san+push).  Both are OPTIONAL — passing neither reproduces
    the original code path exactly, which is what _test_extract_equivalence.py
    diffs against:
      hasher    — an IncrementalZobrist reused across games (reset per game).
      epd_memo  — dict[position_hash -> EPD], owned by the caller and cleared per
                  read batch.  Sound because polyglot hashes the ep file on mere
                  pawn ADJACENCY while board.epd() uses a legal-only rule, so the
                  hash discriminates at least as finely as the EPD on every field:
                  hash -> EPD is a function (the reverse is NOT — see
                  _test_epd_memo.py).  Measured hit rate 35.1%, identical per-batch
                  and whole-file, so clearing per batch costs nothing.
    """
    if not movetext:
        if term_buf is not None:
            _append_term(term_buf, _START_HASH, TERM_ENDED, reason, white_score)
        return False
    toks = list(iter_san_moves(movetext))
    if not toks:
        if term_buf is not None:
            _append_term(term_buf, _START_HASH, TERM_ENDED, reason, white_score)
        return False
    w1 = toks[0]
    b1 = toks[1] if len(toks) > 1 else None
    maxply = min(classify_maxply(tiers, w1, b1, max_ply_cap), max_ply_cap, len(toks))
    decisive = white_norm or black_norm
    board = chess.Board()
    if hasher is None:
        get_hash = lambda: zobrist_int64(board)
        push = board.push
    else:
        hasher.reset(board)
        get_hash = lambda: hasher.current(board)
        push = lambda mv: hasher.push_move(board, mv)
    # child_hash comes free from the replay: the position AFTER the move at ply p
    # is the position BEFORE the move at ply p+1. Carrying `ph` across iterations
    # costs exactly one extra get_hash() per GAME (not per ply) and removes the
    # merge's single-threaded python-chess re-derivation tail entirely. It is also
    # what gives below-floor edges a child hash — without one they cannot be joined
    # to the eval DB for the other-moves bucket.
    ph = get_hash()
    for ply in range(1, maxply + 1):
        san = toks[ply - 1]
        if epd_memo is None:
            epd = board.epd()
        else:
            epd = epd_memo.get(ph)
            if epd is None:
                epd = board.epd()
                epd_memo[ph] = epd
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            return True
        if decisive and move_count is not None:
            b = move_count - ((ply - 1) // 2)
            b = 1 if b < 1 else (60 if b > 60 else b)
        else:
            b = 0
        push(move)
        ch = get_hash()
        buf["parent_hash"].append(ph)
        buf["child_hash"].append(ch)
        buf["move_san"].append(san)
        buf["parent_epd"].append(epd)
        buf["ply"].append(ply)
        buf["ws"].append(white_score)
        buf["wn"].append(1 if white_norm else 0)
        buf["bn"].append(1 if black_norm else 0)
        buf["move_bucket"].append(b)
        ph = ch
    # `ph` is now the position after the last recorded ply. maxply was clamped to
    # min(tier, cap, len(toks)), so it equals len(toks) exactly when nothing
    # truncated us — i.e. the game really ended there rather than running past
    # the horizon. A parse failure returns above without recording either, which
    # is right: we do not know where that game went.
    if term_buf is not None:
        _append_term(term_buf, ph,
                     TERM_ENDED if maxply >= len(toks) else TERM_HORIZON,
                     reason, white_score)
    return False


_BUF_SCHEMA = {
    "parent_hash": pl.Int64, "child_hash": pl.Int64, "move_san": pl.Utf8,
    "parent_epd": pl.Utf8, "ply": pl.Int32, "ws": pl.Float64,
    "wn": pl.Int32, "bn": pl.Int32, "move_bucket": pl.Int32,
}

# Schema-identical to the crush histogram, so Stage 3 consumes a winpos partial
# with no change at all — --crush-db just points at one.
_WINPOS_SCHEMA = {
    "parent_hash": pl.Int64, "move_san": pl.Utf8, "move_bucket": pl.Int32,
    "n": pl.Int64, "white_wins": pl.Int64, "black_wins": pl.Int64,
}


def extract_file(src_file: Path, ps_out: Path, crush_out: Path | None,
                 min_elo: int, max_ply: int, tiers: dict | None,
                 limit_games: int | None = None, optimize: bool = True,
                 term_out: Path | None = None,
                 winpos_out: dict[int, Path] | None = None,
                 with_child_eval: bool = True) -> dict:
    """Fused per-file extractor: filter -> replay once -> pre-aggregated partials.

    `optimize=False` disables the incremental hasher + EPD memo, restoring the
    pre-2026-07 replay path byte-for-byte. Only _test_extract_equivalence.py and
    _bench_extract.py pass it; production always runs optimized.

    `crush_out` writes the RETIRED resignation-proxy histogram, and is None unless
    --crush-hist is passed. It used to be written unconditionally so the retired
    metric could be re-merged without a re-extract — a cheap hedge when the partial
    dir held ps + crush alone. Fusing winpos in tripled that dir, and the 2026-08
    rebuild measured the hedge at ~303 GB against ~280 GB of free-space margin,
    which is the whole reason it is now gated. The hedge is also redundant: the
    thing it insures against is the resignation-proxy win event, and the winpos
    histograms at 200/300/500cp supersede it. Gate the WRITE and the caller's
    skip-gate together or every completed chunk re-runs (see main).

    `term_out` writes the TERM/HORIZON terminal-accounting partial. Optional so the
    equivalence/bench harnesses can call the replay without it; production always
    passes it (see _worker).

    `winpos_out` maps threshold_cp -> partial path. Supplying it FUSES the winpos
    histogram into this replay, replacing build_crush_winpos_phase2.py's second
    full pass over the same files (measured 47 h, peak 76 GB). The events are
    computed by winpos_fused, which is held equal to build_crush_winpos.winpos_sql
    by _test_winpos_fused.py — that query remains the definition of the event.
    Rows are emitted for EVERY edge, not just pool survivors: the `keys` join the
    SQL performs needs a global min_games decision that does not exist yet while a
    single file is being extracted, so it moves to the merge's semi-join.

    `with_child_eval` populates the child_eval column the merge needs for the
    other-moves bucket's aggregate evaluation. It is INDEPENDENT of winpos on
    purpose: both read the same mmap'd arrays, and tying them together meant that
    turning winpos off — the obvious lever if a threshold's partials are too big —
    silently emptied the bucket's eval (other_eval_mean all NULL, other_eval_cov
    0), so Stage 3 fell back to the empirical score with nothing in the logs. Only
    the equivalence/bench harnesses pass False, since they replay without the
    eval arrays present.
    """
    want_crush = crush_out is not None
    want_term = term_out is not None
    want_wp = bool(winpos_out)
    want_ev = want_wp or with_child_eval
    if (ps_out.exists()
            and (not want_crush or crush_out.exists())
            and (not want_term or term_out.exists())
            and (not want_wp or all(p.exists() for p in winpos_out.values()))):
        return {"file": src_file.name, "skipped": True, "games": 0, "sec": 0.0}

    t0 = time.time()
    n_games = n_kept = n_failed = 0
    ps_parts: list[pl.DataFrame] = []
    crush_parts: list[pl.DataFrame] = []
    term_parts: list[pl.DataFrame] = []
    wp_parts: dict[int, list[pl.DataFrame]] = {t: [] for t in (winpos_out or {})}
    buf = _new_buf()
    term_buf = _new_term_buf() if want_term else None
    # Per-game index ranges into buf + the decisive-end facts winpos needs. Only
    # collected when fusing, since they cost a tuple per game.
    spans: list[tuple[int, int]] = []
    facts: list[tuple[int, int, object]] = []
    mm_h = mm_e = None
    if want_ev:
        mm_h, mm_e = open_eval_arrays()
    # Reused across every game in the file; the memo is cleared per read batch.
    hasher = IncrementalZobrist(chess.Board()) if optimize else None
    epd_memo: dict[int, str] | None = {} if optimize else None

    def flush_batch():
        if term_buf is not None and term_buf["position_hash"]:
            term_parts.append(_agg_term(pl.DataFrame(term_buf, schema=_TERM_SCHEMA)))
            for v in term_buf.values():
                v.clear()
        if not buf["parent_hash"]:
            spans.clear()
            facts.clear()
            return
        df = pl.DataFrame(buf, schema=_BUF_SCHEMA)
        # child_eval rides along on the SAME arrays winpos already has open. It is
        # what lets the merge compute the other-moves bucket's aggregate evaluation
        # without a separate join over ~2.25B below-floor edges. Stored NULL where
        # the eval DB has no entry, so SQL aggregates skip it instead of averaging
        # in a sentinel.
        if want_ev:
            cev = pl.Series("child_eval",
                            lookup_evals(np.asarray(buf["child_hash"], dtype=np.int64),
                                         mm_h, mm_e), dtype=pl.Int32)
            df = df.with_columns(
                pl.when(cev == int(_EVAL_MISSING)).then(None)
                  .otherwise(cev).alias("child_eval"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("child_eval"))
        ps_parts.append(_agg_ps(df))
        if want_crush:
            crush_parts.append(_agg_crush(df))
        if want_wp and spans:
            # ONE vectorised lookup for the whole batch. Per-ply binary searches
            # over a 3.2 GB array would be cache-hostile; lookup_evals sorts the
            # queries so the searches sweep instead of jumping (see eval_arrays).
            # The population is buf["parent_hash"] — the position BEFORE each
            # move — which is exactly winpos_sql's `pm.parent_hash`.
            ev = lookup_evals(np.asarray(buf["parent_hash"], dtype=np.int64),
                              mm_h, mm_e).tolist()
            ph, sa, pl_ = buf["parent_hash"], buf["move_san"], buf["ply"]
            for thr in wp_parts:
                raw = winpos_batch(ph, sa, pl_, ev, spans, facts, thr)
                if raw["parent_hash"]:
                    wp_parts[thr].append(_agg_crush_resum(pl.DataFrame(
                        raw, schema=_WINPOS_SCHEMA)))
        spans.clear()
        facts.clear()
        for v in buf.values():
            v.clear()

    def compact():
        nonlocal ps_parts, crush_parts, term_parts
        if len(ps_parts) > 1:
            ps_parts = [_agg_ps_resum(pl.concat(ps_parts))]
        if len(crush_parts) > 1:
            crush_parts = [_agg_crush_resum(pl.concat(crush_parts))]
        if len(term_parts) > 1:
            term_parts = [_agg_term_resum(pl.concat(term_parts))]
        for thr, parts in wp_parts.items():
            if len(parts) > 1:
                wp_parts[thr] = [_agg_crush_resum(pl.concat(parts))]

    pf = pq.ParquetFile(src_file)
    batch_i = 0
    done = False
    for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES, columns=SRC_COLUMNS):
        for rec in batch.to_pylist():
            if limit_games is not None and n_games >= limit_games:
                done = True
                break
            n_games += 1
            me = rec["mean_elo"]
            if me is None or me < min_elo:
                continue
            ws = rec["white_score"]
            if ws is None:
                continue
            n_kept += 1
            term = rec["termination"]
            normal = term == "Normal"
            white_norm = normal and ws == 1.0
            black_norm = normal and ws == 0.0
            g0 = len(buf["parent_hash"]) if want_wp else 0
            if _walk_game(buf, rec["movetext"], ws, white_norm, black_norm,
                          rec["move_count"], tiers, max_ply, hasher, epd_memo,
                          term_buf, _term_reason(term)):
                n_failed += 1
            if want_wp:
                g1 = len(buf["parent_hash"])
                if g1 > g0:
                    # A game that failed SAN parsing keeps the plies it managed,
                    # and so does the second-pass replay this replaces — the
                    # populations must match, so partial games are NOT dropped.
                    spans.append((g0, g1))
                    facts.append((1 if white_norm else 0,
                                  1 if black_norm else 0, rec["move_count"]))
        flush_batch()
        # Bound the memo: the 35.1% hit rate is entirely intra-batch, so clearing
        # here costs no hits and keeps the dict at ~one batch's distinct positions.
        if epd_memo is not None:
            epd_memo.clear()
        batch_i += 1
        if batch_i % COMPACT_EVERY == 0:
            compact()
        if done:
            break

    compact()
    ps_df = ps_parts[0] if ps_parts else pl.DataFrame(schema={
        "parent_hash": pl.Int64, "move_san": pl.Utf8, "parent_epd": pl.Utf8,
        "child_hash": pl.Int64, "child_eval": pl.Int32, "ply": pl.Int32,
        "white_wins": pl.Int64, "draws": pl.Int64, "black_wins": pl.Int64,
        "total": pl.Int64})
    crush_df = crush_parts[0] if crush_parts else pl.DataFrame(schema={
        "parent_hash": pl.Int64, "move_san": pl.Utf8, "move_bucket": pl.Int32,
        "n": pl.Int64, "white_wins": pl.Int64, "black_wins": pl.Int64})
    term_df = term_parts[0] if term_parts else pl.DataFrame(schema={
        "position_hash": pl.Int64, "kind": pl.Int32, "reason": pl.Int32,
        "white_wins": pl.Int64, "draws": pl.Int64, "black_wins": pl.Int64,
        "total": pl.Int64})

    ps_out.parent.mkdir(parents=True, exist_ok=True)
    ps_tmp = ps_out.with_suffix(".parquet.tmp")
    ps_df.write_parquet(ps_tmp, compression="zstd")
    if want_crush:
        cr_tmp = crush_out.with_suffix(".parquet.tmp")
        crush_df.write_parquet(cr_tmp, compression="zstd")
    if want_term:
        tm_tmp = term_out.with_suffix(".parquet.tmp")
        term_df.write_parquet(tm_tmp, compression="zstd")
        tm_tmp.replace(term_out)
    wp_rows = 0
    for thr, out_path in (winpos_out or {}).items():
        parts = wp_parts[thr]
        wdf = parts[0] if parts else pl.DataFrame(schema=_WINPOS_SCHEMA)
        wp_rows += wdf.height
        wp_tmp = out_path.with_suffix(".parquet.tmp")
        wdf.write_parquet(wp_tmp, compression="zstd")
        wp_tmp.replace(out_path)
    # ps LAST: the caller's skip-gate is conjunctive, so the kind renamed last is
    # the one whose absence reopens the gate after an interruption. Previously
    # crush held that slot; with it gated off, ps has to take it or an interrupt
    # between the two renames would leave a chunk that passes the gate half-built.
    if want_crush:
        cr_tmp.replace(crush_out)
    ps_tmp.replace(ps_out)
    return {"file": src_file.name, "skipped": False, "games": n_games, "kept": n_kept,
            "failed": n_failed, "ps_rows": ps_df.height, "crush_rows": crush_df.height,
            "term_rows": term_df.height, "winpos_rows": wp_rows,
            "sec": time.time() - t0}


# Per-worker globals set once by the pool initializer (avoids re-pickling the tier
# tables and config on every task).
_W: dict = {}


def _init_worker(min_elo: int, max_ply: int, tiers: dict | None) -> None:
    _W["min_elo"] = min_elo
    _W["max_ply"] = max_ply
    _W["tiers"] = tiers


def _worker(task: tuple) -> dict:
    src_str, ps_str, cr_str, tm_str, wp_map, limit = task
    return extract_file(Path(src_str), Path(ps_str),
                        Path(cr_str) if cr_str else None,
                        _W["min_elo"], _W["max_ply"], _W["tiers"], limit,
                        term_out=Path(tm_str),
                        winpos_out={int(t): Path(p) for t, p in wp_map.items()})


# ── depth-tier seeding (from existing >=1800 frequency stats) ───────────────────

def build_depth_tiers(seed_path: Path, four: list[str], deep_thresh: float,
                      mid_thresh: float, deep_ply: int, mid_ply: int,
                      shallow_ply: int) -> dict:
    """Derive the asymmetric-depth tier tables from an existing position_stats file.

    P(w1) from the start position; P(b1|w1) using the exact denominator = the
    start->w1 edge total (= number of games that played w1).
    """
    st = pl.read_parquet(seed_path, columns=["parent_hash", "move_san", "total"])
    start = zobrist_int64(chess.Board())
    sd = st.filter(pl.col("parent_hash") == start)
    tot = sd["total"].sum()
    wtot = {r["move_san"]: r["total"] for r in sd.iter_rows(named=True)}
    four_set = set(four)
    common_white_other = {m for m, t in wtot.items()
                          if m not in four_set and t / tot >= deep_thresh}
    deep_resp: dict[str, set] = {}
    mid_resp: dict[str, set] = {}
    for w1 in four:
        denom = wtot.get(w1, 0)
        deep_resp[w1], mid_resp[w1] = set(), set()
        if not denom:
            continue
        b = chess.Board()
        b.push_san(w1)
        h = zobrist_int64(b)
        for r in st.filter(pl.col("parent_hash") == h).iter_rows(named=True):
            f = r["total"] / denom
            if f >= deep_thresh:
                deep_resp[w1].add(r["move_san"])
            elif f >= mid_thresh:
                mid_resp[w1].add(r["move_san"])
    return {"four": four_set, "deep_resp": deep_resp, "mid_resp": mid_resp,
            "common_white_other": common_white_other,
            "deep_ply": deep_ply, "mid_ply": mid_ply, "shallow_ply": shallow_ply}


# ── discovery ─────────────────────────────────────────────────────────────────

def discover_source_files(start_year: int, end_year: int, months: list[int] | None,
                          events: list[str]) -> list[tuple[Path, int, int, str]]:
    out = []
    for year in range(start_year, end_year + 1):
        ydir = SOURCE_ROOT / f"year={year}"
        if not ydir.is_dir():
            continue
        for mdir in sorted(ydir.glob("month=*")):
            month = int(mdir.name.split("=")[1])
            if months and month not in months:
                continue
            if (year, month) in SKIP_PARTITIONS:
                continue
            for ev in events:
                edir = mdir / f"event={ev}"
                if not edir.is_dir():
                    continue
                for f in sorted(edir.glob("*.parquet")):
                    out.append((f, year, month, ev))
    return out


def partial_name(src_file: Path, year: int, month: int, event: str, kind: str) -> str:
    return f"year={year}_month={month}_event={event}_{src_file.stem}.{kind}.parquet"


# ── final merges (DuckDB, single pass) ─────────────────────────────────────────

def _sql_path(p) -> str:
    """Forward-slashed path string for embedding in DuckDB SQL (Windows backslashes
    break the SQL string literal). One idiom for every merge-phase query below."""
    return str(p).replace("\\", "/")


def _duck(threads: int, mem: str, tmp: Path):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}';")
    con.execute(f"SET temp_directory='{_sql_path(tmp)}';")
    con.execute(f"SET threads={threads};")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    return con


_MONTH_RE = re.compile(r"year=(\d+)_month=(\d+)_event=")

# The final cross-month GROUP BYs are two-phase external aggregations:
#   Phase A: stream-partition every monthly file into N key-hash buckets
#            (COPY ... PARTITION_BY, no GROUP BY -> near-zero memory).
#   Phase B: GROUP BY each bucket's slices in RAM (state ~ total/N).
# Rationale (measured, July 2026): ~6B unique (parent_hash, move_san) keys ->
# ~700 GB aggregate state. Monolithic GROUP BYs OOMed at 90/100 GB limits, and
# direct 8-bucket passes OOMed at 60 GB with ZERO spill written — DuckDB 1.5's
# out-of-core aggregation never engaged for this query shape, and even working
# spill would push ~2x state through temp. Two-phase does 2 passes total instead
# of N re-scans. All rows of a key share a parent_hash, so every key lands wholly
# in one bucket and the min_games HAVING remains a true final-pass filter.
N_MERGE_BUCKETS = 32


def _bucket_expr(col: str) -> str:
    # Arithmetic bucketing (not hash()) so resumed runs assign identical buckets
    # regardless of DuckDB version. Zobrist hashes are uniform in the low bits;
    # the double-modulo folds negative int64 values into [0, N).
    return f"(({col} % {N_MERGE_BUCKETS}) + {N_MERGE_BUCKETS}) % {N_MERGE_BUCKETS}"


def _bucket_has_data(part_dir: Path, i: int) -> bool:
    """Does Phase A's partitioning actually contain bucket i?

    COPY ... PARTITION_BY only creates directories for values that occur. At the
    full pool's scale every one of the 32 buckets is populated, so the read glob
    below never came up empty — but a small run (one month, a smoke test) can
    easily leave buckets unrepresented, and DuckDB raises on a glob that matches
    no file rather than returning zero rows.
    """
    return any(part_dir.glob(f"month=*/bkt={i}"))


def _write_empty(path: Path, schema: dict) -> None:
    tmp = path.with_suffix(".parquet.tmp")
    pl.DataFrame(schema=schema).write_parquet(tmp, compression="zstd")
    tmp.replace(path)


_PS_BUCKET_SCHEMA = {
    "parent_hash": pl.Int64, "move_san": pl.Utf8, "parent_epd": pl.Utf8,
    "child_hash": pl.Int64, "ply": pl.Int32, "white_wins": pl.Int64,
    "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64,
}
_OTHER_BUCKET_SCHEMA = {
    "position_hash": pl.Int64, "other_total": pl.Int64,
    "other_white_wins": pl.Int64, "other_draws": pl.Int64,
    "other_black_wins": pl.Int64, "other_edges": pl.Int32,
    "other_eval_mean": pl.Float64, "other_eval_min": pl.Float64,
    "other_eval_max": pl.Float64, "other_eval_cov": pl.Float64,
}
_HIST_BUCKET_SCHEMA = {
    "parent_hash": pl.Int64, "move_san": pl.Utf8, "move_bucket": pl.Int32,
    "n": pl.Int64, "white_wins": pl.Int64, "black_wins": pl.Int64,
    "event": pl.Utf8, "elo_band": pl.Int64,
}


def _run_ps_bucket(task: tuple) -> tuple:
    """One hash bucket's ps GROUP BY, written out TWICE: survivors and the rest.

    The aggregation over every (parent_hash, move_san) key was always being built
    in full — `HAVING SUM(total) >= min_games` only decided what got WRITTEN. So
    the below-floor tail (~82x the surviving edge count, carrying ~24% of all edge
    mass) was materialised and then thrown away on every previous rebuild. Here it
    is collapsed to one row per position instead, which is what makes the
    other-moves bucket nearly free: a single extra COPY off a temp table, not a
    second pass.

    The eval aggregates come from child_eval, carried out of the extract, so no
    join against the 400M-row eval DB happens here. cp is converted to expected
    score per EDGE before averaging — a mean of sigmoids is not the sigmoid of a
    mean, and the difference is largest exactly where the tail is interesting.
    """
    (sql_group, surv_sql, other_sql, surv_tmp, surv_out, oth_tmp, oth_out,
     threads, mem, tmp_str, label) = task
    t0 = time.time()
    for p in (surv_tmp, oth_tmp):
        Path(p).unlink(missing_ok=True)
    con = _duck(threads, mem, Path(tmp_str))
    try:
        con.execute(sql_group)
        con.execute(surv_sql)
        con.execute(other_sql)
    finally:
        con.close()
    Path(surv_tmp).replace(Path(surv_out))
    Path(oth_tmp).replace(Path(oth_out))
    size = Path(surv_out).stat().st_size + Path(oth_out).stat().st_size
    return label, size, time.time() - t0


def _run_copy_query(task: tuple) -> tuple:
    """Run ONE COPY query in a fresh process, then exit (same allocator-isolation
    rationale as _consolidate_one_month). Atomic tmp->rename: output exists only
    when complete. Targets may be files (single COPY) or directories
    (COPY ... PARTITION_BY)."""
    sql, out_tmp_str, out_str, threads, mem, tmp_str, label = task
    out_tmp, out = Path(out_tmp_str), Path(out_str)
    if out_tmp.is_dir():
        shutil.rmtree(out_tmp)
    else:
        out_tmp.unlink(missing_ok=True)
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_str))
    try:
        con.execute(sql)
    finally:
        con.close()
    out_tmp.replace(out)
    size = (sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
            if out.is_dir() else out.stat().st_size)
    return label, size, time.time() - t0


def _partition_by_bucket(src_dir: Path, kind: str, part_dir: Path,
                         threads: int, mem: str, tmp_dir: Path) -> None:
    """Phase A: stream-split every monthly <kind> file into N_MERGE_BUCKETS
    hive partitions (part_dir/month=Y_M/bkt=N/*.parquet). Pure COPY, no GROUP BY,
    so memory stays at partition-writer buffers. Skip-gated per month on the
    final month dir; in-flight writes go to _tmp_month=* (never matched by the
    Phase-B glob) and are cleaned on restart."""
    part_dir.mkdir(parents=True, exist_ok=True)
    for stale in part_dir.glob("_tmp_month=*"):
        shutil.rmtree(stale, ignore_errors=True)
    months = {}
    for f in src_dir.glob(f"*.{kind}.parquet"):
        m = re.match(r"year=(\d+)_month=(\d+)", f.name)
        if m:
            months[(int(m.group(1)), int(m.group(2)))] = f
    tasks = []
    for (y, mo), f in sorted(months.items()):
        out = part_dir / f"month={y}_{mo}"
        if out.exists():
            continue
        out_tmp = part_dir / f"_tmp_month={y}_{mo}"
        sql = f"""
            COPY (
                SELECT *, {_bucket_expr("parent_hash")} AS bkt
                FROM read_parquet('{_sql_path(f)}')
            ) TO '{_sql_path(out_tmp)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
        """
        tasks.append((sql, str(out_tmp), str(out), threads, mem, str(tmp_dir),
                      f"partition {kind} {y}/{mo}"))
    print(f"  Phase A ({kind}): {len(months)} months, {len(tasks)} to partition",
          flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"    {label}: {size/1e6:.0f} MB ({secs:.0f}s)", flush=True)


def _consolidate_one_month(task: tuple) -> tuple:
    """Run ONE month's SUM-only GROUP BY in a fresh process, then exit.

    Per-month process isolation is REQUIRED on Windows. Running every month's
    DuckDB GROUP BY in a single long-lived process degrades throughput ~3x over a
    handful of same-size months (measured: 785s -> 2115s across four flat ~5.5 GB
    months, zero spill) — native-allocator fragmentation from repeated multi-GB
    buffer alloc/free, the same gremlin CLAUDE.md documents as 0xC0000005, here
    surfacing as slowdown rather than a segfault. A fresh process per month resets
    it, holding throughput flat. The DuckDB query is byte-identical to the in-line
    version it replaced.
    """
    kind, grp, sums, in_files, out_str, threads, mem, tmp_str, label = task
    out = Path(out_str)
    out_tmp = out.with_suffix(".parquet.tmp")
    in_list = ", ".join(f"'{_sql_path(Path(p))}'" for p in in_files)
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_str))
    try:
        con.execute(f"""
            COPY (
                SELECT {grp}, {sums}
                FROM read_parquet([{in_list}])
                GROUP BY {grp}
            ) TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out)
    return label, len(in_files), out.stat().st_size, time.time() - t0


def consolidate_monthly(partial_dir: Path, threads: int, mem: str,
                        tmp_base: Path | None = None,
                        kinds: tuple[str, ...] = ("ps", "crush")) -> Path:
    """SUM-only per-(year,month) consolidation of the per-file partials.

    NO min_games filter here — pure summation, so a position split across files/
    events/months can still reach the threshold at the FINAL merge. Collapses ~1800
    file-partials to ~80 monthly partials (within-month dedup), making the final
    GROUP BY tractable and crash-safe. Resumable: skip-gated per month, atomic write.
    Returns the monthly directory.

    `kinds` selects which partial families to consolidate. The default merge path
    passes ("ps",) only — the resignation-proxy crush histogram is retired (see
    --crush-hist), and consolidating its partials is the largest avoidable cost in
    the merge.
    """
    mdir = partial_dir / "_monthly"
    mdir.mkdir(parents=True, exist_ok=True)
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # winpos<T> partials are schema-identical to crush by construction, so they
    # reuse its grouping rather than getting a near-duplicate spec that could drift.
    _HIST = ("parent_hash, move_san, move_bucket",
             "SUM(n)::BIGINT AS n, SUM(white_wins)::BIGINT AS white_wins, "
             "SUM(black_wins)::BIGINT AS black_wins")
    specs = {
        "ps": ("parent_hash, move_san",
               "any_value(parent_epd) AS parent_epd, any_value(child_hash) AS child_hash, "
               "any_value(child_eval) AS child_eval, any_value(ply) AS ply, "
               "SUM(white_wins)::BIGINT AS white_wins, SUM(draws)::BIGINT AS draws, "
               "SUM(black_wins)::BIGINT AS black_wins, SUM(total)::BIGINT AS total"),
        "crush": _HIST,
        "term": ("position_hash, kind, reason",
                 "SUM(white_wins)::BIGINT AS white_wins, SUM(draws)::BIGINT AS draws, "
                 "SUM(black_wins)::BIGINT AS black_wins, SUM(total)::BIGINT AS total"),
    }
    for k in kinds:
        if k.startswith("winpos"):
            specs.setdefault(k, _HIST)
    for kind, (grp, sums) in [(k, specs[k]) for k in kinds if k in specs]:
        months: dict[tuple[int, int], list[Path]] = {}
        for f in partial_dir.glob(f"*.{kind}.parquet"):
            m = _MONTH_RE.search(f.name)
            if m:
                months.setdefault((int(m.group(1)), int(m.group(2))), []).append(f)
        todo = [(ym, fs) for ym, fs in sorted(months.items())
                if not (mdir / f"year={ym[0]}_month={ym[1]}.{kind}.parquet").exists()]
        print(f"  consolidate {kind}: {len(months)} months, {len(todo)} to build", flush=True)
        if not todo:
            continue
        # Per-month process isolation: each GROUP BY runs in a worker that is recycled
        # after one task (max_tasks_per_child=1), resetting the native-allocator
        # fragmentation that otherwise degrades throughput ~3x over a few months.
        # max_workers=1 keeps months sequential — each may use the full --mem budget.
        tasks = [
            (kind, grp, sums, [str(p) for p in files],
             str(mdir / f"year={y}_month={mo}.{kind}.parquet"),
             threads, mem, str(tmp_dir), f"{y}/{mo}")
            for (y, mo), files in todo
        ]
        with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
            for fut in as_completed([ex.submit(_consolidate_one_month, t) for t in tasks]):
                label, nfiles, size, secs = fut.result()
                print(f"    {label} {kind}: {nfiles} files -> "
                      f"{size/1e6:.0f} MB ({secs:.0f}s)", flush=True)
    return mdir


def merge_aux_stats(partial_dir: Path, other_dir: Path, ps_path: Path,
                    out_path: Path, threads: int, mem: str,
                    tmp_base: Path | None = None) -> None:
    """The Stage-3 sidecar: per position, the mass its outgoing edges cannot see.

    Three populations, kept apart because they need different treatment:

      term_*     the game ENDED here. The result is a fact. Split by reason so a
                 TIME FORFEIT (a statement about the clock) stays separable from
                 a mate/resignation (a statement about the position) — they carry
                 the same sign, so a blended term cannot be un-blended later
                 without another 90 h extract.
      horizon_*  the game was still going when the ply cap stopped the replay.
                 NOT split by reason: how such a game eventually ended describes
                 a position far outside our tree, not this one.
      other_*    replies below the pool's per-edge floor, collapsed to one row.

    Written as a SIDECAR rather than as sentinel rows inside position_stats
    because sixteen modules read that file by (parent_hash, move_san) and a row
    whose move_san is "#TERM" would surface as a fake move in every one of them.

    Scoped to positions that are a PARENT in position_stats. A position with only
    below-floor edges, or a leaf where a game happened to end, is not a node in
    Stage 3's graph, so an aux row for it could never be read — and including
    them would blow the table up far past the pool's position count.
    """
    if out_path.exists():
        print(f"SKIP aux merge: {out_path.name} exists")
        return
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    mdir = partial_dir / "_monthly"
    t0 = time.time()

    def kd(kind: int, reason: str, col: str, alias: str) -> str:
        return (f"SUM(CASE WHEN kind = {kind}{reason} THEN {col} ELSE 0 END)"
                f"::BIGINT AS {alias}")

    groups = [("term_normal", TERM_ENDED, f" AND reason = {TERM_NORMAL}"),
              ("term_flag",   TERM_ENDED, f" AND reason = {TERM_FLAG}"),
              ("term_other",  TERM_ENDED,
               f" AND reason NOT IN ({TERM_NORMAL}, {TERM_FLAG})"),
              ("horizon",     TERM_HORIZON, "")]
    sums = ", ".join(
        kd(k, r, c, f"{name}_{a}")
        for name, k, r in groups
        for c, a in (("total", "total"), ("white_wins", "white_wins"),
                     ("draws", "draws"), ("black_wins", "black_wins")))
    zeros = ", ".join(f"0::BIGINT AS {name}_{a}" for name, _, _ in groups
                      for a in ("total", "white_wins", "draws", "black_wins"))
    tcols = [f"{name}_{a}" for name, _, _ in groups
             for a in ("total", "white_wins", "draws", "black_wins")]
    ocols = ["other_total", "other_white_wins", "other_draws", "other_black_wins",
             "other_edges", "other_eval_mean", "other_eval_min", "other_eval_max",
             "other_eval_cov"]

    has_term = any(mdir.glob("*.term.parquet"))
    term_cte = (f"""SELECT position_hash, {sums}
                    FROM read_parquet('{_sql_path(mdir)}/*.term.parquet')
                    GROUP BY position_hash"""
                if has_term else
                f"SELECT NULL::BIGINT AS position_hash, {zeros} WHERE FALSE")
    out_tmp = out_path.with_suffix(".parquet.tmp")
    con = _duck(threads, mem, tmp_dir)
    try:
        con.execute(f"""
            COPY (
                WITH keys AS (
                    SELECT DISTINCT parent_hash AS position_hash
                    FROM read_parquet('{_sql_path(ps_path)}')
                ),
                t AS ({term_cte}),
                o AS (SELECT * FROM
                      read_parquet('{_sql_path(other_dir)}/bucket_*.parquet'))
                SELECT k.position_hash,
                       {', '.join(f'COALESCE(t.{c}, 0) AS {c}' for c in tcols)},
                       COALESCE(o.other_total, 0)      AS other_total,
                       COALESCE(o.other_white_wins, 0) AS other_white_wins,
                       COALESCE(o.other_draws, 0)      AS other_draws,
                       COALESCE(o.other_black_wins, 0) AS other_black_wins,
                       COALESCE(o.other_edges, 0)      AS other_edges,
                       o.other_eval_mean, o.other_eval_min, o.other_eval_max,
                       COALESCE(o.other_eval_cov, 0.0) AS other_eval_cov
                FROM keys k
                LEFT JOIN t ON t.position_hash = k.position_hash
                LEFT JOIN o ON o.position_hash = k.position_hash
            ) TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out_path)
    n = pq.ParquetFile(out_path).metadata.num_rows
    print(f"  aux-stats: {out_path.name} ({out_path.stat().st_size/1e6:.0f} MB, "
          f"{n:,} positions) in {(time.time()-t0)/60:.1f} min", flush=True)
    # Consumed. Deleted only now, so a crash before this point resumes from the
    # buckets instead of re-running the whole Phase-B aggregation.
    shutil.rmtree(other_dir, ignore_errors=True)


def merge_position_stats(src_dir: Path, partial_dir: Path, out_path: Path, min_games: int,
                         threads: int, mem: str, tmp_base: Path | None = None) -> None:
    if out_path.exists():
        print(f"SKIP ps merge: {out_path.name} exists")
        return
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    # Imported lazily and NOT redefined: the cp -> expected-score curve must be the
    # same one Stage 3 blends evals with, or the bucket's value sits on a different
    # scale from every other eval in the objective.
    from stage3_backwards_induction import LICHESS_CP_SCALE
    # Durable GROUP BY checkpoint. The HAVING-filtered aggregation is the expensive part;
    # the single-threaded child_hash loop that follows can run for many minutes and is a
    # crash risk. Write the aggregation to a temp then atomically rename to the checkpoint,
    # so it exists ONLY when complete — a crash in the child_hash loop then resumes from
    # here instead of re-running the whole multi-billion-row GROUP BY.
    agg_ckpt = out_path.with_name(out_path.stem + "_agg.parquet")
    bdir = out_path.with_name(out_path.stem + "_agg_buckets")
    odir = out_path.with_name(out_path.stem + "_other_buckets")
    pdir = out_path.with_name(out_path.stem + "_agg_parts")
    if agg_ckpt.exists():
        print(f"  reusing ps GROUP BY checkpoint {agg_ckpt.name}", flush=True)
    else:
        agg_write = out_path.with_name(out_path.stem + "_agg.parquet.tmp")
        bdir.mkdir(parents=True, exist_ok=True)
        odir.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        _partition_by_bucket(src_dir, "ps", pdir, threads, mem, tmp_dir)
        tasks = []
        for i in range(N_MERGE_BUCKETS):
            bout = bdir / f"bucket_{i}.parquet"
            oout = odir / f"bucket_{i}.parquet"
            if bout.exists() and oout.exists():
                continue
            if not _bucket_has_data(pdir, i):
                _write_empty(bout, _PS_BUCKET_SCHEMA)
                _write_empty(oout, _OTHER_BUCKET_SCHEMA)
                continue
            btmp = bout.with_suffix(".parquet.tmp")
            otmp = oout.with_suffix(".parquet.tmp")
            sql_group = f"""
                CREATE TEMP TABLE g AS
                SELECT parent_hash, move_san,
                       any_value(parent_epd)  AS parent_epd,
                       any_value(child_hash)  AS child_hash,
                       any_value(child_eval)  AS child_eval,
                       any_value(ply)         AS ply,
                       SUM(white_wins)::BIGINT AS white_wins,
                       SUM(draws)::BIGINT      AS draws,
                       SUM(black_wins)::BIGINT AS black_wins,
                       SUM(total)::BIGINT      AS total
                FROM read_parquet('{_sql_path(pdir)}/month=*/bkt={i}/*.parquet')
                GROUP BY parent_hash, move_san
            """
            surv_sql = f"""
                COPY (SELECT parent_hash, move_san, parent_epd, child_hash, ply,
                             white_wins, draws, black_wins, total
                      FROM g WHERE total >= {int(min_games)})
                TO '{_sql_path(btmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            # Below-floor edges, collapsed to one row per POSITION. es is the
            # per-edge expected score; averaging cp and converting afterwards
            # would be a different (and wrong) quantity.
            other_sql = f"""
                COPY (
                    SELECT parent_hash AS position_hash,
                           SUM(total)::BIGINT       AS other_total,
                           SUM(white_wins)::BIGINT  AS other_white_wins,
                           SUM(draws)::BIGINT       AS other_draws,
                           SUM(black_wins)::BIGINT  AS other_black_wins,
                           COUNT(*)::INTEGER        AS other_edges,
                           SUM(CASE WHEN child_eval IS NULL THEN 0
                                    ELSE total * (1.0 / (1.0 + exp(-{LICHESS_CP_SCALE}
                                                                   * child_eval))) END)
                             / NULLIF(SUM(CASE WHEN child_eval IS NULL THEN 0
                                               ELSE total END), 0)  AS other_eval_mean,
                           MIN(1.0 / (1.0 + exp(-{LICHESS_CP_SCALE} * child_eval)))
                                                    AS other_eval_min,
                           MAX(1.0 / (1.0 + exp(-{LICHESS_CP_SCALE} * child_eval)))
                                                    AS other_eval_max,
                           SUM(CASE WHEN child_eval IS NULL THEN 0 ELSE total END)
                             / NULLIF(SUM(total), 0)::DOUBLE        AS other_eval_cov
                    FROM g WHERE total < {int(min_games)}
                    GROUP BY parent_hash)
                TO '{_sql_path(otmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            tasks.append((sql_group, surv_sql, other_sql, str(btmp), str(bout),
                          str(otmp), str(oout), threads, mem, str(tmp_dir),
                          f"ps bucket {i}"))
        print(f"  Phase B (ps): GROUP BY in {N_MERGE_BUCKETS} hash buckets, "
              f"survivors + other-moves ({len(tasks)} to build)...", flush=True)
        with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
            for fut in as_completed([ex.submit(_run_ps_bucket, t) for t in tasks]):
                label, size, secs = fut.result()
                print(f"    {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
        # Buckets are disjoint: the checkpoint is a cheap streaming concat.
        agg_write.unlink(missing_ok=True)
        con = _duck(threads, mem, tmp_dir)
        try:
            con.execute(f"""
                COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
                TO '{_sql_path(agg_write)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            con.close()
        agg_write.replace(agg_ckpt)
        shutil.rmtree(bdir, ignore_errors=True)
        shutil.rmtree(pdir, ignore_errors=True)  # ~E: footprint of the inputs; free before crush
        print(f"  ps GROUP BY done in {(time.time()-t0)/60:.1f} min; stamping...", flush=True)

    # Final stamping. child_hash now arrives from the EXTRACT (the position after
    # ply p is the position before ply p+1 — see _walk_game), so the single-threaded
    # python-chess re-derivation that used to run here is gone: it was measured at
    # 11,286 rows/s (88.6 µs/row, ~40 min on 27.4M rows) in the merge's serial tail.
    # _test_extract_child_hash.py is what holds the extract to the same answer.
    df = pl.read_parquet(agg_ckpt)
    df = df.with_columns(
        pl.lit(POOL_EVENT).alias("event"),
        pl.lit(POOL_ELO, dtype=pl.Int64).alias("elo_band"),
        ((pl.col("white_wins") + 0.5 * pl.col("draws")) / pl.col("total")).alias("white_score_avg"),
    )
    out_tmp = out_path.with_suffix(".parquet.tmp")
    df.write_parquet(out_tmp, compression="zstd")
    out_tmp.replace(out_path)
    agg_ckpt.unlink(missing_ok=True)
    # Repeated here for the checkpoint-resume path: a crash between the checkpoint
    # rename and the else-branch cleanup leaves ~390 GB of partition files behind,
    # which starved the crush merge of disk once already.
    shutil.rmtree(bdir, ignore_errors=True)
    shutil.rmtree(pdir, ignore_errors=True)
    print(f"  position-stats: {out_path.name} ({out_path.stat().st_size/1e6:.0f} MB, "
          f"{df.height:,} rows)", flush=True)


def merge_crush(src_dir: Path, partial_dir: Path, ps_path: Path, out_path: Path,
                threads: int, mem: str, tmp_base: Path | None = None,
                kind: str = "crush") -> None:
    """Histogram merge. `kind` selects the partial family — "crush" for the
    retired resignation-proxy histogram, "winpos<T>" for a fused winpos one.

    The SEMI JOIN to surviving position-stats keys is what makes the fused winpos
    path correct: the extract cannot apply the pool's min_games floor (it is a
    global decision made here), so it emits a row for EVERY edge and the
    restriction happens on this side — the same thing winpos_sql's `keys` INNER
    JOIN did when winpos was a separate pass.
    """
    if out_path.exists():
        print(f"SKIP {kind} merge: {out_path.name} exists")
        return
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_tmp = out_path.with_suffix(".parquet.tmp")
    ps = _sql_path(ps_path)
    t0 = time.time()
    # Two-phase like the ps merge: Phase A partitions the monthly crush files by
    # parent_hash bucket; Phase B runs each bucket's GROUP BY + SEMI JOIN to the
    # surviving position-stats keys (collapse the tail), with the join build side
    # filtered to the same bucket.
    bdir = out_path.with_name(out_path.stem + "_buckets")
    bdir.mkdir(parents=True, exist_ok=True)
    pdir = out_path.with_name(out_path.stem + "_parts")
    _partition_by_bucket(src_dir, kind, pdir, threads, mem, tmp_dir)
    tasks = []
    for i in range(N_MERGE_BUCKETS):
        bout = bdir / f"bucket_{i}.parquet"
        if bout.exists():
            continue
        if not _bucket_has_data(pdir, i):
            _write_empty(bout, _HIST_BUCKET_SCHEMA)
            continue
        sql = f"""
            COPY (
                SELECT c.parent_hash, c.move_san, c.move_bucket,
                       SUM(c.n)::BIGINT          AS n,
                       SUM(c.white_wins)::BIGINT AS white_wins,
                       SUM(c.black_wins)::BIGINT AS black_wins,
                       '{POOL_EVENT}' AS event, {POOL_ELO} AS elo_band
                FROM read_parquet('{_sql_path(pdir)}/month=*/bkt={i}/*.parquet') c
                SEMI JOIN (SELECT DISTINCT parent_hash, move_san FROM read_parquet('{ps}')
                           WHERE {_bucket_expr("parent_hash")} = {i}) k
                  ON c.parent_hash = k.parent_hash AND c.move_san = k.move_san
                GROUP BY c.parent_hash, c.move_san, c.move_bucket
            ) TO '{_sql_path(bout.with_suffix(".parquet.tmp"))}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        tasks.append((sql, str(bout.with_suffix(".parquet.tmp")), str(bout),
                      threads, mem, str(tmp_dir), f"{kind} bucket {i}"))
    print(f"  Phase B ({kind}): GROUP BY in {N_MERGE_BUCKETS} hash buckets "
          f"({len(tasks)} to build)...", flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"    {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
    out_tmp.unlink(missing_ok=True)
    con = _duck(threads, mem, tmp_dir)
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
            TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out_path)
    shutil.rmtree(bdir, ignore_errors=True)
    shutil.rmtree(pdir, ignore_errors=True)
    print(f"  {kind}: {out_path.name} ({out_path.stat().st_size/1e6:.0f} MB) "
          f"in {(time.time()-t0)/60:.1f} min", flush=True)


# ── orchestration ──────────────────────────────────────────────────────────────

def main() -> None:
    # Declared up front: --source rebinds it below, and the argparse help text
    # reads it, so the declaration has to precede the first use in this scope.
    global SOURCE_ROOT, WINPOS_THRESHOLDS
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    ap.add_argument("--months", type=int, nargs="*", default=None,
                    help="Restrict to these months (default: all present).")
    ap.add_argument("--min-elo", type=int, default=1800)
    ap.add_argument("--events", nargs="+", default=["Blitz", "Rapid", "Classical"])
    ap.add_argument("--fuse-winpos", action=argparse.BooleanOptionalAction, default=True,
                    help=f"Compute the winpos crush histogram inside THIS replay at "
                         f"thresholds {WINPOS_THRESHOLDS} cp, instead of running "
                         f"build_crush_winpos_phase2.py as a second full pass over "
                         f"the same files (measured 47 h, peak 76 GB). Needs the "
                         f"mmap'd eval arrays (python/eval_arrays.py). Events are "
                         f"identical to build_crush_winpos.winpos_sql — see "
                         f"_test_winpos_fused.py — except that pool-survivor "
                         f"filtering moves to the merge, since min_games is not "
                         f"known during extraction. DEFAULT ON.")
    ap.add_argument("--source", default=None,
                    help=f"Derived-parquet root to read (default: {SOURCE_ROOT}). "
                         f"Discovery happens here in main and worker tasks carry "
                         f"absolute paths, so pointing this at a small tree is "
                         f"enough to run the whole pipeline over a bounded slice.")
    ap.add_argument("--winpos-thresholds", type=int, nargs="+",
                    default=list(WINPOS_THRESHOLDS),
                    help=f"Crossing thresholds in cp (default {WINPOS_THRESHOLDS}). "
                         f"Each one is a separate histogram to build AND to merge, "
                         f"so cost scales with the count — the eval lookup is shared "
                         f"but nothing downstream of it is.")
    ap.add_argument("--min-games", type=int, default=50)
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--crush-hist", action="store_true",
                    help="Also merge the resignation-proxy crush histogram. OFF by "
                         "default: no consumer reads it (build_sharp_reps.py uses the "
                         "winpos histogram crush_hist_relwin_*), and consolidating its "
                         "partials is the largest avoidable cost in the merge. The "
                         "extract still writes .crush.parquet partials, so this can be "
                         "turned back on later without re-extracting.")
    ap.add_argument("--workers", type=int, default=11)
    ap.add_argument("--threads", type=int, default=8, help="DuckDB threads for the merge phase.")
    ap.add_argument("--mem", default="48GB", help="DuckDB memory_limit for the merge phase.")
    ap.add_argument("--phase", choices=["extract", "merge", "all"], default="all")
    ap.add_argument("--limit-games", type=int, default=None, help="Per-file game cap (benchmarking).")
    ap.add_argument("--partial-dir", default=None, help="Override the partials directory.")
    ap.add_argument("--tmp-dir", default=None,
                    help="Override the DuckDB merge-phase temp/spill directory "
                         "(default: <partial-dir>/_merge_duckdb_tmp). Point this at a "
                         "drive with ample free space if the default drive is tight.")
    ap.add_argument("--tag", default=None, help="Override the output filename tag.")
    # ── asymmetric-depth prune (seeded from existing >=1800 frequency stats) ──
    ap.add_argument("--no-prune", action="store_true",
                    help="Disable asymmetric-depth pruning (extract every line to --max-ply).")
    ap.add_argument("--seed-stats",
                    default=str(STATS_DIR / "position_stats_pooled_1900_2200_brc.parquet"),
                    help="Existing position_stats used to seed first-move/response frequencies.")
    ap.add_argument("--main-moves", nargs="+", default=["e4", "d4", "c4", "Nf3"],
                    help="White first moves kept at full --deep-ply depth.")
    ap.add_argument("--deep-thresh", type=float, default=0.01,
                    help="Freq cutoff (>=) for deep tier: P(w1) for offbeat first moves, "
                         "P(b1|w1) for replies to main moves. Default 1/100.")
    ap.add_argument("--mid-thresh", type=float, default=0.0025,
                    help="Freq cutoff (>=) for mid tier. Default 1/400.")
    ap.add_argument("--deep-ply", type=int, default=30)
    ap.add_argument("--mid-ply", type=int, default=18)
    ap.add_argument("--shallow-ply", type=int, default=10)
    args = ap.parse_args()

    if args.source:
        SOURCE_ROOT = Path(args.source)
    WINPOS_THRESHOLDS = tuple(args.winpos_thresholds)

    ev_tag = "".join(e[0].lower() for e in args.events)  # brc
    tag = args.tag or f"ge{args.min_elo}_{args.start_year}_{args.end_year}_{ev_tag}"
    partial_dir = Path(args.partial_dir) if args.partial_dir else \
        STATS_DIR / f"_pooled_partials_{tag}"
    ps_out = STATS_DIR / f"position_stats_pooled_{tag}.parquet"
    aux_out = STATS_DIR / f"position_stats_aux_pooled_{tag}.parquet"
    crush_out = STATS_DIR / f"crush_hist_rel_pooled_{tag}.parquet"

    print(f"Target: events={args.events} mean_elo>={args.min_elo} "
          f"years {args.start_year}-{args.end_year} months={args.months or 'all'}")
    print(f"Partials: {partial_dir}")
    print(f"Outputs:  {ps_out.name}"
          + (f" | {crush_out.name}" if args.crush_hist
             else "  (resignation-proxy crush histogram SKIPPED; --crush-hist to build)"),
          flush=True)

    # Build the asymmetric-depth tier tables (unless disabled).
    tiers = None
    if not args.no_prune:
        seed = Path(args.seed_stats)
        if not seed.exists():
            sys.exit(f"FATAL: --seed-stats not found: {seed} (use --no-prune to skip pruning)")
        tiers = build_depth_tiers(seed, args.main_moves, args.deep_thresh, args.mid_thresh,
                                  args.deep_ply, args.mid_ply, args.shallow_ply)
        print(f"\nDepth prune (seed={seed.name}):")
        print(f"  {args.deep_ply}-ply: main {sorted(tiers['four'])} + replies P(b1|w1)>="
              f"{args.deep_thresh}")
        for w1 in args.main_moves:
            print(f"      vs {w1}: {sorted(tiers['deep_resp'].get(w1, []))}")
        print(f"  {args.mid_ply}-ply: offbeat first moves {sorted(tiers['common_white_other'])} "
              f"+ mid replies " + ", ".join(
                  f"{w1}:{sorted(tiers['mid_resp'].get(w1, []))}" for w1 in args.main_moves
                  if tiers['mid_resp'].get(w1)))
        print(f"  {args.shallow_ply}-ply: everything else\n", flush=True)
    else:
        print("\nDepth prune: DISABLED (full depth)\n", flush=True)

    if args.phase in ("extract", "all"):
        # Fail here, not 6 workers deep. Every extract needs the eval arrays now
        # (child_eval for the other-moves bucket, plus the winpos crossings when
        # fused), and open_eval_arrays runs inside the worker — so a missing or
        # STALE pair would otherwise surface as N identical tracebacks out of a
        # process pool at the start of a ~90 h run. Verifying once up front turns
        # that into one line naming the fix.
        try:
            print(f"Eval arrays: {verify_eval_arrays()}", flush=True)
        except (FileNotFoundError, ValueError) as e:
            sys.exit(f"FATAL: {e}")

        files = discover_source_files(args.start_year, args.end_year, args.months, args.events)
        partial_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        for f, y, m, ev in files:
            ps_p = partial_dir / partial_name(f, y, m, ev, "ps")
            # Gated on the same flag as the merge below. Both must move together:
            # dropping the write while the gate still demands cr_p.exists() would
            # fail every already-complete chunk and re-extract the lot.
            cr_p = (partial_dir / partial_name(f, y, m, ev, "crush")
                    if args.crush_hist else None)
            tm_p = partial_dir / partial_name(f, y, m, ev, "term")
            wp_p = {t: partial_dir / partial_name(f, y, m, ev, f"winpos{t}")
                    for t in (WINPOS_THRESHOLDS if args.fuse_winpos else ())}
            if (ps_p.exists() and tm_p.exists()
                    and (cr_p is None or cr_p.exists())
                    and all(p.exists() for p in wp_p.values())):
                continue
            tasks.append((str(f), str(ps_p), str(cr_p) if cr_p else None, str(tm_p),
                          {str(t): str(p) for t, p in wp_p.items()},
                          args.limit_games))
        print(f"Extract: {len(files)} source files, {len(tasks)} to process, "
              f"{args.workers} workers\n", flush=True)
        t0 = time.time()
        done = tot_games = tot_kept = 0
        if tasks:
            with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker,
                                     initargs=(args.min_elo, args.max_ply, tiers)) as ex:
                futs = [ex.submit(_worker, t) for t in tasks]
                for fut in as_completed(futs):
                    r = fut.result()
                    done += 1
                    tot_games += r.get("games", 0)
                    tot_kept += r.get("kept", 0)
                    if done % 10 == 0 or done == len(tasks):
                        el = time.time() - t0
                        rate = tot_games / el if el else 0
                        print(f"  [{done}/{len(tasks)}] {r['file']}: "
                              f"{r.get('kept',0):,}/{r.get('games',0):,} kept, "
                              f"ps={r.get('ps_rows',0):,} wp={r.get('winpos_rows',0):,} "
                              f"({r['sec']:.0f}s) | {rate:,.0f} games/s agg | "
                              f"{el/60:.1f} min", flush=True)
        el = time.time() - t0
        print(f"\nExtract done: {tot_kept:,}/{tot_games:,} games kept in {el/60:.1f} min "
              f"({tot_games/el if el else 0:,.0f} games/s)\n", flush=True)

    if args.phase in ("merge", "all"):
        tmp_base = Path(args.tmp_dir) if args.tmp_dir else None
        # Stage 1: SUM-only per-month consolidation (NO min_games filter).
        kinds = ["ps"]
        if args.crush_hist:
            kinds.append("crush")
        if any(partial_dir.glob("*.term.parquet")):
            kinds.append("term")
        if args.fuse_winpos:
            kinds += [f"winpos{t}" for t in WINPOS_THRESHOLDS
                      if any(partial_dir.glob(f"*.winpos{t}.parquet"))]
        print("Consolidating per-month (sum only, no filter)...", flush=True)
        monthly_dir = consolidate_monthly(partial_dir, args.threads, args.mem, tmp_base,
                                          tuple(kinds))
        # Stage 2: final global merge — min_games applied here ONCE, over all months.
        print("Final merge: position-stats (min_games applied here)...", flush=True)
        merge_position_stats(monthly_dir, partial_dir, ps_out, args.min_games,
                             args.threads, args.mem, tmp_base)
        # Stage 2b: the Stage-3 sidecar. Everything the outgoing edges cannot see —
        # terminations, horizon truncations, and the below-floor tail that Phase B
        # now keeps instead of discarding.
        print("Final merge: aux-stats sidecar (term / horizon / other-moves)...",
              flush=True)
        merge_aux_stats(partial_dir,
                        ps_out.with_name(ps_out.stem + "_other_buckets"),
                        ps_out, aux_out, args.threads, args.mem, tmp_base)
        if args.crush_hist:
            print("Final merge: crush histogram...", flush=True)
            merge_crush(monthly_dir, partial_dir, ps_out, crush_out, args.threads, args.mem,
                       tmp_base)
        if args.fuse_winpos:
            for thr in WINPOS_THRESHOLDS:
                wp_out = ps_out.with_name(
                    ps_out.stem.replace("position_stats_pooled",
                                        "crush_hist_relwin_pooled")
                    + f"_t{thr}.parquet")
                print(f"Final merge: fused winpos histogram @{thr}cp...", flush=True)
                merge_crush(monthly_dir, partial_dir, ps_out, wp_out,
                            args.threads, args.mem, tmp_base, kind=f"winpos{thr}")
        shutil.rmtree((tmp_base or partial_dir) / "_merge_duckdb_tmp", ignore_errors=True)
        print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
