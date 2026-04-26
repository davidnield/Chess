"""
Stage 4: Stockfish safety filter for the repertoire.

Walks the reachable opening tree from the starting position (following our
recommended best_moves and every common opponent response), runs Stockfish on
each unique resulting position, and annotates the Stage 3 repertoire with the
engine evaluation. Positions whose engine eval falls below a threshold from
our perspective are flagged as unsafe -- typically catching empirically-good
trap lines that a strong opponent would punish.

Engine evaluations are cached in a persistent parquet keyed on EPD (the eval
of a position is slice-independent). Re-runs only evaluate new positions.
The cache is checkpointed every `save_every` evaluations so an interrupted
run loses minimal work.

Output columns added to the Stage 3 repertoire:
    eval_cp           -- engine evaluation in centipawns from white's POV
                         (mate scores converted via mate_score=10000)
    eval_depth        -- depth used for the cached evaluation
    engine_best_uci   -- engine's preferred move (UCI format)
    is_safe           -- True if eval_cp passes our perspective's threshold,
                         null if the position wasn't evaluated

Usage:
    .venv/Scripts/python.exe python/stage4_engine_filter.py \\
        --input-repertoire D:/data/chess/repertoire/repertoire_2019.parquet \\
        --input-stats      D:/data/chess/position-stats/position_stats_2019.parquet \\
        --output           D:/data/chess/repertoire/repertoire_2019_safe.parquet \\
        --depth 18 --threshold-cp 100

    # Dry run (annotate using existing cache only -- no Stockfish):
    .venv/Scripts/python.exe python/stage4_engine_filter.py --no-eval ...

Stockfish must be available on PATH or via --stockfish.
"""

from __future__ import annotations

import argparse
import datetime
import shutil
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import chess
import chess.engine
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DEFAULT_REPERTOIRE = Path("D:/data/chess/repertoire/repertoire.parquet")
DEFAULT_STATS      = Path("D:/data/chess/position-stats/position_stats.parquet")
DEFAULT_OUTPUT     = Path("D:/data/chess/repertoire/repertoire_safe.parquet")
DEFAULT_CACHE      = Path("D:/data/chess/engine-cache/evals.parquet")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64
MATE_SCORE  = 10000  # cp equivalent used to flatten forced-mate scores


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Cache I/O ─────────────────────────────────────────────────────────────

def load_cache(path: Path) -> dict[str, dict]:
    """Return EPD -> eval-record dict; empty if cache file doesn't exist."""
    if not path.exists():
        return {}
    df = pl.read_parquet(path)
    return {r["epd"]: r for r in df.iter_rows(named=True)}


def save_cache(cache: dict[str, dict], path: Path) -> None:
    """Atomically write cache via .tmp + replace."""
    if not cache:
        return
    df = pl.from_dicts(list(cache.values()))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)


# ── Position walker ────────────────────────────────────────────────────────

def collect_reachable_epds(
    repertoire: pl.DataFrame,
    stats: pl.DataFrame,
    perspective: str,
    opp_min_games: int,
    max_depth: int,
) -> set[str]:
    """
    Walk from the starting position through every (event, elo_band) slice's
    recommended tree, returning the union of unique EPDs encountered (capped
    at `max_depth` plies from start).

    At our turn we follow only our best_move; at opponent's turn we follow
    every move with `total >= opp_min_games`. Same EPD across multiple slices
    counts once (eval is slice-independent).
    """
    our_color = chess.WHITE if perspective == "white" else chess.BLACK
    start_hash = zobrist_int64(chess.Board())
    epds: set[str] = set()

    slices = repertoire.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        rep_slice = repertoire.filter(
            (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        )
        stats_slice = stats.filter(
            (pl.col("event") == ev)
            & (pl.col("elo_band") == eb)
            & (pl.col("total") >= opp_min_games)
        )

        rep_idx = {r["position_hash"]: r for r in rep_slice.iter_rows(named=True)}
        opp_idx: dict[int, list[str]] = defaultdict(list)
        for r in stats_slice.iter_rows(named=True):
            opp_idx[r["parent_hash"]].append(r["move_san"])

        queue: deque[tuple[int, int]] = deque([(start_hash, 0)])
        seen: set[int] = set()
        while queue:
            ph, depth = queue.popleft()
            if ph in seen or depth > max_depth:
                continue
            seen.add(ph)

            row = rep_idx.get(ph)
            if row is None:
                continue
            epds.add(row["position_epd"])

            board = chess.Board(row["position_epd"])
            if board.turn == our_color:
                moves = [row["best_move"]] if row["best_move"] else []
            else:
                moves = opp_idx.get(ph, [])

            for san in moves:
                try:
                    b2 = board.copy()
                    b2.push(b2.parse_san(san))
                    queue.append((zobrist_int64(b2), depth + 1))
                except (ValueError, AssertionError):
                    continue

    return epds


# ── Engine evaluation ──────────────────────────────────────────────────────

def check_stockfish(path: str) -> str:
    """Validate Stockfish binary is reachable; return resolved absolute path."""
    resolved = shutil.which(path) or path
    if not Path(resolved).exists():
        raise FileNotFoundError(
            f"Stockfish binary not found at {path!r}. "
            f"Install from https://stockfishchess.org/download/ or pass --stockfish <path>."
        )
    return resolved


def evaluate_with_engine(
    epds: list[str],
    cache: dict[str, dict],
    stockfish_path: str,
    depth: int,
    threads: int,
    save_path: Path,
    save_every: int = 200,
) -> None:
    """Evaluate EPDs not already cached at `depth` or deeper. Updates cache in place."""
    to_eval = [e for e in epds if e not in cache or cache[e].get("depth", 0) < depth]
    n_total, n_eval = len(epds), len(to_eval)
    print(f"Cache hit: {n_total - n_eval:,}/{n_total:,}; "
          f"need to evaluate {n_eval:,} at depth {depth}.")
    if not to_eval:
        return

    engine = chess.engine.SimpleEngine.popen_uci(stockfish_path)
    try:
        try:
            engine.configure({"Threads": threads})
        except Exception as e:
            print(f"  WARN: could not set Threads={threads}: {e}")

        t0 = time.time()
        for i, epd in enumerate(to_eval, 1):
            try:
                board = chess.Board(epd)
                info  = engine.analyse(board, chess.engine.Limit(depth=depth))
                score = info["score"].white().score(mate_score=MATE_SCORE)
                pv    = info.get("pv", [])
                cache[epd] = {
                    "epd":             epd,
                    "depth":           depth,
                    "cp":              int(score) if score is not None else None,
                    "engine_best_uci": pv[0].uci() if pv else None,
                    "evaluated_at":    datetime.datetime.now().isoformat(timespec="seconds"),
                }
            except Exception as e:
                print(f"  WARN: failed {epd[:60]}: {e}")
                continue

            if i % save_every == 0:
                save_cache(cache, save_path)
                elapsed = time.time() - t0
                rate    = i / elapsed if elapsed > 0 else 0
                eta_min = (n_eval - i) / rate / 60 if rate > 0 else 0
                print(f"  [{i:>6,}/{n_eval:,}]  {rate:5.1f} pos/sec  ETA {eta_min:.1f} min")
    finally:
        engine.quit()
        save_cache(cache, save_path)


# ── Annotation ─────────────────────────────────────────────────────────────

def annotate_repertoire(
    repertoire: pl.DataFrame,
    cache: dict[str, dict],
    perspective: str,
    threshold_cp: int,
) -> pl.DataFrame:
    """Left-join engine eval columns and compute is_safe."""
    if cache:
        cache_df = pl.from_dicts(list(cache.values()))
    else:
        cache_df = pl.DataFrame(schema={
            "epd": pl.String, "depth": pl.Int64, "cp": pl.Int64,
            "engine_best_uci": pl.String, "evaluated_at": pl.String,
        })

    out = repertoire.join(
        cache_df.select(
            pl.col("epd").alias("position_epd"),
            pl.col("cp").alias("eval_cp"),
            pl.col("depth").alias("eval_depth"),
            pl.col("engine_best_uci"),
        ),
        on="position_epd",
        how="left",
    )

    if perspective == "white":
        safe_expr = pl.col("eval_cp") >= -threshold_cp
    else:
        safe_expr = pl.col("eval_cp") <= threshold_cp

    out = out.with_columns(
        pl.when(pl.col("eval_cp").is_null())
          .then(None)
          .otherwise(safe_expr)
          .alias("is_safe")
    )
    return out


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input-repertoire", default=str(DEFAULT_REPERTOIRE))
    parser.add_argument("--input-stats",      default=str(DEFAULT_STATS))
    parser.add_argument("--output",           default=str(DEFAULT_OUTPUT))
    parser.add_argument("--cache",            default=str(DEFAULT_CACHE))
    parser.add_argument("--stockfish",        default="stockfish",
                        help="Path to Stockfish binary (default: 'stockfish' on PATH)")
    parser.add_argument("--depth",        type=int, default=18,
                        help="Stockfish search depth (default: 18)")
    parser.add_argument("--threads",      type=int, default=4,
                        help="Stockfish UCI Threads option (default: 4)")
    parser.add_argument("--threshold-cp", type=int, default=100,
                        help="Safety threshold in centipawns from our POV (default: 100 = 1 pawn)")
    parser.add_argument("--perspective",  choices=["white", "black"], default="white")
    parser.add_argument("--max-depth",    type=int, default=16,
                        help="Max plies from start to walk for evaluation (default: 16)")
    parser.add_argument("--opp-min-games", type=int, default=10,
                        help="Min total games for an opp move to be followed (default: 10)")
    parser.add_argument("--save-every",   type=int, default=200,
                        help="Checkpoint cache every N evaluations (default: 200)")
    parser.add_argument("--no-eval",      action="store_true",
                        help="Skip engine; only annotate using existing cache")
    args = parser.parse_args()

    rep_path   = Path(args.input_repertoire)
    stats_path = Path(args.input_stats)
    out_path   = Path(args.output)
    cache_path = Path(args.cache)

    print(f"Repertoire:    {rep_path}")
    print(f"Stats:         {stats_path}")
    print(f"Output:        {out_path}")
    print(f"Cache:         {cache_path}")
    print(f"Perspective:   {args.perspective}")
    print(f"Threshold:     {args.threshold_cp}cp from our POV")
    print(f"Walk depth:    {args.max_depth} plies, opp_min_games={args.opp_min_games}")
    if not args.no_eval:
        print(f"Stockfish:     {args.stockfish} (depth {args.depth}, {args.threads} threads)")
        stockfish_path = check_stockfish(args.stockfish)
        print(f"  resolved -> {stockfish_path}")
    else:
        print("Engine eval:   SKIPPED (--no-eval)")
        stockfish_path = None

    # ── Load
    rep   = pl.read_parquet(rep_path)
    stats = pl.read_parquet(stats_path)
    cache = load_cache(cache_path)
    print(f"\nLoaded {len(rep):,} repertoire rows; "
          f"{stats['parent_hash'].n_unique():,} distinct positions in stats; "
          f"{len(cache):,} cached evals.")

    # ── Walk
    print("\nCollecting reachable EPDs...")
    t0 = time.time()
    epds = collect_reachable_epds(
        rep, stats, args.perspective,
        opp_min_games=args.opp_min_games,
        max_depth=args.max_depth,
    )
    print(f"  Found {len(epds):,} unique reachable EPDs in {time.time() - t0:.1f}s")

    # ── Evaluate
    if not args.no_eval:
        print()
        evaluate_with_engine(
            list(epds), cache, stockfish_path,
            depth=args.depth, threads=args.threads,
            save_path=cache_path, save_every=args.save_every,
        )

    # ── Annotate
    print("\nAnnotating repertoire...")
    out = annotate_repertoire(rep, cache, args.perspective, args.threshold_cp)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(out_path, compression="zstd")

    # ── Summary
    n        = len(out)
    n_evald  = out.filter(pl.col("eval_cp").is_not_null()).height
    n_safe   = out.filter(pl.col("is_safe") == True).height
    n_unsafe = out.filter(pl.col("is_safe") == False).height

    print(f"\nWrote {out_path} ({out_path.stat().st_size / 1e6:.1f} MB)")
    print(f"Total rows: {n:,}")
    print(f"Evaluated:  {n_evald:,}  ({n_evald / n * 100:.1f}%)")
    if n_evald:
        print(f"Safe:       {n_safe:,}  ({n_safe / n * 100:.1f}% of total, "
              f"{n_safe / n_evald * 100:.1f}% of evaluated)")
        print(f"Unsafe:     {n_unsafe:,}  ({n_unsafe / n * 100:.1f}% of total, "
              f"{n_unsafe / n_evald * 100:.1f}% of evaluated)")
    else:
        print("Safe / unsafe: no positions evaluated yet.")

    # Per-slice unsafe-first-move report
    start_hash = zobrist_int64(chess.Board())
    flagged = (
        out.filter((pl.col("position_hash") == start_hash) & (pl.col("is_safe") == False))
        .select(["event", "elo_band", "best_move", "value", "eval_cp", "engine_best_uci"])
        .sort(["event", "elo_band"])
    )
    if len(flagged):
        print(f"\nSlices whose recommended FIRST MOVE is engine-unsafe:")
        print(flagged)
    else:
        print("\nNo slice's recommended first move is engine-unsafe.")

    # Show worst-eval positions on the recommended path
    worst = (
        out.filter(pl.col("eval_cp").is_not_null())
        .sort("eval_cp", descending=(args.perspective == "black"))
        .select(["event", "elo_band", "side_to_move", "best_move", "value", "eval_cp", "is_safe"])
        .head(15)
    )
    print(f"\n15 worst-eval positions in the annotated repertoire (from our perspective):")
    print(worst)


if __name__ == "__main__":
    main()
