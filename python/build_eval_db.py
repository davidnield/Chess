"""
Build a position → Stockfish eval lookup table from Lichess movetext annotations.

Lichess games contain per-move Stockfish evaluations embedded in the movetext:

    1. e4 { [%eval 0.31] [%clk 0:03:00] } 1... Nf6 { [%eval 0.21] [%clk 0:03:00] }

The annotation after each move evaluates the position AFTER that move.  This
script scans source parquets in reverse chronological order (newest games first)
so that each position's eval comes from the most recent Stockfish version.

Output: a parquet with (position_hash: Int64, eval_cp: Int32) — one row per
unique position, ready for Stage 3 to join against.

Usage:
    .venv/Scripts/python.exe python/build_eval_db.py \\
        --output E:/chess/eval_db.parquet \\
        --stats  E:/chess/position-stats/position_stats_2016.parquet \\
        --year-range 2020 2025

    # Without targeting (extract evals for every position encountered):
    .venv/Scripts/python.exe python/build_eval_db.py \\
        --output E:/chess/eval_db.parquet \\
        --year-range 2024 2025
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


SOURCE_ROOT    = Path("D:/data/chess/standard-chess-games-compressed")
DEFAULT_OUTPUT = Path("E:/chess/eval_db.parquet")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64

EVAL_RE     = re.compile(r"\[%eval ([#\-\d.]+)\]")
MOVE_NUM_RE = re.compile(r"^\d+\.+$")
RESULT_TOKENS = {"1-0", "0-1", "1/2-1/2", "*"}

MATE_CP = 10_000  # centipawn equivalent for forced-mate positions


def zobrist_int64(board: chess.Board) -> int:
    """Polyglot Zobrist hash cast to signed int64 (parquet/polars compatible)."""
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


def parse_eval(s: str) -> int:
    """Convert Lichess eval string to centipawns.

    '0.31' → 31,  '-1.5' → -150,  '#3' → +10000,  '#-5' → -10000
    """
    if s.startswith("#"):
        mate_in = int(s[1:])
        return MATE_CP if mate_in > 0 else -MATE_CP
    return int(round(float(s) * 100))


# ── Movetext parser ───────────────────────────────────────────────────────────

def iter_moves_with_eval(movetext: str):
    """Yield (SAN, eval_cp | None) for each move in annotated Lichess movetext.

    The eval in the annotation block after move N represents the position
    AFTER that move is played on the board (the child position).

    Handles: [%eval 0.31], [%eval -1.5], [%eval #3], [%eval #-5],
    mixed annotations like { [%eval 0.31] [%clk 0:03:00] }, and NAG
    suffixes on SAN tokens (e.g. 'g5??' → 'g5').
    """
    # Split on annotation blocks: alternating (text, annotation, text, ...)
    segments = re.split(r"(\{[^}]*\})", movetext)

    pending_san: str | None = None

    for i, seg in enumerate(segments):
        seg = seg.strip()
        if not seg:
            continue

        if seg.startswith("{"):
            # Annotation block — extract eval if present
            eval_cp: int | None = None
            m = EVAL_RE.search(seg)
            if m:
                eval_cp = parse_eval(m.group(1))
            if pending_san is not None:
                yield (pending_san, eval_cp)
                pending_san = None
        else:
            # Non-annotation text — find SAN tokens
            # If there was a pending SAN without annotation, emit it first
            if pending_san is not None:
                yield (pending_san, None)
                pending_san = None

            for tok in seg.split():
                # Strip trailing NAGs: ?, ??, !, !!, ?!, !?
                clean = tok.rstrip("?!")
                if not clean:
                    continue
                if MOVE_NUM_RE.match(clean):
                    continue
                if clean in RESULT_TOKENS:
                    return
                pending_san = clean

    # Handle last move without annotation
    if pending_san is not None:
        yield (pending_san, None)


def extract_evals_from_game(
    movetext: str,
    max_ply: int = 30,
) -> list[tuple[int, int]]:
    """Play through a game and return (position_hash, eval_cp) pairs.

    The eval annotation after move N evaluates the position after that move.
    We push the move on the board, then record (hash-of-child, eval_cp).

    Returns a list of (position_hash, eval_cp) tuples where eval was available.
    """
    board = chess.Board()
    results: list[tuple[int, int]] = []

    for ply, (san, eval_cp) in enumerate(iter_moves_with_eval(movetext), 1):
        if ply > max_ply:
            break
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            break
        board.push(move)
        if eval_cp is not None:
            results.append((zobrist_int64(board), eval_cp))

    return results


# ── Partition discovery ───────────────────────────────────────────────────────

def discover_partitions_reverse_chrono(
    source_root: Path,
    year_range: tuple[int, int] | None = None,
) -> list[Path]:
    """Find all parquet files under source_root, sorted newest-first.

    Returns individual .parquet file paths, ordered by (year desc, month desc,
    event asc, part asc) so that the most recent games are processed first.
    """
    partitions: list[tuple[int, int, str, Path]] = []

    for year_dir in source_root.glob("year=*"):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue
        if year_range and not (year_range[0] <= year <= year_range[1]):
            continue
        for month_dir in year_dir.glob("month=*"):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            for event_dir in month_dir.glob("event=*"):
                event = event_dir.name.split("=", 1)[1]
                for pq_file in sorted(event_dir.glob("*.parquet")):
                    partitions.append((year, month, event, pq_file))

    # Sort by (year desc, month desc, event asc, path asc)
    partitions.sort(key=lambda x: (-x[0], -x[1], x[2], str(x[3])))
    return [p for _, _, _, p in partitions]


# ── Target hash loading ──────────────────────────────────────────────────────

def load_target_hashes(stats_path: Path) -> set[int]:
    """Load the set of position hashes that Stage 3 will encounter.

    This is the union of parent_hash (every position in the stats) and
    child_hash (every position reachable from a move in the stats, including
    leaves where Stockfish eval is especially valuable).
    """
    stats = pl.read_parquet(stats_path, columns=["parent_hash", "child_hash"])
    parents = set(stats["parent_hash"].unique().to_list())
    children = set(stats["child_hash"].drop_nulls().unique().to_list())
    return parents | children


# ── Main ──────────────────────────────────────────────────────────────────────

def build_eval_db(
    source_root: Path,
    output_path: Path,
    target_hashes: set[int] | None,
    max_ply: int,
    year_range: tuple[int, int] | None,
) -> None:
    """Build the eval DB by scanning source data in reverse chronological order."""
    eval_db: dict[int, int] = {}  # position_hash → eval_cp
    target_size = len(target_hashes) if target_hashes else None

    partitions = discover_partitions_reverse_chrono(source_root, year_range)
    print(f"Found {len(partitions)} partition files to scan")
    if target_size:
        print(f"Targeting {target_size:,} positions from stats")

    t_start = time.time()
    total_games_scanned = 0
    total_eval_games = 0
    total_positions_added = 0
    partitions_scanned = 0

    for part_path in partitions:
        partitions_scanned += 1

        try:
            df = pl.read_parquet(part_path, columns=["movetext", "has_eval"])
        except Exception as e:
            print(f"  WARN: failed to read {part_path}: {e}")
            continue

        total_games_scanned += len(df)
        eval_games = df.filter(pl.col("has_eval") == True)
        n_eval = len(eval_games)
        total_eval_games += n_eval

        if n_eval == 0:
            continue

        added_this_partition = 0
        for row in eval_games.iter_rows(named=True):
            pairs = extract_evals_from_game(row["movetext"], max_ply)
            for pos_hash, eval_cp in pairs:
                if pos_hash in eval_db:
                    continue  # already have a newer eval
                if target_hashes is not None and pos_hash not in target_hashes:
                    continue  # not a position we care about
                eval_db[pos_hash] = eval_cp
                added_this_partition += 1
                total_positions_added += 1

        # Progress report
        elapsed = time.time() - t_start
        coverage_str = ""
        if target_size:
            pct = len(eval_db) / target_size * 100
            coverage_str = f"  coverage={pct:.1f}%"
        # Extract year/month from path for display
        parts = str(part_path).replace("\\", "/")
        ym = "/".join(p for p in parts.split("/") if p.startswith(("year=", "month=", "event=")))
        print(f"  [{partitions_scanned:>4}/{len(partitions)}] {ym}: "
              f"+{added_this_partition:,} positions  "
              f"(db={len(eval_db):,}{coverage_str})  "
              f"[{elapsed:.0f}s]")

        # Early stopping
        if target_hashes and len(eval_db) >= target_size:
            print(f"\n  All {target_size:,} target positions covered — stopping early.")
            break

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    print(f"\n{'='*70}")
    print(f"Eval DB build complete in {elapsed:.1f}s")
    print(f"  Partitions scanned:    {partitions_scanned:>12,}")
    print(f"  Total games scanned:   {total_games_scanned:>12,}")
    print(f"  Games with eval:       {total_eval_games:>12,}")
    print(f"  Unique positions in DB:{len(eval_db):>12,}")
    if target_size:
        print(f"  Target positions:      {target_size:>12,}")
        print(f"  Coverage:              {len(eval_db)/target_size*100:>11.1f}%")

    # ── Eval distribution summary ────────────────────────────────────────
    if eval_db:
        cps = list(eval_db.values())
        cps.sort()
        n = len(cps)
        print(f"\n  Eval distribution (centipawns):")
        print(f"    min={cps[0]:>6}  p5={cps[int(n*0.05)]:>6}  "
              f"median={cps[n//2]:>6}  p95={cps[int(n*0.95)]:>6}  max={cps[-1]:>6}")
        mate_count = sum(1 for c in cps if abs(c) >= MATE_CP)
        print(f"    Mate positions: {mate_count:,}")

    # ── Write output ─────────────────────────────────────────────────────
    result = pl.DataFrame({
        "position_hash": list(eval_db.keys()),
        "eval_cp":       list(eval_db.values()),
    }).cast({"position_hash": pl.Int64, "eval_cp": pl.Int32})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(".parquet.tmp")
    result.write_parquet(str(tmp), compression="zstd")
    tmp.replace(output_path)
    size_mb = output_path.stat().st_size / 1e6
    print(f"\nWrote {output_path}  ({size_mb:.1f} MB, {len(result):,} rows)")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source", default=str(SOURCE_ROOT),
        help=f"Root of compressed Lichess parquets (default: {SOURCE_ROOT})")
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output parquet path (default: {DEFAULT_OUTPUT})")
    parser.add_argument(
        "--stats", default=None,
        help="Path to Stage 2 stats parquet.  When provided, only extract evals "
             "for positions present in the stats (much faster due to early stopping).")
    parser.add_argument(
        "--max-ply", type=int, default=30,
        help="Maximum ply depth to extract per game (default: 30)")
    parser.add_argument(
        "--year-range", type=int, nargs=2, metavar=("FROM", "TO"),
        help="Limit scan to years FROM..TO inclusive (e.g. 2020 2025)")
    args = parser.parse_args()

    source_root = Path(args.source)
    output_path = Path(args.output)
    year_range  = tuple(args.year_range) if args.year_range else None

    print(f"Source:     {source_root}")
    print(f"Output:     {output_path}")
    print(f"Max ply:    {args.max_ply}")
    print(f"Year range: {year_range or 'all'}")

    target_hashes: set[int] | None = None
    if args.stats:
        print(f"\nLoading target positions from {args.stats}...")
        t = time.time()
        target_hashes = load_target_hashes(Path(args.stats))
        print(f"  {len(target_hashes):,} target positions loaded in {time.time()-t:.1f}s")

    print()
    build_eval_db(source_root, output_path, target_hashes, args.max_ply, year_range)


if __name__ == "__main__":
    main()
