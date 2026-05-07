"""
Build a position -> Stockfish eval lookup table from the official Lichess
evaluations dataset (https://database.lichess.org/#evals).

The dataset is hosted on HuggingFace as 17 parquet shards totalling ~40 GB
and ~845M rows.  Each row is one principal variation (PV) for one position at
one analysis depth.  Multiple rows exist per position.

This script:
  1. Downloads the parquet shards (skipping already-downloaded files).
  2. Deduplicates to one evaluation per FEN: highest depth, first PV
     (per the official Lichess recommendation).
  3. Converts each FEN to a Polyglot Zobrist position_hash (signed Int64).
  4. Writes the result as a compact parquet with the same schema that
     Stage 3 already consumes: (position_hash: Int64, eval_cp: Int32).

Usage:
    .venv/Scripts/python.exe python/build_lichess_eval_db.py \\
        --output E:/chess/lichess_eval_db.parquet \\
        --cache-dir E:/chess/lichess-evals \\
        --workers 8

    # Single-shard smoke test:
    .venv/Scripts/python.exe python/build_lichess_eval_db.py \\
        --output E:/chess/lichess_eval_db_test.parquet \\
        --shards 0
"""

from __future__ import annotations

import argparse
import multiprocessing
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import polars as pl
import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# ── Constants ────────────────────────────────────────────────────────────────

HF_SHARD_API = (
    "https://huggingface.co/api/datasets/"
    "Lichess/chess-position-evaluations/parquet/default/train"
)
NUM_SHARDS     = 17
DEFAULT_OUTPUT = Path("E:/chess/lichess_eval_db.parquet")
DEFAULT_CACHE  = Path("E:/chess/lichess-evals")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64
MATE_CP     = 10_000


def zobrist_int64(board: chess.Board) -> int:
    """Polyglot Zobrist hash cast to signed int64 (parquet/polars compatible)."""
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Download ─────────────────────────────────────────────────────────────────

def shard_url(index: int) -> str:
    return f"{HF_SHARD_API}/{index}.parquet"


def download_shard(index: int, cache_dir: Path) -> Path:
    """Download a single HuggingFace parquet shard if not already cached."""
    dest = cache_dir / f"shard_{index:02d}.parquet"
    url  = shard_url(index)

    if dest.exists():
        # Quick size check: if file is reasonably large, assume complete
        if dest.stat().st_size > 100_000_000:  # > 100 MB
            return dest
        # Otherwise re-download (partial/corrupt)

    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.tmp")

    print(f"  Downloading shard {index} ...")
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()

    total = int(resp.headers.get("Content-Length", 0))
    downloaded = 0
    t0 = time.time()

    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = downloaded / total * 100
                rate = downloaded / (time.time() - t0 + 0.001) / 1e6
                print(f"\r    {pct:5.1f}%  ({downloaded/1e9:.2f} / {total/1e9:.2f} GB"
                      f"  @ {rate:.0f} MB/s)", end="", flush=True)
    print()  # newline after progress

    tmp.rename(dest)
    elapsed = time.time() - t0
    print(f"    Saved {dest.name}  ({dest.stat().st_size/1e9:.2f} GB in {elapsed:.0f}s)")
    return dest


def download_all_shards(
    shard_indices: list[int],
    cache_dir: Path,
) -> list[Path]:
    """Download requested shards, returning local file paths."""
    paths: list[Path] = []
    for i in shard_indices:
        paths.append(download_shard(i, cache_dir))
    return paths


# ── Per-shard deduplication ──────────────────────────────────────────────────

def dedup_shard(path: Path) -> pl.DataFrame:
    """Read a shard and deduplicate to one eval per FEN (highest depth).

    Returns a DataFrame with columns [fen, depth, cp, mate] — one row per
    unique FEN in this shard, with the highest-depth evaluation.
    """
    df = pl.read_parquet(
        str(path),
        columns=["fen", "depth", "cp", "mate"],
    )
    # Sort by depth descending so that group_by().first() picks the deepest
    return (
        df.sort("depth", descending=True)
        .group_by("fen")
        .first()
    )


# ── FEN -> position_hash conversion (parallelisable) ────────────────────────

def _fen_chunk_to_hashes(fens: list[str]) -> list[int]:
    """Convert a list of FEN strings to Zobrist hashes. Worker function."""
    board = chess.Board()
    hashes = []
    for fen in fens:
        board.set_fen(fen)
        hashes.append(zobrist_int64(board))
    return hashes


def convert_fens_to_hashes(
    fens: list[str],
    n_workers: int = 1,
    chunk_size: int = 50_000,
) -> list[int]:
    """Convert FEN strings to Zobrist hashes, optionally in parallel."""
    if n_workers <= 1:
        return _fen_chunk_to_hashes(fens)

    # Split into chunks for multiprocessing
    chunks = [fens[i:i + chunk_size] for i in range(0, len(fens), chunk_size)]
    print(f"  Converting {len(fens):,} FENs using {n_workers} workers "
          f"({len(chunks)} chunks of ~{chunk_size:,}) ...")

    t0 = time.time()
    with multiprocessing.Pool(n_workers) as pool:
        results = []
        for i, batch in enumerate(pool.imap(_fen_chunk_to_hashes, chunks)):
            results.extend(batch)
            done = len(results)
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(fens) - done) / rate if rate > 0 else 0
            print(f"\r    {done:>12,} / {len(fens):,}  "
                  f"({done/len(fens)*100:.1f}%)  "
                  f"{rate:,.0f} FEN/s  ETA {eta/60:.1f}m",
                  end="", flush=True)
    print()
    return results


# ── Main build pipeline ──────────────────────────────────────────────────────

def build_lichess_eval_db(
    shard_indices: list[int],
    cache_dir: Path,
    output_path: Path,
    n_workers: int,
    compare_path: Path | None,
) -> None:
    t_total = time.time()

    # ── Step 1: Download shards ──────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 1: Download {len(shard_indices)} shard(s) to {cache_dir}")
    print(f"{'='*70}")
    shard_paths = download_all_shards(shard_indices, cache_dir)

    # ── Step 2: Per-shard deduplication ──────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 2: Deduplicate each shard (highest depth per FEN)")
    print(f"{'='*70}")
    deduped_frames: list[pl.DataFrame] = []
    for i, path in enumerate(shard_paths):
        t0 = time.time()
        df = dedup_shard(path)
        elapsed = time.time() - t0
        print(f"  Shard {shard_indices[i]:>2}: {df.height:>12,} unique FENs  ({elapsed:.1f}s)")
        deduped_frames.append(df)

    # ── Step 3: Cross-shard deduplication ────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 3: Cross-shard deduplication")
    print(f"{'='*70}")
    t0 = time.time()
    combined = pl.concat(deduped_frames)
    print(f"  Combined: {combined.height:,} rows")

    # Free per-shard frames
    del deduped_frames

    # Final dedup: highest depth wins across shards
    unique = (
        combined.sort("depth", descending=True)
        .group_by("fen")
        .first()
    )
    del combined
    print(f"  After final dedup: {unique.height:,} unique FENs  ({time.time()-t0:.1f}s)")

    # ── Step 4: Handle mate scores ───────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 4: Resolve mate scores and compute eval_cp")
    print(f"{'='*70}")
    t0 = time.time()

    # Where cp is null but mate is not null, compute cp from mate.
    # mate > 0 -> white mates -> +MATE_CP
    # mate < 0 -> black mates -> -MATE_CP
    # The dataset uses cp values up to ±20000 for very lopsided positions;
    # clamp to ±MATE_CP (10_000) for consistency with the rest of our pipeline.
    unique = unique.with_columns(
        pl.when(pl.col("cp").is_not_null())
        .then(pl.col("cp").cast(pl.Int32).clip(-MATE_CP, MATE_CP))
        .when(pl.col("mate") > 0)
        .then(pl.lit(MATE_CP, dtype=pl.Int32))
        .when(pl.col("mate") < 0)
        .then(pl.lit(-MATE_CP, dtype=pl.Int32))
        .otherwise(pl.lit(0, dtype=pl.Int32))
        .alias("eval_cp")
    )

    n_cp   = unique.filter(pl.col("cp").is_not_null()).height
    n_mate = unique.filter(pl.col("mate").is_not_null() & pl.col("cp").is_null()).height
    n_null = unique.filter(pl.col("cp").is_null() & pl.col("mate").is_null()).height
    print(f"  Positions with cp:   {n_cp:>12,}")
    print(f"  Positions with mate: {n_mate:>12,}")
    print(f"  Both null:           {n_null:>12,}")
    print(f"  ({time.time()-t0:.1f}s)")

    # ── Step 5: FEN -> position_hash ─────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 5: Convert FEN -> position_hash ({unique.height:,} FENs)")
    print(f"{'='*70}")
    t0 = time.time()

    fens = unique["fen"].to_list()
    hashes = convert_fens_to_hashes(fens, n_workers=n_workers)

    unique = unique.with_columns(
        pl.Series("position_hash", hashes, dtype=pl.Int64)
    )
    elapsed = time.time() - t0
    print(f"  Hashing complete: {elapsed:.1f}s "
          f"({len(fens)/elapsed:,.0f} FEN/s)")

    # Check for hash collisions
    n_unique_hashes = unique["position_hash"].n_unique()
    n_fens = unique.height
    if n_unique_hashes < n_fens:
        print(f"  WARNING: {n_fens - n_unique_hashes:,} hash collisions detected!")
        print(f"  Deduplicating by position_hash (keeping highest depth) ...")
        unique = (
            unique.sort("depth", descending=True)
            .group_by("position_hash")
            .first()
        )
        print(f"  After hash dedup: {unique.height:,} rows")

    # ── Step 6: Write output ─────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Step 6: Write output")
    print(f"{'='*70}")

    result = unique.select([
        pl.col("position_hash"),
        pl.col("eval_cp"),
    ])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(".parquet.tmp")
    result.write_parquet(str(tmp), compression="zstd")
    tmp.rename(output_path)
    size_mb = output_path.stat().st_size / 1e6
    print(f"  Wrote {output_path}  ({size_mb:.1f} MB, {result.height:,} rows)")

    # ── Eval distribution summary ────────────────────────────────────────
    print(f"\n  Eval distribution (centipawns):")
    desc = result["eval_cp"].describe()
    print(f"    min={result['eval_cp'].min()}  "
          f"median={result['eval_cp'].median():.0f}  "
          f"max={result['eval_cp'].max()}")
    mate_count = result.filter(pl.col("eval_cp").abs() >= MATE_CP).height
    print(f"    Mate positions: {mate_count:,}")

    # ── Step 7: Compare against existing eval DB (if requested) ──────────
    if compare_path and compare_path.exists():
        print(f"\n{'='*70}")
        print(f"Step 7: Compare against {compare_path}")
        print(f"{'='*70}")
        compare_eval_dbs(result, compare_path)

    elapsed_total = time.time() - t_total
    print(f"\nTotal time: {elapsed_total/60:.1f} minutes")


# ── Comparison ───────────────────────────────────────────────────────────────

def compare_eval_dbs(new_db: pl.DataFrame, old_path: Path) -> None:
    """Compare the new Lichess eval DB against the existing game-annotation DB."""
    old = pl.read_parquet(str(old_path))
    print(f"  Old DB: {old.height:,} rows")
    print(f"  New DB: {new_db.height:,} rows")

    # Join on position_hash
    joined = old.join(new_db, on="position_hash", suffix="_new")
    n_common = joined.height
    n_old_only = old.height - n_common
    pct_covered = n_common / old.height * 100

    print(f"\n  Coverage:")
    print(f"    Common positions:     {n_common:>10,} ({pct_covered:.1f}% of old DB)")
    print(f"    Old-only (missing):   {n_old_only:>10,} ({100-pct_covered:.1f}%)")

    if n_common > 0:
        # Centipawn comparison
        diffs = (joined["eval_cp"] - joined["eval_cp_new"]).abs()
        print(f"\n  Centipawn differences (|old_cp - new_cp|) for {n_common:,} common positions:")
        print(f"    mean:   {diffs.mean():.1f}")
        print(f"    median: {diffs.median():.1f}")
        p95 = diffs.quantile(0.95)
        print(f"    p95:    {p95:.1f}")
        print(f"    max:    {diffs.max()}")

        # Exact match rate
        exact = diffs.filter(diffs == 0).len()
        close = diffs.filter(diffs <= 10).len()
        print(f"    Exact match (diff=0):  {exact:,} ({exact/n_common*100:.1f}%)")
        print(f"    Close (diff<=10cp):    {close:,} ({close/n_common*100:.1f}%)")

        # Mate agreement
        old_mate = joined.filter(pl.col("eval_cp").abs() >= MATE_CP)
        if old_mate.height > 0:
            mate_agree = old_mate.filter(pl.col("eval_cp_new").abs() >= MATE_CP).height
            print(f"\n  Mate agreement:")
            print(f"    Old DB mate positions: {old_mate.height:,}")
            print(f"    Also mate in new DB:   {mate_agree:,} "
                  f"({mate_agree/old_mate.height*100:.1f}%)")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output parquet path (default: {DEFAULT_OUTPUT})")
    parser.add_argument(
        "--cache-dir", default=str(DEFAULT_CACHE),
        help=f"Directory to cache downloaded shards (default: {DEFAULT_CACHE})")
    parser.add_argument(
        "--shards", type=int, nargs="*", default=None,
        help="Specific shard indices to process (0-16). Default: all 17 shards.")
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers for FEN->hash conversion (default: 1)")
    parser.add_argument(
        "--compare", default=None,
        help="Path to existing eval DB parquet to compare against")
    args = parser.parse_args()

    output_path = Path(args.output)
    cache_dir   = Path(args.cache_dir)
    compare_path = Path(args.compare) if args.compare else None

    shard_indices = args.shards if args.shards is not None else list(range(NUM_SHARDS))

    print(f"Output:       {output_path}")
    print(f"Cache dir:    {cache_dir}")
    print(f"Shards:       {shard_indices}")
    print(f"Workers:      {args.workers}")
    if compare_path:
        print(f"Compare with: {compare_path}")

    build_lichess_eval_db(
        shard_indices=shard_indices,
        cache_dir=cache_dir,
        output_path=output_path,
        n_workers=args.workers,
        compare_path=compare_path,
    )


if __name__ == "__main__":
    main()
