"""
Build a position -> Stockfish eval lookup table from the official Lichess
evaluations dataset (https://database.lichess.org/#evals).

The dataset is hosted on HuggingFace as parquet shards (20 as of the 2026-06
refresh, ~46 GB) auto-partitioned by HF; the count grows as Lichess adds data, so
NUM_SHARDS / the HF parquet API listing is the source of truth.  Each row is one
principal variation (PV) for one position at
one analysis depth.  Multiple rows exist per position.

This script:
  1. Downloads the parquet shards (skipping already-downloaded files).
  2. Aggregates the multiPV rows to one eval per FEN (DuckDB) with Lichess's
     recommended logic: the PRINCIPAL PV at the HIGHEST depth — i.e. among the
     deepest rows, the best line for the side to move (max if White to move, min
     if Black), with MATE PVs ranked above/below every cp so a position whose
     best line is a forced mate scores +-EVAL_CAP, not the runner-up PV's cp
     (the old prefer-cp logic was mate-blind — see aggregate_evals). Cap
     +-EVAL_CAP; carry n_rows. (A median over all PVs was wrong: it blended
     the best move with the engine's inferior lines.)
  3. Converts each FEN to a Polyglot Zobrist position_hash (signed Int64), then
     collapses FENs that share a hash (encoding variants + the odd troll FEN that
     collides with a real position) by keeping the best-supported (max n_rows).
  4. Writes the result as a compact parquet with the same schema that Stage 3
     already consumes: (position_hash: Int64, eval_cp: Int32). Decisive evals
     (winning cp and genuine mates) are capped at +-EVAL_CAP rather than the old
     +-10000 mate sentinel, so a "winning" eval no longer trivially passes
     Stage 3's refutation gate or dominates the eval blend.

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
import shutil
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import duckdb
import polars as pl
import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# ── Constants ────────────────────────────────────────────────────────────────

HF_SHARD_API = (
    "https://huggingface.co/api/datasets/"
    "Lichess/chess-position-evaluations/parquet/default/train"
)
NUM_SHARDS     = 20
DEFAULT_OUTPUT = Path("E:/chess/lichess_eval_db.parquet")
DEFAULT_CACHE  = Path("E:/chess/lichess-evals")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64
MATE_CP     = 10_000   # legacy sentinel; only used by compare_eval_dbs() for old DBs
# Cap magnitude for decisive evals (real cp and genuine mates). At +-2000cp the
# Lichess sigmoid (LICHESS_CP_SCALE) gives ~0.9994 / 0.0006 expected score —
# "decisively winning/losing" — without an unbreakable +-10000 mate sentinel
# that defeats Stage 3's refutation gate or dominates the leaf eval blend.
EVAL_CAP    = 2_000


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

    tmp.replace(dest)  # .replace (not .rename) — overwrites a partial dest on Windows
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


# ── Robust per-FEN aggregation ───────────────────────────────────────────────

def aggregate_fens_duckdb(shard_paths: list[Path], out_parquet: Path,
                          tmp_dir: Path, memory_limit: str = "90GB",
                          threads: int = 8) -> None:
    """Aggregate ALL raw eval rows to one (fen, eval_cp, n_rows) per FEN using
    Lichess's recommended logic: the PRINCIPAL PV at the HIGHEST depth.

    The raw rows are multiPV — for one (fen, depth) analysis there are several
    rows, one per candidate move (the engine's top-N lines). The position's eval
    is pvs[0]: the BEST move for the side to move. So among the deepest cp rows we
    take max(cp) if White is to move, min(cp) if Black is to move. (An earlier
    median-over-all-PVs blended the best move with inferior ones — e.g. after
    1.d4 e5 2.dxe5 Nc6 3.Nf3 Nxe5 the best PV is +377 (Nxe5 wins the knight) but
    the median across PVs was +47, the eval of NOT taking it.)

    - prefer cp: cp rows decide whenever any exist; a FEN with only mate rows
      falls back to the SIGN of its net mate vote -> +-EVAL_CAP.
    - cap at +-EVAL_CAP.
    - n_rows (raw row support) is carried so the later position_hash collision
      dedup keeps the well-sampled real position over a thin colliding troll FEN.

    DuckDB with spill does the 845M-row -> ~342M-group aggregation without OOM.
    Output parquet columns: [fen, eval_cp(Int32), n_rows(Int64), n_cp(Int64)].
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    files = "[" + ", ".join("'" + str(p).replace("\\", "/") + "'"
                            for p in shard_paths) + "]"
    out = str(out_parquet).replace("\\", "/")
    mate_vote = "SUM(CASE WHEN mate > 0 THEN 1 WHEN mate < 0 THEN -1 ELSE 0 END)"
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}';")
    con.execute(f"SET temp_directory='{str(tmp_dir).replace(chr(92), '/')}';")
    con.execute(f"SET threads={threads};")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    con.execute(f"""
        COPY (
            WITH src AS (
                SELECT fen, depth, cp, mate,
                       (split_part(fen, ' ', 2) = 'w') AS wtm,
                       -- Order key making mate PVs comparable with cp PVs (white
                       -- POV): a mate FOR white outranks any cp (shorter mates
                       -- higher), a mate AGAINST white underranks any cp. The old
                       -- prefer-cp logic ('cp rows decide whenever any exist')
                       -- kept the best NON-mating PV's cp at positions whose
                       -- principal line is a mate — e.g. the Scholar's-mate-in-1
                       -- node scored -128, a mate-in-1-against scored +143 —
                       -- systematically defeating Stage 3's refutation gate at
                       -- exactly the mate-adjacent trap positions it exists for
                       -- (1,177 in-DAG sign-flips vs the fishnet median, found
                       -- 2026-07-12). mate=0 = side to move already mated.
                       CASE WHEN mate > 0 THEN  1000000 - mate
                            WHEN mate < 0 THEN -1000000 - mate
                            WHEN mate = 0 THEN CASE WHEN (split_part(fen, ' ', 2) = 'w')
                                                    THEN -1000000 ELSE 1000000 END
                            ELSE cp END AS ord
                FROM read_parquet({files})
            ),
            per AS (
                SELECT fen,
                       COUNT(*)                          AS n_rows,
                       COUNT(cp)                         AS n_cp,
                       any_value(wtm)                    AS wtm,
                       MAX(depth) FILTER (WHERE ord IS NOT NULL) AS md,
                       {mate_vote}                       AS mate_net
                FROM src GROUP BY fen
            ),
            -- principal PV at the deepest analysis: best line for the side to
            -- move among rows at that FEN's max depth, mates included.
            best AS (
                SELECT s.fen,
                       MAX(s.ord) AS max_o, MIN(s.ord) AS min_o
                FROM src s JOIN per p
                  ON s.fen = p.fen AND s.depth = p.md AND s.ord IS NOT NULL
                GROUP BY s.fen
            )
            SELECT
                p.fen,
                CAST(
                  CASE
                    WHEN b.fen IS NOT NULL THEN GREATEST(-{EVAL_CAP}, LEAST({EVAL_CAP},
                         CASE WHEN p.wtm THEN b.max_o ELSE b.min_o END))
                    WHEN p.mate_net > 0 THEN  {EVAL_CAP}
                    WHEN p.mate_net < 0 THEN -{EVAL_CAP}
                    ELSE 0
                  END AS INTEGER)        AS eval_cp,
                p.n_rows::BIGINT         AS n_rows,
                p.n_cp::BIGINT           AS n_cp
            FROM per p LEFT JOIN best b ON p.fen = b.fen
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()


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

    # ── Steps 2-4: robust per-FEN aggregation (DuckDB; deepest-best-PV cp + n_rows) ─
    print(f"\n{'='*70}")
    print(f"Steps 2-4: Robust per-FEN aggregation (deepest-best-PV cp, capped, + n_rows)")
    print(f"{'='*70}")
    t0 = time.time()
    agg_parquet = output_path.with_name(output_path.stem + "._fenagg.parquet")
    duckdb_tmp  = cache_dir / "_eval_duckdb_tmp"
    # Skip-gate: the aggregation is the expensive step (~2.5 h) and writes a durable
    # intermediate that is only unlinked on full success (Step 6). If a later step
    # (e.g. the Step 5 hashing) crashes, that intermediate survives — reuse it on the
    # next run instead of re-aggregating from scratch.
    if agg_parquet.exists():
        print(f"  Reusing FEN-aggregation checkpoint {agg_parquet.name} "
              f"({agg_parquet.stat().st_size/1e9:.1f} GB) — skipping re-aggregation.")
    else:
        aggregate_fens_duckdb(shard_paths, agg_parquet, duckdb_tmp, threads=n_workers)
    unique = pl.read_parquet(agg_parquet)   # [fen, eval_cp, n_rows, n_cp]
    n_cp_pos   = unique.filter(pl.col("n_cp") > 0).height
    print(f"  Aggregated to {unique.height:,} unique FENs "
          f"({n_cp_pos:,} with cp, {unique.height - n_cp_pos:,} mate-only) "
          f"in {(time.time()-t0)/60:.1f} min")

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

    # Collapse FENs that share a position_hash. These are overwhelmingly the
    # SAME position under different encodings (move counters, non-capturable ep)
    # plus the occasional fabricated/troll FEN that collides with a real one.
    # Keep the BEST-SUPPORTED variant (max n_rows) — the real position dominates
    # row count, so a 1-row troll FEN can no longer overwrite the start's eval
    # the way the old depth-only pick did.
    n_unique_hashes = unique["position_hash"].n_unique()
    n_fens = unique.height
    if n_unique_hashes < n_fens:
        print(f"  {n_fens - n_unique_hashes:,} FENs share a position_hash; "
              f"keeping the best-supported (max n_rows) per hash ...")
        unique = (
            unique.sort("n_rows", descending=True)
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
    tmp.replace(output_path)  # .replace (not .rename) — overwrites on Windows too
    size_mb = output_path.stat().st_size / 1e6
    print(f"  Wrote {output_path}  ({size_mb:.1f} MB, {result.height:,} rows)")

    # Drop the intermediate FEN-aggregation parquet + DuckDB spill dir.
    try:
        if agg_parquet.exists():
            agg_parquet.unlink()
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
    except OSError:
        pass

    # ── Eval distribution summary ────────────────────────────────────────
    print(f"\n  Eval distribution (centipawns):")
    desc = result["eval_cp"].describe()
    print(f"    min={result['eval_cp'].min()}  "
          f"median={result['eval_cp'].median():.0f}  "
          f"max={result['eval_cp'].max()}")
    decisive_count = result.filter(pl.col("eval_cp").abs() >= EVAL_CAP).height
    print(f"    Decisive (|eval| >= {EVAL_CAP}cp, capped): {decisive_count:,} "
          f"({decisive_count/result.height*100:.2f}%)")

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
        help=f"Specific shard indices to process (0..{NUM_SHARDS - 1}). "
             f"Default: all {NUM_SHARDS} shards.")
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Parallel workers for the FEN->hash conversion AND the DuckDB "
             "aggregation thread count (default: 1; the full build uses 8).")
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
