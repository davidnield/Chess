"""Compare row group size choices on identical data + identical zstd level."""
from __future__ import annotations
import shutil, sys, time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent))
from process_pgn_parquets import EVENT_MAP, PARTITION_SCHEMA, WRITE_SCHEMA, transform

SOURCE = Path("D:/data/chess/standard-chess-games/year=2016/month=01")
BENCH  = Path("D:/zstd_bench_2016_01")

# Settings to compare. (label, zstd_level, max_rows_per_group)
TESTS = [
    ("rg_tiny_z3",   3, 1024),       # Roughly mirrors R defaults
    ("rg_1M_z3",     3, 1_000_000),  # My new default at z3
    ("rg_1M_z6",     6, 1_000_000),  # My recommended new default
    ("rg_100k_z3",   3, 100_000),    # Middle ground
]


def preload() -> pa.Table:
    print("Loading source...")
    t0 = time.time()
    ds = pads.dataset(str(SOURCE), format="parquet")
    scanner = ds.scanner(
        filter=pc.field("Event").isin(list(EVENT_MAP.keys())),
        batch_size=500_000,
    )
    parts = []
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        df = pl.from_arrow(batch)
        df = transform(df, 2016, 1)
        parts.append(df.to_arrow().cast(WRITE_SCHEMA))
    table = pa.concat_tables(parts)
    print(f"  {table.num_rows:,} rows, {table.nbytes/1e9:.2f} GB in-memory ({time.time()-t0:.1f}s)")
    return table


def write_one(table: pa.Table, outdir: Path, level: int, max_rg: int) -> float:
    if outdir.exists(): shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    opts = pads.ParquetFileFormat().make_write_options(
        compression="zstd", compression_level=level, version="2.6",
        write_statistics=True,
    )
    t0 = time.time()
    pads.write_dataset(
        table, base_dir=str(outdir), format="parquet",
        partitioning=pads.partitioning(PARTITION_SCHEMA, flavor="hive"),
        max_rows_per_file=2_000_000,
        max_rows_per_group=max_rg,
        min_rows_per_group=max(max_rg // 2, 1),
        file_options=opts,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="part-{i}.parquet",
        use_threads=True,
    )
    return time.time() - t0


def time_read(outdir: Path) -> float:
    t0 = time.time()
    ds = pads.dataset(str(outdir), format="parquet")
    tbl = ds.to_table()
    _ = tbl.num_rows
    for c in tbl.column_names:
        _ = tbl.column(c).nbytes
    return time.time() - t0


def analyse(outdir: Path) -> dict:
    """Break down each file: data_bytes, metadata_bytes, ratio, rg_count."""
    files = sorted(outdir.rglob("*.parquet"))
    out = {"files": len(files), "size": 0, "data": 0, "rg": 0, "rows": 0}
    for f in files:
        size = f.stat().st_size
        out["size"] += size
        pf = pq.ParquetFile(f)
        md = pf.metadata
        out["rows"] += md.num_rows
        out["rg"] += md.num_row_groups
        for rg in range(md.num_row_groups):
            for c in range(md.row_group(rg).num_columns):
                out["data"] += md.row_group(rg).column(c).total_compressed_size
    out["meta"] = out["size"] - out["data"]
    return out


def main():
    BENCH.mkdir(parents=True, exist_ok=True)
    table = preload()

    print()
    hdr = f"{'Test':<14} {'Files':>5} {'RGs':>7} {'Comp':>7} {'Total':>10} {'Data':>10} {'Meta':>9} {'Meta%':>6} {'Read':>6}"
    print(hdr)
    print("─" * len(hdr))
    rows = []
    for label, lvl, max_rg in TESTS:
        outdir = BENCH / label
        comp_t = write_one(table, outdir, lvl, max_rg)
        a = analyse(outdir)
        read_t = time_read(outdir)
        meta_pct = a["meta"] / a["size"] * 100
        print(f"{label:<14} {a['files']:>5} {a['rg']:>7} {comp_t:>6.1f}s "
              f"{a['size']/1e9:>9.3f}G {a['data']/1e9:>9.3f}G {a['meta']/1e6:>7.1f}M "
              f"{meta_pct:>5.1f}% {read_t:>5.1f}s")
        rows.append((label, lvl, max_rg, comp_t, a, read_t))

    # Reference: R's actual Jan 2016 output
    r_path = Path("D:/data/chess/standard-chess-games-compressed/year=2016/month=1")
    if r_path.exists():
        a = analyse(r_path)
        meta_pct = a["meta"] / a["size"] * 100
        print(f"{'R prod (z3)':<14} {a['files']:>5} {a['rg']:>7} {'      ?':>7} "
              f"{a['size']/1e9:>9.3f}G {a['data']/1e9:>9.3f}G {a['meta']/1e6:>7.1f}M "
              f"{meta_pct:>5.1f}% {time_read(r_path):>5.1f}s")

    # Cleanup
    for label, _, _ in TESTS:
        outdir = BENCH / label
        if outdir.exists():
            shutil.rmtree(outdir)


if __name__ == "__main__":
    main()
