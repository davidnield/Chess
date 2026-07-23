"""
benchmark_compression_matrix.py — parquet recipe benchmark for the 2026-07 ingest (Phases 0-2).

Benchmarks (codec, level) x row-group size x data-page size x encodings x sort order x
movetext preprocessing on staged raw months, using the PRODUCTION transform imported from
process_pgn_parquets.py — so what's measured is exactly what production would write.
Successor to benchmark_zstd_levels.py / compare_row_group_sizes.py (kept for history).

Datasets are staged raw HuggingFace files under E:/bench/src/<id>/ (see the plan; F: is a
slow USB disk, so benchmarks never read it). Each config writes to E:/bench/out/<run>/ in
production shape — per-event subdirs, files capped at MAX_ROWS_PER_FILE rows, partition
columns stripped — then measures size, per-column compressed bytes, and consumer-shaped
read timings. Results append to E:/bench/parquet_recipe_202607/results.csv; a run whose
key is already present is skipped (resume-by-CSV).

Axes:
  codec/level   none | zstd 1-22 | brotli 1-11
  rg            max rows per row group (prod: 1,000,000)
  page_kb       parquet data page size (prod/pyarrow default: 1024 KB) — pages are the
                real compression window for long movetext
  enc           E0 defaults | E1 dict OFF for high-cardinality strings | E2 = E1 minus
                dictionaries everywhere + DELTA_BYTE_ARRAY on site/game_id (+movetext
                when sorted by it)
  sort          S0 natural | S1 mean_elo | S2 (eco, opening, movetext) | S3
                (mean_elo//200, eco, movetext); scope 'global' (whole month, emulates a
                DuckDB external sort) or 'perfile' (within each 2M-row output file —
                the bounded-memory streaming strategy)
  movetext      M0 as-is | M1 brace comments stripped (%clk/%eval) — matches what
                iter_san_moves discards at read time today; has_eval is computed by the
                transform BEFORE stripping

Usage:
    .venv/Scripts/python.exe python/benchmark_compression_matrix.py --phase baseline \
        --dataset m2016_01 s2025_03
    .venv/Scripts/python.exe python/benchmark_compression_matrix.py --phase 1 \
        --dataset m2016_01 s2025_03
    .venv/Scripts/python.exe python/benchmark_compression_matrix.py --phase 2a \
        --dataset m2016_01 m2019_06 s2025_03 --codec zstd --level 15 [--max-files 30]
    # --only substring filters configs; --list prints the configs a phase would run.
"""
from __future__ import annotations

import argparse
import csv
import gc
import json
import statistics
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from process_pgn_parquets import EVENT_MAP, MAX_ROWS_PER_FILE, WRITE_SCHEMA, transform

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SRC_ROOT   = Path("E:/bench/src")
OUT_ROOT   = Path("E:/bench/out")
RESULT_DIR = Path("E:/bench/parquet_recipe_202607")
RESULTS    = RESULT_DIR / "results.csv"

# (year, month) per staged dataset id — transform() wants them for partition cols.
DATASETS = {"m2016_01": (2016, 1), "m2019_06": (2019, 6),
            "s2025_03": (2025, 3), "m2025_03": (2025, 3)}

DATA_COLUMNS = [n for n in WRITE_SCHEMA.names if n not in ("year", "month", "event")]

# Consumer-shaped projections (from build_pooled_stats / extract_crush_per_game /
# stage1_extract_positions SRC/INPUT_COLUMNS).
PROJECTIONS = {
    "pooled": ["movetext", "white_score", "termination", "move_count", "mean_elo"],
    "crush":  ["game_id", "white_score", "termination", "move_count"],
    "stage1": ["game_id", "movetext", "white_score", "elo_band", "mean_elo"],
}

HIGH_CARD_STRINGS = ["site", "white", "black", "movetext", "game_id"]
READ_REPS = 3

# ── configs ──────────────────────────────────────────────────────────────────


def cfg(codec, level, rg=1_000_000, page_kb=1024, enc="E0", sort="S0",
        scope="global", movetext="M0"):
    return {"codec": codec, "level": level, "rg": rg, "page_kb": page_kb,
            "enc": enc, "sort": sort, "scope": scope, "movetext": movetext}


def phase_configs(phase: str, codec: str, level: int) -> list[dict]:
    if phase == "1":
        return ([cfg("none", 0)]
                + [cfg("zstd", l) for l in (3, 6, 9, 12, 15, 19, 22)]
                + [cfg("brotli", l) for l in (5, 9, 11)])
    if phase == "2a":
        return [cfg(codec, level, enc=e) for e in ("E0", "E1", "E2")] + [
            cfg(codec, level, enc="E1", page_kb=8192),
            cfg(codec, level, enc="E1", page_kb=16384),
        ]
    if phase == "2b":
        out = []
        for s in ("S0", "S1", "S2", "S3"):
            for m in ("M0", "M1"):
                out.append(cfg(codec, level, sort=s, movetext=m))
        # per-file scope for the sort winner candidates (strategy-A ratio gap)
        out += [cfg(codec, level, sort=s, scope="perfile") for s in ("S2", "S3")]
        return out
    if phase == "2c":
        return [cfg(codec, level, rg=r, sort="S1")
                for r in (250_000, 500_000, 1_000_000)]
    raise SystemExit(f"unknown phase {phase!r}")


def config_key(dataset: str, c: dict, max_files: int | None) -> str:
    # max_files is part of the identity: a capped run and a full-size run of the
    # same dataset must never collide under the resume-by-CSV-key skip logic.
    mf = f"mf{max_files}" if max_files else "mfAll"
    return (f"{dataset}|{mf}|{c['codec']}-{c['level']}|rg{c['rg']}|pg{c['page_kb']}"
            f"|{c['enc']}|{c['sort']}-{c['scope']}|{c['movetext']}")


# ── dataset loading (once per dataset, production transform) ────────────────


def load_dataset(ds_id: str, max_files: int | None) -> pa.Table:
    """Raw staged files -> Event filter -> production transform -> in-memory table
    (27 data columns + event; partition year/month dropped)."""
    src = SRC_ROOT / ds_id
    files = sorted(str(p) for p in src.glob("*.parquet"))
    if not files:
        raise SystemExit(f"FATAL: no staged files under {src} — run the staging step first.")
    if max_files:
        files = files[:max_files]
    year, month = DATASETS[ds_id]
    t0 = time.time()
    scanner = pads.dataset(files, format="parquet").scanner(
        filter=pc.field("Event").isin(list(EVENT_MAP.keys())), batch_size=500_000)
    parts = []
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        df = transform(pl.from_arrow(batch), year, month)
        parts.append(df.to_arrow().cast(WRITE_SCHEMA))
    tbl = pa.concat_tables(parts).combine_chunks()
    tbl = tbl.drop_columns(["year", "month"])
    # 64-bit string offsets: sort_by/take must materialize single-chunk columns,
    # and movetext alone exceeds the 2 GB 32-bit-offset limit on recent months
    # ("offset overflow while concatenating arrays"). Parquet output is identical
    # either way (BYTE_ARRAY has no 32/64 distinction).
    tbl = tbl.cast(pa.schema([
        pa.field(f.name, pa.large_string()) if f.type == pa.string() else f
        for f in tbl.schema]))
    print(f"  loaded {ds_id}: {tbl.num_rows:,} rows, {tbl.nbytes/1e9:.2f} GB in-memory "
          f"({len(files)} files, {time.time()-t0:.0f}s)", flush=True)
    return tbl


def strip_movetext(tbl: pa.Table) -> pa.Table:
    """M1: drop brace comments ({[%clk ...]}, {[%eval ...]}, any annotation) and the
    '1...' black-move renumbering they force, then collapse runs of spaces. Matches
    what iter_san_moves discards at parse time (has_eval was already computed)."""
    df = pl.from_arrow(tbl.select(["movetext"]))
    clean = (df["movetext"]
             .str.replace_all(r"\s*\{[^}]*\}", "")
             .str.replace_all(r"\d+\.\.\.\s*", "")
             .str.replace_all(r"\s{2,}", " ")
             .str.strip_chars())
    i = tbl.schema.get_field_index("movetext")
    col = pl.DataFrame({"m": clean}).to_arrow()["m"].cast(pa.large_string())
    return tbl.set_column(i, "movetext", col)


SORT_KEYS = {
    "S0": None,
    "S1": [("mean_elo", "ascending")],
    "S2": [("eco", "ascending"), ("opening", "ascending"), ("movetext", "ascending")],
    "S3": [("_elo200", "ascending"), ("eco", "ascending"), ("movetext", "ascending")],
}


def sorted_table(tbl: pa.Table, sort: str) -> pa.Table:
    keys = SORT_KEYS[sort]
    if keys is None:
        return tbl
    if sort == "S3":
        band = pc.multiply(pc.floor(pc.divide(pc.cast(tbl["mean_elo"], pa.float64()), 200)),
                           pa.scalar(200.0))
        tbl = tbl.append_column("_elo200", pc.cast(band, pa.int16()))
    out = tbl.sort_by(keys)
    return out.drop_columns(["_elo200"]) if sort == "S3" else out


# ── writing one config ───────────────────────────────────────────────────────


def write_options(c: dict, sorted_by_movetext: bool) -> dict:
    kw: dict = {"version": "2.6", "write_statistics": True,
                "data_page_size": c["page_kb"] * 1024,
                "row_group_size": c["rg"]}
    if c["codec"] != "none":
        kw["compression"] = c["codec"]
        kw["compression_level"] = c["level"]
    else:
        kw["compression"] = "none"
    if c["enc"] == "E1":
        kw["use_dictionary"] = [n for n in DATA_COLUMNS if n not in HIGH_CARD_STRINGS]
    elif c["enc"] == "E2":
        kw["use_dictionary"] = False
        enc = {"site": "DELTA_BYTE_ARRAY", "game_id": "DELTA_BYTE_ARRAY"}
        if sorted_by_movetext:
            enc["movetext"] = "DELTA_BYTE_ARRAY"
        kw["column_encoding"] = enc
    return kw


def run_config(ds_id: str, base: pa.Table, m1: pa.Table | None, c: dict,
               max_files: int | None) -> dict:
    out_dir = OUT_ROOT / config_key(ds_id, c, max_files).replace("|", "_")
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    tbl = m1 if c["movetext"] == "M1" else base
    kw = write_options(c, sorted_by_movetext=c["sort"] in ("S2", "S3"))

    # Prep (sort) is timed separately from the write: production pays it differently
    # (streaming per-file vs external sort); Phase 3 measures the real ingest wall.
    t0 = time.time()
    events = tbl["event"].unique().to_pylist()
    per_event = {}
    for ev in sorted(events):
        sub = tbl.filter(pc.equal(tbl["event"], ev)).drop_columns(["event"])
        if c["scope"] == "global":
            sub = sorted_table(sub, c["sort"])
        per_event[ev] = sub
    prep_s = time.time() - t0

    t0 = time.time()
    n_written = 0
    for ev, sub in per_event.items():
        ev_dir = out_dir / f"event={ev}"
        ev_dir.mkdir()
        for i in range(0, sub.num_rows, MAX_ROWS_PER_FILE):
            sl = sub.slice(i, MAX_ROWS_PER_FILE)
            if c["scope"] == "perfile":
                sl = sorted_table(sl, c["sort"])
            pq.write_table(sl, ev_dir / f"part-{i // MAX_ROWS_PER_FILE}.parquet", **kw)
            n_written += sl.num_rows
    write_s = time.time() - t0
    assert n_written == tbl.num_rows

    # ── size + metadata accounting ──
    files = list(out_dir.rglob("*.parquet"))
    size = sum(f.stat().st_size for f in files)
    col_bytes: dict[str, int] = {}
    n_rg = 0
    skippable_rows = 0
    for f in files:
        md = pq.ParquetFile(f).metadata
        n_rg += md.num_row_groups
        elo_idx = next(i for i in range(md.num_columns)
                       if md.schema.column(i).name == "mean_elo")
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for ci in range(rg.num_columns):
                col = rg.column(ci)
                name = md.schema.column(ci).name
                col_bytes[name] = col_bytes.get(name, 0) + col.total_compressed_size
            st = rg.column(elo_idx).statistics
            if st is not None and st.has_min_max and st.max < 1800:
                skippable_rows += rg.num_rows

    # ── consumer-shaped reads (warm, median of READ_REPS) ──
    def timed_read(fn):
        ts = []
        for _ in range(READ_REPS):
            t = time.time()
            got = fn()
            _ = got.nbytes           # force full materialization
            ts.append(time.time() - t)
            del got
        return statistics.median(ts)

    ds = pads.dataset(str(out_dir), format="parquet", partitioning="hive")
    reads = {"full": timed_read(lambda: ds.to_table())}
    for name, cols in PROJECTIONS.items():
        reads[name] = timed_read(lambda cols=cols: ds.to_table(columns=cols))
    reads["pooled_flt"] = timed_read(
        lambda: ds.to_table(columns=PROJECTIONS["pooled"],
                            filter=pc.field("mean_elo") >= 1800))

    row = {"key": config_key(ds_id, c, max_files), "dataset": ds_id, **c,
           "rows": tbl.num_rows, "prep_s": round(prep_s, 1),
           "write_s": round(write_s, 1), "size_bytes": size,
           "bytes_per_game": round(size / tbl.num_rows, 1),
           "n_row_groups": n_rg,
           "skippable_row_frac": round(skippable_rows / tbl.num_rows, 4),
           "movetext_bytes": col_bytes.get("movetext", 0),
           "col_bytes": json.dumps(col_bytes),
           **{f"read_{k}_s": round(v, 2) for k, v in reads.items()}}

    import shutil
    shutil.rmtree(out_dir)               # outputs are disposable; CSV is the record
    return row


# ── baseline (phase 0) ───────────────────────────────────────────────────────


def baseline(ds_id: str, max_files: int | None) -> None:
    src = SRC_ROOT / ds_id
    files = sorted(str(p) for p in src.glob("*.parquet"))
    if max_files:
        files = files[:max_files]
    size = sum(Path(f).stat().st_size for f in files)
    t0 = time.time()
    tbl = pads.dataset(files, format="parquet").to_table()
    secs = time.time() - t0
    print(f"[baseline {ds_id}] raw: {len(files)} files, {size/1e9:.2f} GB on disk, "
          f"{tbl.num_rows:,} rows, {tbl.nbytes/1e9:.2f} GB uncompressed; full scan "
          f"{secs:.1f}s = {tbl.nbytes/1e6/secs:.0f} MB/s uncompressed-equivalent "
          f"(the 1/4-gate denominator)", flush=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULT_DIR / f"baseline_{ds_id}.json", "w", encoding="utf-8") as f:
        json.dump({"dataset": ds_id, "raw_files": len(files), "raw_bytes": size,
                   "rows": tbl.num_rows, "uncompressed_bytes": tbl.nbytes,
                   "full_scan_s": round(secs, 1),
                   "scan_mb_s": round(tbl.nbytes / 1e6 / secs, 0)}, f, indent=2)
    del tbl
    gc.collect()


# ── main ─────────────────────────────────────────────────────────────────────


def existing_keys() -> set[str]:
    if not RESULTS.exists():
        return set()
    with open(RESULTS, newline="", encoding="utf-8") as f:
        return {r["key"] for r in csv.DictReader(f)}


def append_row(row: dict) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    new = not RESULTS.exists()
    with open(RESULTS, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase", required=True, choices=["baseline", "1", "2a", "2b", "2c"])
    ap.add_argument("--dataset", nargs="+", required=True, choices=list(DATASETS))
    ap.add_argument("--codec", default="zstd", help="Phase-2 carry-forward codec.")
    ap.add_argument("--level", type=int, default=6, help="Phase-2 carry-forward level.")
    ap.add_argument("--max-files", type=int, default=None,
                    help="Load only the first N staged files (memory governor).")
    ap.add_argument("--only", default=None, help="Substring filter on config keys.")
    ap.add_argument("--list", action="store_true", help="Print configs and exit.")
    args = ap.parse_args()

    if args.phase == "baseline":
        for ds_id in args.dataset:
            baseline(ds_id, args.max_files)
        return

    configs = phase_configs(args.phase, args.codec, args.level)
    # Every session re-measures the prod control (zstd-6 defaults) to detect drift.
    control = cfg("zstd", 6)
    if not any(c == control for c in configs):
        configs.insert(0, control)

    done = existing_keys()
    for ds_id in args.dataset:
        todo = [c for c in configs
                if config_key(ds_id, c, args.max_files) not in done
                and (args.only is None or args.only in config_key(ds_id, c, args.max_files))]
        # control is exempt from resume-skip (it's the drift check) — keep first dup
        if args.list:
            for c in todo:
                print(config_key(ds_id, c, args.max_files))
            continue
        if not todo:
            print(f"{ds_id}: nothing to do (all keys present).")
            continue
        base = load_dataset(ds_id, args.max_files)
        m1 = None
        if any(c["movetext"] == "M1" for c in todo):
            t0 = time.time()
            m1 = strip_movetext(base)
            print(f"  M1 strip: {(base.nbytes - m1.nbytes)/1e9:.2f} GB of comments "
                  f"removed ({time.time()-t0:.0f}s)", flush=True)
        for c in todo:
            key = config_key(ds_id, c, args.max_files)
            t0 = time.time()
            row = run_config(ds_id, base, m1, c, args.max_files)
            append_row(row)
            print(f"  [{key}] size={row['size_bytes']/1e6:,.0f} MB "
                  f"write={row['write_s']}s full-read={row['read_full_s']}s "
                  f"({time.time()-t0:.0f}s total)", flush=True)
        del base, m1
        gc.collect()


if __name__ == "__main__":
    main()
