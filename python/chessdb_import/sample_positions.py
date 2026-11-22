"""Timing-test sample for the ChessDB import: N random book positions with >= 2 games.

Positions come from one book bucket (all 27 slices), grouped by the full key; parent_epd is the
book's own (legal ep only, as cdbdirect requires). Writes one EPD per line plus a parquet with keys.
Usage: sample_positions.py BUCKET N OUT_PREFIX
"""
import sys, time, duckdb
from pathlib import Path
sys.path.insert(0, "C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python")
from explorer_storage import relation
BOOK = Path("E:/chess/position-stats/brc-p51-train-201301-202607-20261115")
b, n, out = int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3])
files = sorted(BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
con = duckdb.connect()
con.execute("SET memory_limit='16GB'; SET threads=4; SET preserve_insertion_order=false; "
            "SET temp_directory='D:/chess_duckdb_tmp/cdb_sample'")
t = time.time()
# keys first (cheap), then the EPD for the sampled keys only: any_value(epd) over every group OOMs
con.execute(f"""CREATE TEMP TABLE pos AS
  SELECT parent_hash AS lo, parent_hash_hi AS hi, min(ply) AS ply, sum(total) AS games
  FROM read_parquet([{", ".join("'" + f.as_posix() + "'" for f in files)}], hive_partitioning=false)
  GROUP BY 1, 2 HAVING sum(total) >= 2""")
tot = con.execute("SELECT count(*) FROM pos").fetchone()[0]
con.execute(f"CREATE TEMP TABLE pick AS SELECT * FROM pos ORDER BY hash(lo, hi, 20261121) LIMIT {n}")
con.execute(f"""CREATE TEMP TABLE pick_epd AS
  SELECT p.*, e.epd FROM pick p JOIN (
    SELECT parent_hash AS lo, parent_hash_hi AS hi, any_value(parent_epd) AS epd
    FROM {relation(files)} WHERE (parent_hash, parent_hash_hi) IN (SELECT lo, hi FROM pick) GROUP BY 1, 2) e
  USING (lo, hi)""")
con.execute(f"COPY pick_epd TO '{out.with_suffix('.parquet').as_posix()}' (FORMAT parquet)")
con.execute(f"COPY (SELECT epd FROM pick_epd) TO '{out.with_suffix('.epd').as_posix()}' (FORMAT csv, HEADER false, QUOTE '')")
print(f"bucket {b}: {tot:,} positions with >= 2 games; wrote {min(n, tot):,} in {time.time()-t:.0f}s")
