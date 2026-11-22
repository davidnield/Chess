"""One-bucket census of the ply-51 book: how many positions each ChessDB-import scope keeps.

Bucket 000 of every slice (1/512 of positions by hash, so counts x512 estimate the book).
Groups by the full key, sums games over slices and plies, takes the position's first ply.
"""
import sys, time, duckdb
from pathlib import Path
BOOK = Path("E:/chess/position-stats/brc-p51-train-201301-202607-20261115")
b = int(sys.argv[1]) if len(sys.argv) > 1 else 0
files = sorted(p.as_posix() for p in BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
con = duckdb.connect()
con.execute("SET memory_limit='16GB'; SET threads=4; SET preserve_insertion_order=false; "
            "SET temp_directory='D:/chess_duckdb_tmp/scope_census'")
t = time.time()
lit = ", ".join(f"'{f}'" for f in files)
con.execute(f"""CREATE TEMP TABLE pos AS SELECT parent_hash, parent_hash_hi, min(ply) AS ply, sum(total) AS games
                FROM read_parquet([{lit}], hive_partitioning=false) GROUP BY 1, 2""")
rows = con.execute("""
  SELECT CASE WHEN ply <= 30 THEN 'ply<=30' ELSE 'ply31-51' END AS plies,
         count(*) n, sum(games) g,
         count(*) FILTER (games >= 2) n2, sum(games) FILTER (games >= 2) g2,
         count(*) FILTER (games >= 5) n5, sum(games) FILTER (games >= 5) g5,
         count(*) FILTER (games >= 20) n20, sum(games) FILTER (games >= 20) g20,
         count(*) FILTER (games >= 100) n100, sum(games) FILTER (games >= 100) g100
  FROM pos GROUP BY 1 ORDER BY 1""").fetchall()
cols = ["plies", "n", "g", "n2", "g2", "n5", "g5", "n20", "g20", "n100", "g100"]
print(f"bucket {b}: {len(files)} files, {time.time()-t:.0f}s")
for r in rows:
    d = dict(zip(cols, r))
    print(d["plies"], {k: f"{d[k]*512/1e9:.2f}B" for k in cols[1:]})
