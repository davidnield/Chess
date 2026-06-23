"""Inspect actual EPD format in repertoire parquet files."""
import polars as pl, sys
sys.stdout.reconfigure(encoding="utf-8")

df = pl.read_parquet("E:/chess/repertoire/repertoire_2025_apr_white_f030_e050.parquet")
blitz = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == 2200))
our = blitz.filter(pl.col("best_move").is_not_null())

# Show some early-ply positions (shortest EPDs first)
with_len = our.with_columns(
    pl.col("position_epd").str.len_bytes().alias("epd_len")
).sort("epd_len")

print("=== Shortest EPDs (earliest positions) in white parquet ===")
for r in with_len.head(25).iter_rows(named=True):
    print(f"  len={r['epd_len']:>3}  move={r['best_move']:<6}  val={r['value']:.4f}  epd={r['position_epd']}")

# Search specifically for the starting position
print("\n=== Searching for starting position ===")
for pattern in [
    "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR",
    "rnbqkbnr/pppppppp",
    "PPPPPPPP/RNBQKBNR w",
]:
    rows = blitz.filter(pl.col("position_epd").str.contains(pattern, literal=True))
    print(f"  Pattern '{pattern[:40]}...' => {rows.height} rows")
    for r in rows.head(3).iter_rows(named=True):
        print(f"    epd={r['position_epd']}  move={r['best_move']}  side={r['side_to_move']}")

# Also search for "e4" positions
print("\n=== Searching for after-e4 position ===")
for pattern in ["4P3/8/PPPP1PPP", "PPPP1PPP/RNBQKBNR"]:
    rows = blitz.filter(
        pl.col("position_epd").str.contains(pattern, literal=True)
        & pl.col("best_move").is_not_null()
    )
    print(f"  Pattern '{pattern}' (our-turn) => {rows.height} rows")
    for r in rows.head(5).iter_rows(named=True):
        print(f"    epd={r['position_epd'][:65]}  move={r['best_move']}")

# Check how position_hash is used
print("\n=== position_hash for common positions ===")
# Get unique EPDs with the fewest characters (likely opening positions)
unique_epds = blitz.select("position_epd", "position_hash", "side_to_move", "best_move", "value").unique(subset=["position_hash"])
with_len2 = unique_epds.with_columns(
    pl.col("position_epd").str.len_bytes().alias("epd_len")
).sort("epd_len")
print("  Top 10 unique shortest-EPD positions:")
for r in with_len2.head(10).iter_rows(named=True):
    bm = r["best_move"] or "(opp)"
    print(f"    hash={r['position_hash']}  side={r['side_to_move']}  move={bm:<6}  epd={r['position_epd']}")
