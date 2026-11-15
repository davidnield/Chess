"""Compare a ply-51 book with directly capped extractions on identical sampled games."""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import chess
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from month_archive import sha, write_json
from verify_book import COUNTS, _lit
from zobrist128 import zobrist128

EVENTS = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"]
SPEEDS = ["Blitz", "Classical", "Rapid"]
PILOTS = [(2026, 3), (2024, 6), (2020, 4)]


def build_sample(source, out):
    samples = []
    columns = ["movetext", "mean_elo", "white_score", "termination", "white_title", "black_title"]
    for speed in SPEEDS:
        target = out / "year=2026/month=3" / f"event={speed}"
        target.mkdir(parents=True)
        for year, month in PILOTS:
            choices = sorted(p for p in (source / f"year={year}").glob(f"month=*/event={speed}/*.parquet")
                             if int(p.parent.parent.name[6:]) == month)
            original = choices[len(choices) // 2]
            parquet = pq.ParquetFile(original)
            group = parquet.num_row_groups // 2
            batch = next(parquet.iter_batches(batch_size=128, row_groups=[group], columns=columns))
            data = pa.Table.from_batches([batch])
            name = target / f"sample-{year}-{month}.parquet"
            pq.write_table(data, name)
            samples.append(dict(file=str(original), group=group, rows=data.num_rows, sample_sha256=sha(name)))
    rows = []
    transpositions = []
    for first in ("Nf3 Nf6 g3 g6", "g3 g6 Nf3 Nf6"):
        board = chess.Board()
        opening = first.split()
        for move in opening:
            board.push_san(move)
        transpositions.append(zobrist128(board))
        sequence = opening + "Ng1 Ng8 Nf3 Nf6".split() * 12
        for ply, score, reason in [(30, 1., "Time forfeit"), (31, .5, "Normal"),
                                   (50, 0., "Time forfeit"), (51, .5, "Normal"), (52, 1., "Normal")]:
            rows.append(dict(movetext=" ".join(sequence[:ply]), mean_elo=1800, white_score=score,
                             termination=reason, white_title=None, black_title=None))
    if len(set(transpositions)) != 1:
        raise ValueError("invalid transposition control")
    name = out / "year=2026/month=3/event=Blitz/control.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=data.schema), name)
    return samples, len(rows)


def require_equal(con, label, actual, expected):
    for lhs, rhs in ((actual, expected), (expected, actual)):
        mismatch = con.execute(f"SELECT * FROM (({lhs}) EXCEPT ALL ({rhs})) LIMIT 1").fetchone()
        if mismatch is not None:
            raise ValueError(f"{label}: {mismatch}")


def relation(files):
    return f"read_parquet({_lit(files)}, hive_partitioning=false)"


def compare(con, book, direct, cap):
    ps = relation(sorted((book / "ps").glob("event=*/elo_band=*/*.parquet")))
    term = relation(sorted((book / "term").glob("bkt*.parquet")))
    ps_ref = relation(sorted((direct / "month=2026_3").glob("bkt=*/*.parquet")))
    term_ref = relation([direct / "_term/year=2026_month=3.term.parquet"])
    cols = ", ".join(COUNTS)
    sums = ", ".join(f"SUM({c}) AS {c}" for c in COUNTS)
    edge = f"parent_hash,parent_hash_hi,parent_epd,move_san,event,elo_band,ply,child_hash,child_hash_hi,{cols}"
    require_equal(con, f"ply {cap} edge rows", f"SELECT {edge} FROM {ps} WHERE ply<={cap}",
                  f"SELECT {edge} FROM {ps_ref}")
    position = "position_hash,position_hash_hi,event_id,elo_band"
    end_key = position + ",end_ply,reason"
    require_equal(con, f"ply {cap} real endings",
                  f"SELECT {end_key},{cols} FROM {term} WHERE kind=0 AND end_ply<={cap}",
                  f"SELECT {end_key},{cols} FROM {term_ref} WHERE kind=0")
    ids = json.loads((book / "_book.meta.json").read_text(encoding="utf-8"))["term_event_ids"]
    if ids != EVENTS:
        raise ValueError(f"unexpected event mapping {ids}")
    id_expr = "CASE event " + " ".join(f"WHEN '{name}' THEN {i}" for i, name in enumerate(ids)) + " END"
    deltas = ", ".join(f"a.{c} - coalesce(e.{c}, 0) AS {c}" for c in COUNTS)
    horizon = f"""
        WITH arrivals AS (SELECT child_hash AS position_hash, child_hash_hi AS position_hash_hi,
                    {id_expr} AS event_id, elo_band, {sums} FROM {ps} WHERE ply={cap} GROUP BY ALL),
             ended AS (SELECT {position}, {sums} FROM {term} WHERE kind=0 AND end_ply={cap} GROUP BY ALL)
        SELECT {position},{deltas} FROM arrivals a LEFT JOIN ended e USING ({position})
        WHERE a.total > coalesce(e.total,0)"""
    require_equal(con, f"ply {cap} horizon mass", horizon,
                  f"SELECT {position},{sums} FROM {term_ref} WHERE kind=1 GROUP BY ALL")
    counts = con.execute(f"SELECT COUNT(*), SUM(total) FROM {ps_ref}").fetchone()
    return {"ply": cap, "distinct_edges": counts[0], "plies": int(counts[1]),
            "terminal_games": con.execute(f"SELECT SUM(total) FROM {term_ref} WHERE kind=0").fetchone()[0],
            "horizon_games": con.execute(f"SELECT SUM(total) FROM {term_ref} WHERE kind=1").fetchone()[0]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("exe", "source", "scratch", "receipt"):
        parser.add_argument("--" + flag, type=Path, required=True)
    args = parser.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.scratch) as tmp:
        work = Path(tmp)
        sample = work / "games"
        samples, controls = build_sample(args.source, sample)
        for cap in (30, 50, 51):
            subprocess.run([args.exe, "month", "--source", sample, "--months", "2026_3", "--events", *SPEEDS,
                            "--out", work / str(cap), "--max-ply", str(cap), "--key128", "--ply-key",
                            "--term-slices", "--threads", "1", "--mem-gb", "1", "--passes", "1"], check=True)
        book = work / "book"
        subprocess.run([args.exe, "merge", "--months-root", work / "51", "--months", "2026_3",
                        "--out", book, "--stage-dir", work / "stage", "--stage-gb", "0.2",
                        "--min-free-gb", "0", "--test-one-volume", "--threads", "1"], check=True)
        with duckdb.connect(config={"threads": 1}) as con:
            results = [compare(con, book, work / str(cap), cap) for cap in (30, 50)]
        write_json(args.receipt, {"exe_sha256": sha(args.exe), "check_sha256": sha(Path(__file__)),
                                  "source_samples": samples, "transposition_controls": controls,
                                  "results": results})
        print(results, flush=True)


if __name__ == "__main__":
    main()
