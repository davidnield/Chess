"""verify_book.py and compare_explorer_outputs.py book hold a merged book to its months (skips without the exe).

A two-month synthetic book is built the way the real one is: legal games from
python-chess, `explorer-extract month --ply-key` (the real producer) for each
month, then `explorer-extract merge`. Required:
  * the merge finishes the book (_BOOK.DONE), and both independent checkers
    pass it: verify_book.py (digests over every bucket, duplicate keys, the
    book-wide sums against the manifests, the scan, _collisions, a sample
    replayed by python-chess) and compare_explorer_outputs.py book (a DuckDB
    GROUP BY of the months, every column, and term);
  * with one book row's sums corrupted (white_wins and total +1, the average
    recomputed so the row still scans clean), both checkers fail.

Run: .venv/Scripts/python.exe python/_test_verify_book.py
Exe: $EXPLORER_EXTRACT_EXE, else D:/rust-target/explorer_extract/release/explorer-extract.exe
"""
from __future__ import annotations

import os
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
EXE = Path(os.environ.get("EXPLORER_EXTRACT_EXE",
                          "D:/rust-target/explorer_extract/release/explorer-extract.exe"))
YEAR = 2099
MONTHS = (1, 2)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    return bool(ok)


def pgn(sans: list[str], result: str) -> str:
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i // 2 + 1}.")
        out.append(s)
    return " ".join(out + [result])


def games(n: int, seed: int) -> list[tuple]:
    """Legal games of 1-40 plies from a few shared openings (so keys repeat
    across games, slices and months), some with a bad token mid-game, plus a
    few with no moves at all."""
    rng = random.Random(seed)
    openings = [[], "e4 e5 Nf3 Nc6".split(), "d4 d5 c4 e6".split(), "e4 c5 Nf3 d6".split(),
                "Nf3 Nf6 Ng1 Ng8".split()]
    out = []
    for _ in range(n):
        b, sans = chess.Board(), []
        for san in rng.choice(openings):
            mv = b.parse_san(san)
            sans.append(b.san(mv))
            b.push(mv)
        target = rng.randint(max(1, len(sans)), 40)
        while len(sans) < target and not b.is_game_over():
            mv = rng.choice(list(b.legal_moves))
            sans.append(b.san(mv))
            b.push(mv)
        if not sans:
            continue
        if len(sans) > 3 and rng.random() < 0.05:
            k = rng.randrange(1, len(sans))
            sans = sans[:k] + ["Qxz9"] + sans[k + 1:]
        ws = rng.choice([1.0, 0.5, 0.0])
        out.append((pgn(sans, {1.0: "1-0", 0.5: "1/2-1/2", 0.0: "0-1"}[ws]), ws,
                    rng.choice(["Normal", "Time forfeit", "Normal"]), rng.randint(800, 2700), None, None))
    # Kept games with no moves: month mode writes term(START_HASH, kind 0, end_ply 0) for them.
    for k in range(max(1, n // 200)):
        out.insert(rng.randrange(len(out) + 1), (["1-0", "0-1", ""][k % 3], [1.0, 0.0, 0.5][k % 3],
                                                 "Time forfeit", rng.randint(800, 2700), None, None))
    return out


def write_source(path: Path, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows))
    n = len(rows)
    pq.write_table(pa.table({
        "movetext": pa.array(cols[0], pa.string()),
        "white_score": pa.array(cols[1], pa.float64()),
        "termination": pa.array(cols[2], pa.string()),
        "move_count": pa.array([None] * n, pa.int16()),
        "mean_elo": pa.array(cols[3], pa.int16()),
        "white_title": pa.array(cols[4], pa.string()),
        "black_title": pa.array(cols[5], pa.string()),
        "white_elo": pa.array([None] * n, pa.int16()),
        "black_elo": pa.array([None] * n, pa.int16()),
    }), path, compression="zstd")


def run(*args, timeout=1800) -> subprocess.CompletedProcess:
    return subprocess.run([str(a) for a in args], capture_output=True, text=True, timeout=timeout)


def tail(p: subprocess.CompletedProcess, n: int = 1500) -> str:
    return (p.stdout + p.stderr)[-n:]


def corrupt_one_row(book: Path) -> tuple[Path, int]:
    """white_wins and total +1 on the first row of the biggest ps file, the
    average recomputed: a row that still scans clean but carries wrong sums."""
    files = sorted((book / "ps").glob("event=*/elo_band=*/*.parquet"), key=lambda f: -f.stat().st_size)
    f = files[0]
    # Not pq.read_table(f): pyarrow's default hive partitioning clashes with
    # the in-file event/elo_band columns (the book's README says so).
    t = pq.ParquetFile(f).read()
    d = {c: t[c].to_pylist() for c in t.column_names}
    d["white_wins"][0] += 1
    d["total"][0] += 1
    d["white_score_avg"][0] = (d["white_wins"][0] + 0.5 * d["draws"][0]) / d["total"][0]
    pq.write_table(pa.Table.from_pydict(d, schema=t.schema), f, compression="zstd")
    return f, int(f.stem[3:])


def main() -> None:
    if not EXE.exists():
        print(f"  SKIP  no explorer-extract at {EXE} (set EXPLORER_EXTRACT_EXE)")
        print("\nALL PASS (0 checks -- exe unavailable)")
        sys.exit(0)
    tmp = Path(tempfile.mkdtemp(prefix="test_verify_book_"))
    try:
        src, months, book = tmp / "src", tmp / "months", tmp / "book"
        for k, m in enumerate(MONTHS):
            g = games(1200, 7 + k)
            ev = src / f"year={YEAR}" / f"month={m}"
            write_source(ev / "event=Blitz" / "part-0.parquet", g[:800])
            write_source(ev / "event=Rapid" / "part-0.parquet", g[800:])
        tags = [f"{YEAR}_{m}" for m in MONTHS]
        p = run(EXE, "month", "--source", src, "--months", *tags, "--out", months, "--ply-key",
                "--max-ply", "30", "--mem-gb", "2", "--threads", "2")
        check(p.returncode == 0, f"month --ply-key builds both months (rc {p.returncode}) "
                                 f"{tail(p) if p.returncode else ''}")
        p = run(EXE, "merge", "--months-root", months, "--months", f"{tags[0]}..{tags[-1]}",
                "--out", book, "--stage-dir", tmp / "stage", "--threads", "3", "--min-free-gb", "1",
                "--test-one-volume")
        check(p.returncode == 0 and (book / "_BOOK.DONE").exists(),
              f"merge finishes the book (rc {p.returncode}) {tail(p) if p.returncode else ''}")
        rows = sum(pq.read_metadata(f).num_rows for f in (book / "ps").glob("event=*/elo_band=*/*.parquet"))
        check(rows > 5000, f"the book has {rows:,} rows")
        start = pq.ParquetFile(book / "term" / "bkt156.parquet").read().to_pylist()
        zero = [r for r in start if r["position_hash"] == 5060803636482931868 and r["end_ply"] == 0]
        check(len(zero) > 0 and all(r["kind"] == 0 for r in zero),
              f"the no-move games' term rows (start position, kind 0, end_ply 0) are in the book: "
              f"{sum(r['total'] for r in zero)} games")
        duck = ["--tmp-dir", tmp / "duck"]
        v = run(sys.executable, HERE / "verify_book.py", book, months, "--digest-buckets", "0-511",
                "--dup-buckets", "0-511", "--sums", "--scan", "--collisions", "--sample", "3000",
                "--workers", "2", *duck)
        check(v.returncode == 0 and "ALL PASS" in v.stdout,
              f"verify_book.py passes the book (rc {v.returncode})")
        if v.returncode:
            print(tail(v, 3000))
        c = run(sys.executable, HERE / "compare_explorer_outputs.py", "book", months, book,
                "--months", *tags, *duck)
        check(c.returncode == 0 and "IDENTICAL" in c.stdout,
              f"compare_explorer_outputs.py book: identical to the months' GROUP BY, ps and term "
              f"(rc {c.returncode})")
        if c.returncode:
            print(tail(c, 3000))

        f, b = corrupt_one_row(book)
        print(f"\n  corrupted the first row of {f.relative_to(book)}")
        v = run(sys.executable, HERE / "verify_book.py", book, months, "--digest-buckets", str(b),
                "--scan", *duck)
        check(v.returncode == 1 and "FAIL  the 8 hash-weighted sums" in v.stdout
              and "PASS  every book row" in v.stdout,
              "verify_book.py fails the corrupted bucket's digest (and the row still scans clean)")
        s = run(sys.executable, HERE / "verify_book.py", book, months, "--sums", *duck)
        check(s.returncode == 1, "verify_book.py --sums fails against the manifests")
        c = run(sys.executable, HERE / "compare_explorer_outputs.py", "book", months, book,
                "--months", *tags, *duck)
        check(c.returncode == 1 and "MISMATCHES" in c.stdout,
              "compare_explorer_outputs.py book fails the corrupted book")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
