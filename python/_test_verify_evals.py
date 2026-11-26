"""verify_evals.py against a real `explorer-extract evals` build of a synthetic book (exit 0 = pass).

Builds a small book (real positions from played games, two slices, a child-only position, a pinned
en-passant position keyed by its ep hash), cloud files in the dataset's own types (PV blocks, ties across
files) and fishnet months in all three tiers; runs the exe; requires verify_evals.py to pass; then
corrupts one output value, drops one output row, and requires it to fail each time.

Exe: $EXPLORER_EXTRACT_EXE, else D:/rust-target/explorer_extract/release/explorer-extract.exe; absent, or a
build without the `evals` subcommand -> skip (exit 0).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from zobrist import zobrist_int64  # noqa: E402
from zobrist128 import zobrist128  # noqa: E402
import explorer_exe  # noqa: E402

EXE = explorer_exe.resolve()
FAILS: list[str] = []


def check(ok: bool, label: str) -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {label}", flush=True)
    if not ok:
        FAILS.append(label)


def bkt(h: int) -> int:
    return ((h % 512) + 512) % 512


def games() -> list[list[chess.Board]]:
    lines = [
        "e2e4 e7e5 g1f3 b8c6 f1b5 a7a6 b5a4 g8f6 e1g1 f8e7",
        "d2d4 d7d5 c2c4 e7e6 b1c3 g8f6 c1g5 f8e7 e2e3 e8g8",
        "e2e4 c7c5 g1f3 d7d6 d2d4 c5d4 f3d4 g8f6 b1c3 a7a6",
        "f2f3 e7e5 g2g4 d8h4",
        "e2e4 a7a6 e4e5 a6a5 e1e2 a8a6 e2d3 a6h6 d3c4 h6h5 c4c5 f7f5",
    ]
    out = []
    for ln in lines:
        b = chess.Board()
        seq = [b.copy()]
        for u in ln.split():
            b.push_uci(u)
            seq.append(b.copy())
        out.append(seq)
    return out


def main() -> int:
    why = explorer_exe.unusable(EXE, {"evals": []})
    if why:
        print(f"SKIP: {why}")
        return 0
    tmp = Path(tempfile.mkdtemp(prefix="ee_verify_evals_"))
    try:
        return run(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_v2(tmp: Path, gs: list, cmd: list[str]) -> bool:
    """The same book with both halves of every key (a book-v2): the build writes an eval-db-v2 that
    verify_evals.py passes, and a child row whose high half is off by one fails it."""
    print("\nbook-v2 -> eval-db-v2", flush=True)
    hi_of = {zobrist_int64(b): zobrist128(b)[1] for seq in gs for b in seq}
    book2 = tmp / "book128"
    shutil.copytree(tmp / "book", book2)
    for f in book2.glob("ps/*/*/bkt*.parquet"):
        t = pq.ParquetFile(f).read()
        t = t.append_column("parent_hash_hi", pa.array([hi_of[h] for h in t["parent_hash"].to_pylist()], pa.int64()))
        t = t.append_column("child_hash_hi", pa.array([hi_of[h] for h in t["child_hash"].to_pylist()], pa.int64()))
        pq.write_table(t, f)
    (book2 / "_merge_params.json").write_text(json.dumps(
        {"schema": "book-v2", "input_params": {"max_ply": 50, "key_bits": 128}}), encoding="utf-8")
    out2 = tmp / "out128"
    c2 = list(cmd)
    for flag, val in (("--book", book2), ("--work", tmp / "work128"), ("--out", out2)):
        c2[c2.index(flag) + 1] = str(val)
    r = subprocess.run(c2, capture_output=True, text=True)
    ok = r.returncode == 0 and json.loads((out2 / "_build.meta.json").read_text())["schema"] == "eval-db-v2"
    check(ok, f"explorer-extract evals on a book-v2 writes an eval-db-v2 ({r.stderr[-400:] if r.returncode else ''})")
    if not ok:
        return False
    a = {r["epd"]: r for f in sorted((tmp / "out").glob("bkt*.parquet")) for r in pq.read_table(f).to_pylist()}
    b = {r["epd"]: r for f in sorted(out2.glob("bkt*.parquet")) for r in pq.read_table(f).to_pylist()}
    same = set(a) == set(b) and all({**b[e], "position_hash_hi": None} == {**a[e], "position_hash_hi": None}
                                    for e in a)
    his = all(r["position_hash_hi"] == hi_of.get(r["position_hash"], r["position_hash_hi"]) for r in b.values())
    last = pq.read_schema(sorted(out2.glob("bkt*.parquet"))[0]).names[-1]
    check(same and his and last == "position_hash_hi",
          f"the eval-db-v2 has the same {len(b)} rows with position_hash_hi last, every one zobrist128's")
    ver = [sys.executable, str(HERE / "verify_evals.py"), str(out2), "--buckets", "512", "--positive", "39",
           "--negative", "5", "--threads", "2", "--mem", "2GB", "--tmp-dir", str(tmp / "duck"), "--no-sha"]
    r = subprocess.run(ver, capture_output=True, text=True)
    check(r.returncode == 0 and "ALL PASS" in r.stdout and "zobrist128 gives" in r.stdout
          and "child_hash_hi) keys" in r.stdout and "additional distinct positives:" in r.stdout
          and "positive sample: 39 distinct rows" in r.stdout,
          "verify_evals.py passes the eval-db-v2, keys held on both halves")
    if r.returncode:
        print(r.stdout[-3000:], r.stderr[-2000:])
    for f in sorted(out2.glob("bkt*.parquet")):
        t = pq.read_table(f)
        kinds = t["in_book"].to_pylist()
        if "child" in kinds:
            k = kinds.index("child")
            hi = t["position_hash_hi"].to_pylist()
            hi[k] += 1
            t = t.set_column(t.schema.get_field_index("position_hash_hi"), "position_hash_hi", pa.array(hi, pa.int64()))
            pq.write_table(t, f)
            break
    r = subprocess.run(ver, capture_output=True, text=True)
    check(r.returncode == 1 and "FAIL" in r.stdout and "key (" in r.stdout,
          "a child row whose position_hash_hi is off by one fails the identity check")
    return True


def _checks(stdout: str) -> list[str]:
    """The PASS/FAIL lines, with timings masked."""
    return [re.sub(r"\d[\d,]*s\b", "Ns", ln) for ln in stdout.splitlines() if ln.startswith(("  PASS", "  FAIL"))]


def run_checkpoint(tmp: Path, ver: list[str], plain: str) -> None:
    """--checkpoint: a full run, a run resumed after losing its later units, and a fully reused run report
    exactly the checks of the uninterrupted run without it; damaged units stop the run; a changed DB file
    starts a fresh unit set (2026-11-20)."""
    print("\ncheckpointed verification", flush=True)
    ck, res = tmp / "ck", tmp / "result.json"
    want = _checks(plain)

    def go(extra=()):
        r = subprocess.run(ver + ["--checkpoint", str(ck), "--result-json", str(res), *extra],
                           capture_output=True, text=True)
        return r, (json.loads(res.read_text(encoding="utf-8")) if res.is_file() else None)

    r, out = go()
    if r.returncode or out is None:
        print(r.stdout[-3000:], r.stderr[-3000:])
    check(r.returncode == 0 and _checks(r.stdout) == want and out["all_pass"] and out["units_reused"] == 0
          and out["units_written"] > 0, f"a checkpointed run reports the plain run's {len(want)} checks "
          f"({out and out['units_written']} units written)")
    (unit_dir,) = [d for d in ck.iterdir() if d.is_dir()]
    later = sorted((unit_dir / "child").glob("*.json")) + sorted((unit_dir / "scan").rglob("*.json"))[1:]
    for f in later:
        f.unlink()
    r, out = go()
    check(r.returncode == 0 and _checks(r.stdout) == want and out["units_written"] == len(later)
          and out["units_reused"] > 0 and "sample reused from the checkpoint" in r.stdout,
          f"after losing {len(later)} later units the resumed run recomputes only those and matches")
    r, out = go()
    check(r.returncode == 0 and _checks(r.stdout) == want and out["units_written"] == 0,
          f"a complete unit set is reused whole ({out['units_reused']} units) and matches")
    victim = sorted((unit_dir / "scan").rglob("*.json"))[0]
    good = victim.read_bytes()
    victim.write_text("{", encoding="utf-8")
    r, _ = go()
    check(r.returncode != 0 and "unreadable checkpoint unit" in (r.stdout + r.stderr), "a corrupt unit stops the run")
    unit = json.loads(good)
    unit["depends"]["targets"] = "0" * 64
    victim.write_text(json.dumps(unit), encoding="utf-8")
    r, _ = go()
    check(r.returncode != 0 and "does not match its dependencies" in (r.stdout + r.stderr),
          "a unit recorded against other dependencies stops the run")
    victim.write_bytes(good)
    bucket_file = sorted((tmp / "out").glob("bkt*.parquet"))[0]
    st = bucket_file.stat()
    os.utime(bucket_file, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    r, out = go()
    check(r.returncode == 0 and _checks(r.stdout) == want and out["units_reused"] == 0
          and len([d for d in ck.iterdir() if d.is_dir()]) == 2,
          "a touched DB file gets a fresh unit set, never the stored sample")
    os.utime(bucket_file, ns=(st.st_atime_ns, st.st_mtime_ns))
    shutil.rmtree(ck)
    res.unlink(missing_ok=True)


def run(tmp: Path) -> int:
    gs = games()
    # Book: every parent of every game in Blitz/1600 (ply = index + 1), and the first 4 again in Rapid/1800.
    rows = {}
    for gi, seq in enumerate(gs):
        for i in range(len(seq) - 1):
            p, c = seq[i], seq[i + 1]
            san = p.san(c.move_stack[-1])
            key = (zobrist_int64(p), p.epd(), san, i + 1)
            rows[key] = zobrist_int64(c)
    slices = {("Blitz", 1600): rows, ("Rapid", 1800): dict(list(rows.items())[:4])}
    buckets = set()
    for (ev, band), rs in slices.items():
        by: dict[int, list] = {}
        for (h, e, san, ply), ch in sorted(rs.items()):
            by.setdefault(bkt(h), []).append((h, san, ev, band, e, ch, ply, 1, 0, 0, 1, 1.0))
            buckets.add(bkt(h))
            buckets.add(bkt(ch))
        for b, v in by.items():
            cols = list(zip(*v))
            t = pa.table({
                "parent_hash": pa.array(cols[0], pa.int64()), "move_san": pa.array(cols[1]),
                "event": pa.array(cols[2]), "elo_band": pa.array(cols[3], pa.int64()),
                "parent_epd": pa.array(cols[4]), "child_hash": pa.array(cols[5], pa.int64()),
                "ply": pa.array(cols[6], pa.int32()), "white_wins": pa.array(cols[7], pa.int64()),
                "draws": pa.array(cols[8], pa.int64()), "black_wins": pa.array(cols[9], pa.int64()),
                "total": pa.array(cols[10], pa.int64()), "white_score_avg": pa.array(cols[11], pa.float64())})
            d = tmp / "book" / "ps" / f"event={ev}" / f"elo_band={band}"
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(t, d / f"bkt{b:03d}.parquet")
    # Sources: cloud evals for some positions (a 2-PV block, a tie in the second file), fishnet rows
    # (every position of every game, three tiers), plus positions not in the book.
    allpos = [b for seq in gs for b in seq]
    cl = []
    for i, b in enumerate(allpos[::3]):
        fen = b.epd()
        cl += [(fen, "e2e4 e7e5", 20, 100, 10 + i, None), (fen, "d2d4 d7d5", 20, 100, 5 + i, None),
               (fen, "c2c4 e7e5", 18, 50, 7, None)]
    cl.append(("8/8/8/8/8/8/k7/4K3 w - -", "e1e2", 30, 10, 0, None))
    cl2 = [(allpos[0].epd(), "g1f3 d7d5", 20, 100, 99, None)]
    for name, rs in (("data_0000", cl), ("data_0001", cl2)):
        c = list(zip(*rs))
        t = pa.table({"fen": pa.array(c[0]), "line": pa.array(c[1]), "depth": pa.array(c[2], pa.uint8()),
                      "knodes": pa.array(c[3], pa.int32()), "cp": pa.array(c[4], pa.int16()),
                      "mate": pa.array(c[5], pa.int8())})
        (tmp / "evals" / "cloud").mkdir(parents=True, exist_ok=True)
        pq.write_table(t, tmp / "evals" / "cloud" / f"{name}.parquet")
    for (y, m), mult in (((2014, 3), 1), ((2018, 7), 2), ((2023, 1), 3)):
        fr = []
        for gi, seq in enumerate(gs):
            for i, b in enumerate(seq):
                if i < len(seq) - 1 and ((i + gi) % 4 == 0 or (i + gi + y) % 5 == 0):
                    continue
                for k in range(mult):
                    if b.is_checkmate():
                        fr.append((b.fen(), None, 0))
                    else:
                        fr.append((b.fen(), (i * 7 + k * 13 + y) % 300 - 150, None))
        fr.append(("4k3/8/8/8/8/8/8/4K3 b - - 0 1", 3, None))
        c = list(zip(*fr))
        t = pa.table({"fen": pa.array(c[0]), "cp": pa.array(c[1], pa.int32()), "mate": pa.array(c[2], pa.int32()),
                      "move": pa.array(["e2e4"] * len(fr))})
        (tmp / "evals" / "fishnet").mkdir(parents=True, exist_ok=True)
        pq.write_table(t, tmp / "evals" / "fishnet" / f"standard_rated_{y}_{m:02d}.parquet")
    bl = ",".join(str(b) for b in sorted(buckets))
    cmd = [str(EXE), "evals", "--test-inputs", "--book", str(tmp / "book"), "--cloud", str(tmp / "evals" / "cloud"),
           "--fishnet", str(tmp / "evals" / "fishnet"), "--work", str(tmp / "work"), "--out", str(tmp / "out"),
           "--threads", "2", "--mem-gb", "2", "--buckets", bl, "--child-sources", bl]
    r = subprocess.run(cmd, capture_output=True, text=True)
    check(r.returncode == 0, f"explorer-extract evals exits 0 ({r.stderr[-400:] if r.returncode else ''})")
    if r.returncode:
        return 1
    meta = json.loads((tmp / "out" / "_build.meta.json").read_text(encoding="utf-8"))
    t = meta["totals"]
    check(t["rows"] > 20 and t["children"] >= 1 and t["ep_variant_rows"] >= 1 and t["cloud"] > 0 and t["fishnet"] > 0,
          f"the synthetic DB has parents, children, an ep variant, cloud and fishnet rows ({t['rows']} rows, "
          f"{t['children']} child, {t['ep_variant_rows']} variant, {t['cloud']} cloud)")
    ver = [sys.executable, str(HERE / "verify_evals.py"), str(tmp / "out"), "--buckets", "512", "--positive", "100000",
           "--negative", "5", "--threads", "2", "--mem", "2GB", "--tmp-dir", str(tmp / "duck")]
    r = subprocess.run(ver, capture_output=True, text=True)
    check(r.returncode == 0 and "ALL PASS" in r.stdout, "verify_evals.py passes the real build")
    if r.returncode:
        print(r.stdout[-3000:], r.stderr[-2000:])
    run_checkpoint(tmp, ver, r.stdout)
    # The book-v2 variant, while the book and the 64-bit output are still intact.
    v2 = run_v2(tmp, gs, cmd)
    print()
    print("corruptions of the 64-bit build", flush=True)
    # Corrupt one value (a cloud row's cloud_line) -> fail; drop a parent row -> the negative check fails.
    outs = sorted((tmp / "out").glob("bkt*.parquet"))
    def cloud_parent(p: Path) -> int | None:
        t = pq.read_table(p)
        return next((k for k, (s, ib) in enumerate(zip(t.column("source").to_pylist(), t.column("in_book").to_pylist()))
                     if s == "cloud" and ib == "parent"), None)

    victim = next(p for p in outs if cloud_parent(p) is not None)
    orig = victim.read_bytes()
    tb = pq.read_table(victim)
    lines = tb.column("cloud_line").to_pylist()
    i = cloud_parent(victim)
    lines[i] = "a2a3 " + lines[i]
    pq.write_table(tb.set_column(tb.schema.get_field_index("cloud_line"), "cloud_line", pa.array(lines)), victim)
    r = subprocess.run(ver + ["--no-sha"], capture_output=True, text=True)
    check(r.returncode == 1 and "cloud_line" in r.stdout, "a corrupted cloud_line fails the recompute")
    victim.write_bytes(orig)
    # The child check alone (fresh processes per group of book buckets) passes the intact build...
    r = subprocess.run(ver + ["--no-sha", "--child-check-only", "1000"], capture_output=True, text=True)
    check(r.returncode == 0 and "PASS" in r.stdout and "child-only hashes are book child_hash values" in r.stdout,
          "--child-check-only passes the intact build")
    # ...and fails once the book no longer produces one child-only hash: drop every book row whose child_hash
    # is that hash (restored afterwards).
    child_h = next(h for p in outs for h, ib in zip(pq.read_table(p).column("position_hash").to_pylist(),
                                                     pq.read_table(p).column("in_book").to_pylist()) if ib == "child")
    saved = {}
    for bf in (tmp / "book" / "ps").rglob("bkt*.parquet"):
        bt = pq.ParquetFile(bf).read()  # not read_table: hive dirs clash with the in-file event column
        ch = bt.column("child_hash").to_pylist()
        if child_h in ch:
            saved[bf] = bf.read_bytes()
            pq.write_table(bt.take(pa.array([k for k, c in enumerate(ch) if c != child_h], pa.int64())), bf)
    r = subprocess.run(ver + ["--no-sha", "--positive", "100000"], capture_output=True, text=True)
    check(bool(saved) and r.returncode == 1 and "child_hash values (1 are not)" in r.stdout,
          f"a child-only hash the book no longer produces fails the child check ({len(saved)} book file(s) edited)")
    for bf, data in saved.items():
        bf.write_bytes(data)
    tb = pq.read_table(victim)
    keep = [k for k in range(tb.num_rows) if k != i]
    pq.write_table(tb.take(pa.array(keep, pa.int64())), victim)
    man = pq.read_table(tmp / "out" / "_manifest.parquet").to_pylist()
    for m in man:
        if f"bkt{m['bucket']:03d}.parquet" == victim.name:
            m["rows"] -= 1
            m["bytes"] = victim.stat().st_size
    pq.write_table(pa.Table.from_pylist(man), tmp / "out" / "_manifest.parquet")
    r = subprocess.run(ver + ["--no-sha", "--negative", "100000"], capture_output=True, text=True)
    check(v2 and r.returncode == 1 and "no raw source row carries their EPD" in r.stdout and "FAIL  " in r.stdout,
          "a dropped row is caught by the negative sample")
    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURES'}")
    return 0 if not FAILS else 1


if __name__ == "__main__":
    raise SystemExit(main())
