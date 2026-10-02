"""The mmap'd eval lookup returns what the parquet says, and is fast enough.

Everything in the fused extract keys off this primitive: the winpos crossing
detection and the other-moves bucket's aggregate eval both come from it. A wrong
answer here is not a crash, it is a repertoire built on the wrong evaluations —
so it is checked against the source of truth rather than against itself.

Also measures throughput, because the design claim ("sort the batch first so the
binary searches sweep instead of jumping") is the reason per-ply lookups are
affordable at all, and an unmeasured performance claim is how the last plan went
wrong.

Usage:  python _test_eval_arrays.py [--sample N]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from eval_arrays import (DEFAULT_ARRAY_DIR, DEFAULT_EVAL_DB, META_NAME, MISSING,
                         build_eval_arrays, lookup_evals, open_eval_arrays,
                         verify_eval_arrays)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def raises(fn, exc) -> bool:
    try:
        fn()
    except exc:
        return True
    except Exception:                                          # noqa: BLE001
        return False
    return False


def check_staleness() -> None:
    """The arrays are a DERIVED copy — rebuild the eval DB and they go stale.

    Nothing about that is visible at read time: a stale array is the right shape,
    sorted, and answers every query. It just answers with the previous DB's
    evaluations, in both the winpos crossings and the other-moves bucket. So the
    guard is tested here on throwaway arrays rather than trusted.
    """
    tmp = Path(tempfile.mkdtemp(prefix="eval_arrays_stale_"))
    try:
        db = tmp / "src.parquet"
        adir = tmp / "arrays"
        pl.DataFrame({"position_hash": [5, 1, 9, 3],
                      "eval_cp": [10, -20, 30, -40]}).write_parquet(db)
        build_eval_arrays(db, adir)
        check((adir / META_NAME).exists(),
              "build writes a fingerprint sidecar")
        check("verified against" in verify_eval_arrays(adir, db),
              "freshly built arrays verify against their source")

        # Rebuild the source with different content. This is exactly the
        # build_fishnet_eval_db.py path that has already happened once.
        pl.DataFrame({"position_hash": [5, 1, 9, 3, 7],
                      "eval_cp": [10, -20, 30, -40, 50]}).write_parquet(db)
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError),
              "a CHANGED source is detected as stale (this is the bug being fixed)")

        # build_eval_arrays is the one caller that can fix staleness, so it
        # rebuilds instead of raising.
        build_eval_arrays(db, adir)
        check("verified against" in verify_eval_arrays(adir, db),
              "build_eval_arrays rebuilds a stale pair rather than raising")
        check(int(np.load(adir / "eval_hash.npy", mmap_mode="r").shape[0]) == 5,
              "the rebuilt arrays carry the new row count")

        # Legacy arrays (built before the sidecar existed) must not force a
        # needless 400M-row rebuild when they are demonstrably current.
        (adir / META_NAME).unlink()
        check("adopted legacy arrays" in verify_eval_arrays(adir, db),
              "meta-less arrays matching row count + mtime are adopted")
        check((adir / META_NAME).exists(),
              "adoption records the fingerprint so later checks are exact")

        # ...but a meta-less pair whose row count disagrees is a hard failure,
        # which is the case adoption must never wave through.
        (adir / META_NAME).unlink()
        pl.DataFrame({"position_hash": [5, 1], "eval_cp": [10, -20]}).write_parquet(db)
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError),
              "meta-less arrays with a MISMATCHED row count are rejected")

        check(raises(lambda: verify_eval_arrays(tmp / "nope", db), FileNotFoundError),
              "absent arrays raise FileNotFoundError with a build hint")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _hb(b: int, k: int) -> int:
    """A hash in bucket b (((h % 512) + 512) % 512 == b)."""
    return k * 512 + b


def _write_db_dir(db: Path, book: Path, rows: list[tuple[int, int, bool]]) -> None:
    """A synthetic eval DB directory: 512 bkt files sorted by hash, manifest, build meta, _DONE."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from eval_arrays import _sha256
    db.mkdir(parents=True, exist_ok=True)
    by: dict[int, list] = {}
    for h, cp, amb in rows:
        by.setdefault(((h % 512) + 512) % 512, []).append((h, cp, amb))
    counts = []
    for b in range(512):
        v = sorted(by.get(b, []), key=lambda r: r[0])
        pq.write_table(pa.table({"position_hash": pa.array([r[0] for r in v], pa.int64()),
                                 "eval_cp": pa.array([r[1] for r in v], pa.int16()),
                                 "hash_ambiguous": pa.array([r[2] for r in v], pa.bool_())}),
                       db / f"bkt{b:03d}.parquet")
        counts.append(len(v))
    pq.write_table(pa.table({"bucket": pa.array(range(512), pa.int64()), "rows": pa.array(counts, pa.int64())}),
                   db / "_manifest.parquet")
    (db / "_build.meta.json").write_text(json.dumps(
        {"inputs": {"book": str(book), "book_meta_sha256": _sha256(book / "_book.meta.json")}}), encoding="utf-8")
    (db / "_DONE").write_text("2026-10-01T00:00:00Z\n", encoding="utf-8")


def _write_book(book: Path, rows: list[tuple], collisions: list[tuple[int, str]]) -> None:
    """A synthetic book: rows (parent_hash, move_san, parent_epd, child_hash, ply, total) in one slice."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    by: dict[int, list] = {}
    for r in rows:
        by.setdefault(((r[0] % 512) + 512) % 512, []).append(r)
    d = book / "ps" / "event=Blitz" / "elo_band=1600"
    d.mkdir(parents=True, exist_ok=True)
    for b, v in by.items():
        c = list(zip(*v))
        pq.write_table(pa.table({"parent_hash": pa.array(c[0], pa.int64()), "move_san": pa.array(c[1]),
                                 "parent_epd": pa.array(c[2]), "child_hash": pa.array(c[3], pa.int64()),
                                 "ply": pa.array(c[4], pa.int32()), "total": pa.array(c[5], pa.int64())}),
                       d / f"bkt{b:03d}.parquet")
    pq.write_table(pa.table({"parent_hash": pa.array([x[0] for x in collisions], pa.int64()),
                             "parent_epd": pa.array([x[1] for x in collisions])}), book / "_collisions.parquet")
    (book / "_book.meta.json").write_text(json.dumps({"synthetic": True}), encoding="utf-8")


def check_dir_source() -> None:
    """Arrays built from an eval DB DIRECTORY: exclusions, checkmates, order, fingerprint."""
    import os

    import pyarrow.parquet as pq
    from eval_arrays import DIR_KIND, read_meta
    from eval_arrays_build import check_terminal_sample
    tmp = Path(tempfile.mkdtemp(prefix="eval_arrays_dir_"))
    try:
        book, db, adir = tmp / "book", tmp / "db", tmp / "arrays"
        A, B, C, D, E = _hb(3, 1), _hb(3, 2), _hb(3, 3), _hb(5, 7), _hb(5, 8)
        F, G, H = -512 * 9 + 7, 2**63 - 512 + 4, -2**63 + 2
        M1, M2, M4 = _hb(9, 11), _hb(9, 12), _hb(11, 13)
        _write_book(book, [
            (_hb(1, 1), "Qh4#", "x", M1, 5, 3),        # odd ply: White mated Black -> +2000
            (_hb(1, 2), "Qxf7#", "x", M2, 6, 1),       # even ply: Black mated White -> -2000
            (_hb(1, 3), "Rh8#", "x", E, 7, 2),         # a mate the DB already holds: the DB wins
            (_hb(1, 4), "Qg7#", "x", M4, 3, 1),        # the same child at both parities: conflict
            (_hb(1, 5), "Qg8#", "x", M4, 4, 1),
            (_hb(1, 6), "Nf7#", "x", D, 9, 1),         # a mate on a collision hash: not added
            (_hb(1, 7), "e4", "x", _hb(13, 1), 1, 9),  # no '#': nothing
        ], collisions=[(D, "p1"), (D, "p2")])
        _write_db_dir(db, book, [(A, 10, False), (B, 20, False), (B, -20, False), (C, 30, True),
                                 (D, -40, False), (E, 2000, False), (F, 55, False), (G, 1, False),
                                 (H, 2, False)])
        build_eval_arrays(db, adir, book=book, terminal=True, work=tmp / "work", threads=1, mem="1GB",
                          tmp=tmp / "duck", terminal_sample=0)
        meta = read_meta(adir) or {}
        check(meta.get("kind") == DIR_KIND and (adir / "excluded.parquet").exists()
              and (adir / "terminal_mates.parquet").exists(), "a directory build writes the meta and sidecars")
        h, e = open_eval_arrays(adir)
        check(bool(np.all(np.asarray(h)[1:] > np.asarray(h)[:-1])), "hashes strictly increasing, int64 extremes included")
        keys = np.array([A, B, C, D, E, F, G, H, M1, M2, M4, _hb(13, 1)], dtype=np.int64)
        want = [10, MISSING, MISSING, MISSING, 2000, 55, 1, 2, 2000, -2000, MISSING, MISSING]
        got = lookup_evals(keys, h, e).tolist()
        check(got == [int(x) for x in want], f"lookups: kept rows, exclusions, mates by parity ({got})")
        ex = pq.read_table(adir / "excluded.parquet").to_pylist()
        reasons = {(r["position_hash"], r["reason"]) for r in ex}
        check(reasons == {(B, "db_duplicate"), (C, "hash_ambiguous"), (D, "book_collision"), (M4, "mate_conflict")},
              f"excluded with their reasons ({sorted(reasons)})")
        cnt = meta.get("counts", {})
        check(cnt.get("mates_added") == 2 and cnt.get("mates_in_db") == 1 and cnt.get("mates_in_db_agree") == 1
              and cnt.get("mates_collision") == 1 and cnt.get("mate_conflicts") == 1,
              f"mate counts: 2 added, 1 already in the DB and agreeing, 1 on an excluded hash, 1 conflict ({cnt})")
        check("verified against" in verify_eval_arrays(adir, db), "a directory build verifies against its source")
        del h, e
        bf = db / "bkt003.parquet"
        st = bf.stat()
        os.utime(bf, ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError), "a touched bucket file makes them stale")
        build_eval_arrays(db, adir, book=book, terminal=True, work=tmp / "work", threads=1, mem="1GB",
                          tmp=tmp / "duck", terminal_sample=0)
        check("verified against" in verify_eval_arrays(adir, db), "build_eval_arrays rebuilds a stale directory build")
        (db / "_DONE").write_text("2026-10-02T00:00:00Z\n", encoding="utf-8")
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError), "a new _DONE makes them stale")
        (db / "_DONE").write_text("2026-10-01T00:00:00Z\n", encoding="utf-8")
        _write_book(book, [], collisions=[(D, "p1"), (D, "p2"), (A, "q1")])
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError), "a changed book collision list makes them stale")
        (db / "_DONE").unlink()
        check(raises(lambda: verify_eval_arrays(adir, db), FileNotFoundError), "a DB without _DONE is refused")

        # The parity rule holds on real games (python-chess replay), and a wrong parity is caught.
        import chess
        from zobrist import zobrist_int64

        def mate_row(moves: str, ply: int) -> tuple:
            b = chess.Board()
            for u in moves.split()[:-1]:
                b.push_uci(u)
            epd, parent = b.epd(), zobrist_int64(b)
            mv = chess.Move.from_uci(moves.split()[-1])
            san = b.san(mv)
            b.push(mv)
            return (parent, san, epd, zobrist_int64(b), ply, 1)

        real = tmp / "book_real"
        fool = mate_row("f2f3 e7e5 g2g4 d8h4", 4)
        scholar = mate_row("e2e4 e7e5 f1c4 b8c6 d1h5 g8f6 h5f7", 7)
        _write_book(real, [fool, scholar], collisions=[])
        n, bad = check_terminal_sample(real, 10, 1, threads=1, mem="1GB", tmp=tmp / "duck", buckets=8)
        check(n == 2 and not bad, f"real mates replay to mate, child_hash and parity ({n} rows, {bad})")
        wrong = tmp / "book_wrong"
        _write_book(wrong, [fool[:4] + (5, 1)], collisions=[])
        n, bad = check_terminal_sample(wrong, 10, 1, threads=1, mem="1GB", tmp=tmp / "duck", buckets=8)
        check(n == 1 and len(bad) == 1 and bad[0].startswith("parity"), f"a wrong ply parity is caught ({bad})")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    # 1.25M ~= one read batch (50K games x ~25 plies), the size the extract will
    # actually issue. Measuring on a small batch understates the amortisation.
    ap.add_argument("--sample", type=int, default=1_250_000)
    a = ap.parse_args()

    # Runs on throwaway arrays, so it works with no E: data present.
    print("Staleness guard:")
    check_staleness()
    print("\nEval DB directory source:")
    check_dir_source()

    if not (DEFAULT_ARRAY_DIR / "eval_hash.npy").exists():
        print(f"\n  SKIP: eval arrays not built at {DEFAULT_ARRAY_DIR}")
        n_fail = sum(1 for ok, _ in _checks if not ok)
        print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} "
              f"({len(_checks)} checks — real arrays unavailable)")
        sys.exit(0 if n_fail == 0 else 1)
    print()

    h, e = open_eval_arrays()
    print(f"Arrays: {h.shape[0]:,} entries\n")

    check(bool(np.all(np.asarray(h[:1_000_000])[:-1] <= np.asarray(h[:1_000_000])[1:])),
          "hash array is sorted (prefix check — searchsorted requires it)")

    # Ground truth straight from the parquet, not from the arrays under test.
    src = pl.read_parquet(DEFAULT_EVAL_DB, columns=["position_hash", "eval_cp"]).head(a.sample)
    keys = src["position_hash"].to_numpy().astype(np.int64)
    want = src["eval_cp"].to_numpy().astype(np.int16)

    got = lookup_evals(keys, h, e)
    bad = int((got != want).sum())
    check(bad == 0, f"present keys return the parquet's eval_cp "
                    f"({len(keys):,} sampled, {bad} wrong)")
    if bad:
        i = int(np.nonzero(got != want)[0][0])
        print(f"        first: hash={keys[i]} got={got[i]} want={want[i]}")

    # Absent keys must report MISSING, not a neighbour's evaluation — the failure
    # mode that would quietly assign a random position's score.
    rng = np.random.default_rng(20260804)
    probe = rng.integers(np.iinfo(np.int64).min, np.iinfo(np.int64).max,
                         size=20_000, dtype=np.int64)
    absent = probe[~np.isin(probe, keys)]
    ga = lookup_evals(absent, h, e)
    # A random int64 landing in a 400M-entry table is ~2e-11 likely; any hit is
    # real, so compare against the table rather than asserting all-missing.
    idx = np.clip(np.searchsorted(h, absent), 0, h.shape[0] - 1)
    truly_absent = np.asarray(h[idx]) != absent
    check(bool(np.all(ga[truly_absent] == MISSING)),
          f"absent keys return MISSING, never a neighbour "
          f"({int(truly_absent.sum()):,} probed)")

    check(int(lookup_evals(np.array([], dtype=np.int64), h, e).shape[0]) == 0,
          "empty query returns empty (no crash on an all-filtered batch)")

    # Order independence: the sort/scatter must not permute results.
    perm = rng.permutation(len(keys))
    gp = lookup_evals(keys[perm], h, e)
    check(bool(np.array_equal(gp, got[perm])),
          "results follow the query order, not the sorted order")

    # Throughput. The bar that matters is not a round lookups/s number — it is
    # whether the lookups are cheap RELATIVE to the replay they ride along with.
    # A full 2013-2026 rebuild is ~2.6B ply-lookups against a projected ~62 h
    # extract on 6 workers = ~370 core-hours; the lookups must disappear into
    # that, not merely be "fast".
    PLY_LOOKUPS = 2.6e9
    EXTRACT_CORE_HOURS = 370.0
    BUDGET_FRAC = 0.05

    batch = keys[:min(len(keys), 1_250_000)]
    lookup_evals(batch[:1000], h, e)                      # warm the pages
    t0 = time.time()
    lookup_evals(batch, h, e)
    dt = time.time() - t0
    rate = len(batch) / dt
    core_hours = PLY_LOOKUPS / rate / 3600
    frac = core_hours / EXTRACT_CORE_HOURS
    print(f"\n  info  {len(batch):,} lookups in {dt:.2f}s = {rate/1e6:.2f}M/s")
    print(f"  info  {PLY_LOOKUPS/1e9:.1f}B ply-lookups -> {core_hours:.1f} core-hours "
          f"= {100*frac:.1f}% of a {EXTRACT_CORE_HOURS:.0f} core-hour extract")
    check(frac < BUDGET_FRAC,
          f"lookup cost under {100*BUDGET_FRAC:.0f}% of the extract "
          f"({100*frac:.1f}%, {rate/1e6:.2f}M/s)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
