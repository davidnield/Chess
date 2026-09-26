"""The Rust explorer-extract is exact against the Python extract (skips without the exe).

The Rust tool only earns its place if a book built from it cannot be told apart
from one built by build_pooled_stats. The cheap, silent ways for a port to be
wrong all live at the edges, so the source here is synthetic and built from them:

  games     castling (and castling rights lost), legal and pinned en passant,
            promotions and under-promotions, null moves, UCI/long-form tokens,
            {comments} (variations) $NAGs, Unicode whitespace and digits, `1-0?`,
            null and empty movetext, zero-token games, a NaN score, a mid-game
            parse failure, games past the 30-ply horizon, and every filter
            (elo, no score, termination, BOT -- and a lowercase "bot" that is kept);
  chunks    files of 0, 250,000, 250,001 and 500,000 rows, a 250,001-row file
            whose one-row tail is dropped (so no tail is written), and row groups
            of 100,000 so a chunk spans row groups;
  events    two event directories, read in CLI order.

Required, through compare_explorer_outputs.py (file names, sentinels, Arrow
schemas, and COUNT + SUM(hash(every column)) per file):
  * Rust `partials` == the Python extract at --epd-max-ply 16 and at 30;
  * a killed Rust run (a sentinel removed, a stale chunk planted) resumes to the
    same output, and a rerun with other settings is refused by the params lock;
  * a file whose non-final row group is not a multiple of 50,000 rows is
    refused, as are the flags outside the explorer contract.
  * `month` == a DuckDB consolidation of Rust's epd-30 partials using MIN(ply),
    MIN(epd), MIN(child), on every column including ply, and its term monthly
    == the SUM consolidation of the term partials;
  * `month` at (threads 2, passes 1) and (threads 12, passes 4) writes
    byte-identical files; the manifest is the 26-field contract; a finished
    month is skipped; a pass over --mem-gb stops cleanly naming --passes 2P;
  * against B1 end to end (Python epd-30 extract, consolidation, bucket_month):
    identical but for ply, parity equal, R.ply <= B1.ply, and R.ply == MIN over
    the partials; the term monthlies equal exactly.

  * `dump-plies` agrees with a python-chess replay on every ply of every game
    (token, parent hash, child hash, EPD) and every game's outcome (drop
    reason, token count, failure ply and token, term hash and kind, reason).

The same per-ply differential runs at scale (spec T2) as
    python python/_test_rust_extract.py --plies-check <dump base> --source <root>
over a `dump-plies --games-per-file N --special` of real months.

Run: .venv/Scripts/python.exe python/_test_rust_extract.py
Exe: $EXPLORER_EXTRACT_EXE, else D:/rust-target/explorer_extract/release/explorer-extract.exe
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from stage1_extract_positions import iter_san_moves  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402
EXE = Path(os.environ.get("EXPLORER_EXTRACT_EXE",
                          "D:/rust-target/explorer_extract/release/explorer-extract.exe"))
YEAR, MONTH = 2099, 1
EVENTS = ["Rapid", "Blitz"]          # CLI order is not alphabetical on purpose
EXPLORER = ["--min-elo", "0", "--max-ply", "30", "--chunk-games", "250000",
            "--exclude-terminations", "Rules infraction", "Abandoned"]

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    return bool(ok)


RUY = ("e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3 Nb8 d4 "
       "Nbd7 c4 c6 cxb5 axb5 Nc3 Bb7 Bg5 b4 Nb1 h6 Bh4 c5 dxe5 Nxe4 Bxe7 Qxe7 "
       "exd6 Qf6 Nbd2 Nxd6").split()


def pgn(sans: list[str], result: str = "1-0") -> str:
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i // 2 + 1}.")
        out.append(s)
    return " ".join(out + [result])


# (movetext, white_score, termination, mean_elo, white_title, black_title)
EDGE = [
    (pgn(RUY), 0.5, "Normal", 1850, None, None),
    (pgn(("Nf3 Nf6 Ng1 Ng8 " * 3).split() + RUY[:24]), 1.0, "Normal", 1850, None, None),
    (pgn(RUY[:19] + ["Qxz9"] + RUY[20:]), 0.0, "Time forfeit", 1500, None, None),
    ("1. e4 {best by test} e5 (1... c5 2. Nf3) 2. Nf3 $1 Nc6 3. Bb5!? a6?! 1-0",
     1.0, "Normal", 1650, None, None),
    ("1. e4 Nf6 2. e5 d5 3. exd6 cxd6 4. Nf3 g6 1-0", 1.0, "Normal", 1999, None, None),
    (pgn("e4 a6 e5 a5 Ke2 Ra6 Kd3 Rh6 Kc4 Rh5 Kc5 f5 d4 g5 Kb5 Nf6".split()),
     0.5, "Normal", 2000, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=Q Nb6 6. Qxb8 1-0",
     1.0, "Normal", 2600, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=N Nb6 0-1", 0.0, "Normal", 1200, None, None),
    ("1. e4 -- 2. d4 Z0 3. Nf3 0000 4. c4 @@@@ 5. Nc3 e6 1-0", 1.0, "Normal", 1700, None, None),
    ("1. e4 e5 2. Nf3 Nc6 3. Bc4 Bc5 4. e1h1 Nf6 5. d3 O-O 1-0", 1.0, "Normal", 2100, None, None),
    ("1. e2-e4 e7e5 2. Ng1-f3 Nxc6 3. g1f3 1-0", 1.0, "Normal", 1350, None, None),
    ("1. d4 d5 2. Nc3 Nc6 3. Bf4 Bf5 4. Qd2 Qd7 5. O-O-O O-O-O 6. e3 e6 7. f3 f6 "
     "8. g4 Bg6 9. h4 h5 10. Kb1 Kb8 1/2-1/2", 0.5, "Normal", 2300, None, None),
    ("1. e4 e5 2. Ke2 Ke7 3. Ke1 Ke8 4. Nf3 Nf6 1-0", 1.0, "Normal", 1400, None, None),
    ("e4\u00a0e5\u3000Nf3\u001cNc6 \u0661. Bb5 $\u0662 a6 1-0?", 1.0, "Normal", 1600, None, None),
    (None, 0.5, "Normal", 1800, None, None),
    ("", 1.0, "Normal", 1800, None, None),
    ("1-0", 1.0, "Time forfeit", 1400, None, None),
    ("{only a comment} *", 0.0, None, 1400, None, None),
    (pgn(RUY[:10]), float("nan"), "Normal", 1800, None, None),
    (pgn(RUY[:30]), 1.0, "Normal", 999, None, None),
    (pgn(RUY[:31]), 0.0, "Normal", 1000, None, None),
    (pgn(RUY[:6]), None, "Normal", 1800, None, None),
    (pgn(RUY[:6]), 1.0, "Normal", None, None, None),
    (pgn(RUY[:6]), 1.0, "Rules infraction", 1800, None, None),
    (pgn(RUY[:6]), 1.0, "Abandoned", 1800, None, None),
    (pgn(RUY[:6]), 1.0, "Normal", 1800, "BOT", None),
    (pgn(RUY[:6]), 1.0, "Normal", 1800, None, "BOT"),
    (pgn(RUY[:6]), 1.0, "Normal", 1800, "bot", "LM"),
    (pgn(RUY[:6]), 0.25, "Normal", -5, None, None),
    (pgn(RUY[:33]), 0.0, "Normal", 2800, None, "GM"),
    (pgn(RUY[:12]), -0.0, "Normal", 2500, None, None),
]
DROPPED = (pgn(RUY[:8]), 1.0, "Normal", None, None, None)     # an elo drop: cheap to skip


def write_source(path: Path, games: list[tuple], rg: int = 100_000) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*games)) if games else [[] for _ in range(6)]
    n = len(games)
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
    }), path, row_group_size=rg, compression="zstd")
    return path


def padded(n: int, head: list[tuple], tail: list[tuple] = ()) -> list[tuple]:
    """n rows: `head`, then elo-dropped filler, then `tail`."""
    return list(head) + [DROPPED] * (n - len(head) - len(tail)) + list(tail)


def build_tree(root: Path) -> None:
    ev = root / f"year={YEAR}" / f"month={MONTH}"
    write_source(ev / "event=Blitz" / "part-0.parquet", EDGE * 3)
    write_source(ev / "event=Blitz" / "part-1.parquet", [])
    write_source(ev / "event=Blitz" / "part-2.parquet", padded(250_000, EDGE))
    write_source(ev / "event=Rapid" / "part-0.parquet", padded(250_001, EDGE, [EDGE[0]]))
    write_source(ev / "event=Rapid" / "part-1.parquet", padded(250_001, EDGE[:5]))
    write_source(ev / "event=Rapid" / "part-2.parquet", padded(500_000, EDGE[5:]))
    # Not an event on the CLI: must be ignored.
    write_source(ev / "event=Chess960" / "part-0.parquet", EDGE)


def python_extract(root: Path, out: Path, epd: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HERE / "build_pooled_stats.py"), "--start-year", str(YEAR),
         "--end-year", str(YEAR), "--months", str(MONTH), "--phase", "extract",
         "--events", *EVENTS, "--no-prune", "--no-fuse-winpos", "--no-child-eval",
         "--exclude-bots", *EXPLORER, "--workers", "2", "--source", str(root),
         "--partial-dir", str(out), "--tag", "rust_test", "--epd-max-ply", str(epd)],
        capture_output=True, text=True, timeout=1800)


def rust(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([str(EXE), *args], capture_output=True, text=True, timeout=1800)


def rust_partials(root: Path, out: Path, epd: int, *extra: str) -> subprocess.CompletedProcess:
    return rust("partials", "--source", str(root), "--months", f"{YEAR}_{MONTH}",
                "--events", *EVENTS, *EXPLORER, "--threads", "4",
                "--partial-dir", str(out), "--epd-max-ply", str(epd), *extra)


# ── the per-ply differential (dump-plies vs python-chess) ─────────────────────

MAX_PLY = 30
_EXCLUDED = {"Rules infraction", "Abandoned"}
_REASON = {"Normal": 0, "Time forfeit": 1, "Abandoned": 2}


def py_game(movetext, ws, term, me, wt, bt) -> tuple[dict, list[tuple]]:
    """What the Python extract's filters and _walk_game make of one game, walked
    whatever the filters say (the dump walks every selected game)."""
    if me is None or me < 0:
        drop = "elo"
    elif ws is None:
        drop = "no_score"
    elif term in _EXCLUDED:
        drop = "termination"
    elif wt == "BOT" or bt == "BOT":
        drop = "bot"
    else:
        drop = None
    toks = list(iter_san_moves(movetext)) if movetext else []
    g = {"drop": drop, "n_tokens": min(len(toks), MAX_PLY + 1), "outcome": "done",
         "failed_ply": None, "failed_token": None, "term_hash": None, "term_kind": None,
         "reason": _REASON.get(term, 3)}
    plies: list[tuple] = []
    board = chess.Board()
    ph = zobrist_int64(board)
    maxply = min(MAX_PLY, len(toks))
    for ply in range(1, maxply + 1):
        tok = toks[ply - 1]
        epd = board.epd()
        try:
            mv = board.parse_san(tok)
        except (ValueError, AssertionError):
            g.update(outcome="failed", failed_ply=ply, failed_token=tok)
            return g, plies
        if not mv and board.is_check():
            g.update(outcome="null_in_check", failed_ply=ply, failed_token=tok)
            return g, plies
        board.push(mv)
        ch = zobrist_int64(board)
        plies.append((ply, tok, ph, ch, epd))
        ph = ch
    g["term_hash"] = ph
    g["term_kind"] = 0 if maxply >= len(toks) else 1
    return g, plies


def _check_file(task: tuple) -> tuple[int, int, int, list[str]]:
    """One source file's games: (games, plies, differences, examples)."""
    src, games, plies = task
    t = pq.read_table(src, columns=["movetext", "white_score", "termination", "mean_elo",
                                    "white_title", "black_title"])
    rows = games["row"]
    sub = t.take(pa.array(rows, pa.int64())).to_pylist()
    by_row: dict[int, list[tuple]] = {}
    for r, ply, tok, ph, ch, epd in zip(plies["row"], plies["ply"], plies["token"],
                                        plies["parent_hash"], plies["child_hash"], plies["epd"]):
        by_row.setdefault(r, []).append((ply, tok, ph, ch, epd))
    n_plies, bad, ex = 0, 0, []
    keys = ("drop", "n_tokens", "outcome", "failed_ply", "failed_token", "term_hash",
            "term_kind", "reason")
    for i, r in enumerate(rows):
        s = sub[i]
        want_g, want_p = py_game(s["movetext"], s["white_score"], s["termination"],
                                 s["mean_elo"], s["white_title"], s["black_title"])
        got_g = {k: games[k][i] for k in keys}
        got_p = sorted(by_row.get(r, []))
        n_plies += len(want_p)
        if got_g != want_g or got_p != want_p:
            bad += 1
            if len(ex) < 5:
                diff_p = next((f"ply {a[0]}: rust {a} vs python {b}"
                               for a, b in zip(got_p, want_p) if a != b),
                              f"{len(got_p)} vs {len(want_p)} plies")
                ex.append(f"{src} row {r}: game rust {got_g} vs python {want_g}; {diff_p}")
    return len(rows), n_plies, bad, ex


def check_plies(base: Path, root: Path, workers: int = 4) -> tuple[int, int, int, list[str]]:
    games = pq.read_table(f"{base}.games.parquet")
    plies = pq.read_table(f"{base}.plies.parquet")
    tasks = []
    for f in pc.unique(games["file"]).to_pylist():
        gm = games.filter(pc.equal(games["file"], f)).to_pydict()
        pm = plies.filter(pc.equal(plies["file"], f)).to_pydict()
        tasks.append((str(root / f), gm, pm))
    tot = [0, 0, 0, []]
    with ProcessPoolExecutor(max_workers=max(1, min(workers, len(tasks)))) as ex:
        futs = [ex.submit(_check_file, t) for t in tasks]
        for fu in futs:
            n, p, b, e = fu.result()
            tot[0] += n
            tot[1] += p
            tot[2] += b
            tot[3] += e[: 20 - len(tot[3])]
    return tot[0], tot[1], tot[2], tot[3]


PS_COLS = ("parent_hash, move_san, event, elo_band, parent_epd, child_hash, child_eval, "
           "ply, white_wins, draws, black_wins, total")


def rust_month(root: Path, out: Path, *extra: str, mem_gb: str = "2") -> subprocess.CompletedProcess:
    return rust("month", "--source", str(root), "--months", f"{YEAR}_{MONTH}",
                "--events", *EVENTS, *EXPLORER, "--out", str(out), "--mem-gb", mem_gb, *extra)


def month_checks(root: Path, tmp: Path) -> None:
    """Month mode against the partials it must be derivable from, against
    itself under another thread and pass count, and against B1's bucket_month."""
    import duckdb
    print("\nmonth mode")
    m1, m4 = tmp / "m1", tmp / "m4"
    r1 = rust_month(root, m1, "--passes", "1", "--threads", "2")
    r4 = rust_month(root, m4, "--passes", "4", "--threads", "12")
    check(r1.returncode == 0 and r4.returncode == 0,
          f"month runs at (threads 2, passes 1) and (threads 12, passes 4) "
          f"(rc {r1.returncode}, {r4.returncode}) {r1.stderr[-300:] if r1.returncode else ''}")
    tag = f"{YEAR}_{MONTH}"
    con = duckdb.connect()
    sp = lambda x: str(x).replace("\\", "/")                                  # noqa: E731
    con.execute(f"""
        CREATE TABLE ref AS
        SELECT parent_hash, move_san, event, elo_band, MIN(parent_epd) AS parent_epd,
               MIN(child_hash) AS child_hash, CAST(NULL AS INTEGER) AS child_eval,
               MIN(ply) AS ply, SUM(white_wins)::BIGINT AS white_wins,
               SUM(draws)::BIGINT AS draws, SUM(black_wins)::BIGINT AS black_wins,
               SUM(total)::BIGINT AS total
        FROM read_parquet('{sp(tmp / 'rs30')}/*.ps.parquet') GROUP BY ALL""")
    diffs = {}
    for name, d in (("m1", m1), ("m4", m4)):
        con.execute(f"CREATE OR REPLACE VIEW m AS SELECT {PS_COLS} FROM read_parquet("
                    f"'{sp(d)}/month={tag}/bkt=*/*.parquet', hive_partitioning=false)")
        a = con.execute("SELECT COUNT(*) FROM (SELECT * FROM ref EXCEPT ALL SELECT * FROM m)").fetchone()[0]
        b = con.execute("SELECT COUNT(*) FROM (SELECT * FROM m EXCEPT ALL SELECT * FROM ref)").fetchone()[0]
        diffs[name] = (con.execute("SELECT COUNT(*) FROM m").fetchone()[0], a, b)
    n = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    check(n > 300 and diffs["m1"] == (n, 0, 0) and diffs["m4"] == (n, 0, 0),
          f"month == a DuckDB consolidation of Rust's epd-30 partials with MIN(ply), "
          f"MIN(epd), MIN(child): {n} rows, every column, ply included ({diffs})")
    t_ref = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT position_hash, kind, reason, SUM(white_wins)::BIGINT, SUM(draws)::BIGINT,
                 SUM(black_wins)::BIGINT, SUM(total)::BIGINT
          FROM read_parquet('{sp(tmp / 'rs30')}/*.term.parquet') GROUP BY ALL
          EXCEPT ALL SELECT * FROM read_parquet('{sp(m1)}/_term/year={YEAR}_month={MONTH}.term.parquet'))
        """).fetchone()[0]
    check(t_ref == 0, "the term monthly == the SUM consolidation of the term partials")
    b1 = sorted(p.relative_to(m1).as_posix() for p in (m1 / f"month={tag}").rglob("*.parquet"))
    b4 = sorted(p.relative_to(m4).as_posix() for p in (m4 / f"month={tag}").rglob("*.parquet"))
    same = b1 == b4 and all((m1 / f).read_bytes() == (m4 / f).read_bytes() for f in b1)
    t1 = (m1 / "_term" / f"year={YEAR}_month={MONTH}.term.parquet").read_bytes()
    t4 = (m4 / "_term" / f"year={YEAR}_month={MONTH}.term.parquet").read_bytes()
    check(same and t1 == t4,
          f"(threads 2, passes 1) and (threads 12, passes 4) write byte-identical "
          f"bucket files ({len(b1)}) and term monthlies")
    sys.path.insert(0, str(HERE))
    import bucket_month as bm
    man = pq.read_schema(m1 / "_manifest" / f"month={tag}.parquet")
    check([(f.name, str(f.type)) for f in man] == list(bm.MANIFEST_FIELDS)
          and (m1 / f"_month={tag}.DONE").exists() and not (m1 / f"_tmp_month={tag}").exists(),
          "the 26-field manifest in bucket_month's order and types, the sentinel, no _tmp_month")
    again = rust_month(root, m1, "--passes", "1", "--threads", "2")
    check(again.returncode == 0 and "already done" in again.stderr,
          "a month with its sentinel is skipped on a rerun")
    tiny = rust_month(root, tmp / "m_tiny", "--passes", "1", mem_gb="0.00001")
    check(tiny.returncode != 0 and "rerun with --passes 2" in tiny.stderr,
          "a pass over the --mem-gb budget stops cleanly, naming --passes 2P")

    # B1 end to end: the Python epd-30 partials, consolidated, then bucket_month.
    import contextlib
    import io
    from build_pooled_stats import consolidate_monthly
    with contextlib.redirect_stdout(io.StringIO()):
        mdir = consolidate_monthly(tmp / "py30", 2, "2GB", tmp / "_cons", ("ps", "term"))
        bm.bucket_month(mdir / f"year={YEAR}_month={MONTH}.ps.parquet", YEAR, MONTH,
                        tmp / "b1", tmp / "b1_work", 512, 2, 2, "2GB", tmp / "b1_duck")
    p = subprocess.run([sys.executable, str(HERE / "compare_explorer_outputs.py"), "month",
                        str(tmp / "b1"), str(m1), "--month", tag, "--ply-le",
                        "--min-ply-from", str(tmp / "rs30"), "--tmp-dir", str(tmp / "_cmp")],
                       capture_output=True, text=True, timeout=1800)
    check(p.returncode == 0,
          f"Rust month vs B1 (extract epd 30, consolidate, bucket_month): identical but for "
          f"ply, parity equal, R.ply <= B1.ply, R.ply == MIN over the partials (rc {p.returncode})")
    if p.returncode:
        print(p.stdout[-2500:])
    tq = subprocess.run([sys.executable, str(HERE / "compare_explorer_outputs.py"), "term",
                         str(mdir), str(m1 / "_term"), "--month", tag,
                         "--tmp-dir", str(tmp / "_cmp")], capture_output=True, text=True)
    check(tq.returncode == 0, "and the term monthly equals B1's consolidated one exactly")


def compare(a: Path, b: Path, tmp: Path) -> tuple[int, str]:
    p = subprocess.run([sys.executable, str(HERE / "compare_explorer_outputs.py"),
                        "partials", str(a), str(b), "--tmp-dir", str(tmp / "_cmp")],
                       capture_output=True, text=True, timeout=1800)
    return p.returncode, p.stdout + p.stderr


def main() -> None:
    if not EXE.exists():
        print(f"  SKIP  no explorer-extract at {EXE} (set EXPLORER_EXTRACT_EXE)")
        print("\nALL PASS (0 checks -- exe unavailable)")
        sys.exit(0)
    st = rust("selftest")
    check(st.returncode == 0 and "SELFTEST PASS" in st.stdout,
          f"selftest passes ({st.stdout.strip().splitlines()[-1] if st.stdout else st.stderr})")
    tmp = Path(tempfile.mkdtemp(prefix="test_rust_extract_"))
    try:
        root = tmp / "src"
        build_tree(root)
        for epd in (16, 30):
            print(f"\npartials at --epd-max-ply {epd}")
            py, rs = tmp / f"py{epd}", tmp / f"rs{epd}"
            pp = python_extract(root, py, epd)
            check(pp.returncode == 0, f"the Python extract runs (rc {pp.returncode})")
            rp = rust_partials(root, rs, epd)
            check(rp.returncode == 0, f"the Rust extract runs (rc {rp.returncode}) "
                                      f"{rp.stderr.strip().splitlines()[-1] if rp.returncode else ''}")
            rc, log = compare(py, rs, tmp)
            check(rc == 0 and "IDENTICAL" in log,
                  "Rust partials == Python partials: file names, sentinels, schemas and "
                  "every row of every file, ply included")
            if rc:
                print(log[-3000:])
            names = sorted(p.name for p in rs.glob("*.ps.parquet"))
            want = ["year=2099_month=1_event=Blitz_part-0_c000.ps.parquet",
                    "year=2099_month=1_event=Blitz_part-1_c000.ps.parquet",
                    "year=2099_month=1_event=Blitz_part-2_c000.ps.parquet",
                    "year=2099_month=1_event=Rapid_part-0_c000.ps.parquet",
                    "year=2099_month=1_event=Rapid_part-0_c001.ps.parquet",
                    "year=2099_month=1_event=Rapid_part-1_c000.ps.parquet",
                    "year=2099_month=1_event=Rapid_part-2_c000.ps.parquet",
                    "year=2099_month=1_event=Rapid_part-2_c001.ps.parquet"]
            check(names == want,
                  "the chunk rule: empty file -> c000; 250,000 rows -> no tail; a one-row "
                  "tail with a game -> c001, without one -> none; 500,000 rows -> two full "
                  "chunks, the second empty")

        print("\nresume, the lock, and refusals")
        rs = tmp / "rs16"
        sent = rs / "_year=2099_month=1_event=Rapid_part-0.DONE"
        sent.unlink()
        (rs / "year=2099_month=1_event=Rapid_part-0_c007.ps.parquet").write_bytes(b"stale")
        rp = rust_partials(root, rs, 16)
        rc, _ = compare(tmp / "py16", rs, tmp)
        check(rp.returncode == 0 and rc == 0 and sent.exists(),
              "a file without its sentinel is redone, its stale chunks cleared, and the "
              "dir compares identical again")
        rp = rust_partials(root, rs, 30)
        check(rp.returncode != 0 and "_extract_params.json" in rp.stderr,
              "a rerun at --epd-max-ply 30 into the epd-16 dir is refused by the lock")
        rp = rust_partials(root, tmp / "rs_gap", 16, "--max-rating-gap", "300")
        check(rp.returncode != 0 and "explorer contract" in rp.stderr,
              "--max-rating-gap is refused: only the explorer contract is supported")
        bad = tmp / "bad"
        write_source(bad / f"year={YEAR}" / f"month={MONTH}" / "event=Blitz" / "part-0.parquet",
                     padded(120_000, EDGE), rg=70_000)
        rp = rust("partials", "--source", str(bad), "--months", f"{YEAR}_{MONTH}",
                  "--events", "Blitz", "--partial-dir", str(tmp / "rs_bad"),
                  "--epd-max-ply", "16")
        check(rp.returncode != 0 and "multiple of 50000" in rp.stderr,
              "a non-final row group of 70,000 rows is refused")
        case = tmp / "case"
        write_source(case / f"year={YEAR}" / f"month={MONTH}" / "event=blitz" / "part-0.parquet",
                     EDGE)
        rp = rust("partials", "--source", str(case), "--months", f"{YEAR}_{MONTH}",
                  "--events", "Blitz", "--partial-dir", str(tmp / "rs_case"),
                  "--epd-max-ply", "16")
        check(rp.returncode != 0 and "exactly" in rp.stderr,
              "an event directory differing only in case is refused, not silently read")

        month_checks(root, tmp)

        print("\ndump-plies against python-chess")
        base = tmp / "dump" / "plies"
        base.parent.mkdir()
        rp = rust("dump-plies", "--source", str(root), "--months", f"{YEAR}_{MONTH}",
                  "--events", *EVENTS, "--games-per-file", "200", "--special",
                  "--threads", "4", "--out", str(base))
        check(rp.returncode == 0, f"dump-plies runs (rc {rp.returncode})")
        n, p, bad, ex = check_plies(base, root)
        check(n > 300 and p > 3000 and bad == 0,
              f"every ply and every game outcome agrees: {n:,} games, {p:,} plies, "
              f"{bad} differing games")
        for e in ex:
            print("    " + e)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


def plies_cli() -> None:
    ap = argparse.ArgumentParser(description="dump-plies vs python-chess, at scale")
    ap.add_argument("--plies-check", required=True, type=Path, metavar="DUMP_BASE")
    ap.add_argument("--source", required=True, type=Path)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    t0 = time.time()
    n, p, bad, ex = check_plies(a.plies_check, a.source, a.workers)
    for e in ex:
        print("  " + e)
    print(f"{n:,} games, {p:,} plies: {bad} differing games ({time.time()-t0:,.0f}s)")
    sys.exit(0 if bad == 0 else 1)


if __name__ == "__main__":
    if "--plies-check" in sys.argv:
        plies_cli()
    else:
        main()
