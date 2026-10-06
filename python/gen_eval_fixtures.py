"""Write the `explorer-extract evals` test fixtures from python-chess and the real eval datasets.

The eval DB keys every source FEN by the book's hash and EPD, so its position
identity must agree with python-chess 1.11.2 (`zobrist.zobrist_int64`,
`board.epd()`) on real inputs from both datasets. This script samples FENs
from the downloads (read-only), adds hand cases (castling, promotions, legal
and pinned en passant, Chess960 castling, garbage), and writes what
python-chess says about each. It also copies the cloud rows of the spec's
three pick examples verbatim.

Writes rust/explorer_extract/tests/fixtures/eval_positions.json:

  cases        {fen, epd, hashes, played_hash?}: `hashes` is the canonical
               hash (the board with its ep square only if the capture is legal)
               then every ep variant, sorted. A variant sets an ep square the
               position could carry -- an enemy pawn that could just have
               double-pushed, its skipped and origin squares empty -- that
               python-chess accepts (`is_valid`), whose EPD is identical and
               whose hash differs: a pseudo-legal but illegal capture.
               `played_hash` is the hash of a board reached by playing moves,
               which must be one of `hashes`.
  fails        FENs python-chess rejects (`is_valid` false, or a parse error):
               the Rust side must skip them.
  cloud_rows   the raw cloud rows (file order) of the spec's pick examples.

Usage:
    .venv/Scripts/python.exe python/gen_eval_fixtures.py [--evals H:/chess/evals] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import chess
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zobrist import zobrist_int64  # noqa: E402

HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE.parent / "rust" / "explorer_extract" / "tests" / "fixtures"

PICK_FENS = [
    "7r/1p3k2/p1bPR3/5p2/2B2P1p/8/PP4P1/3K4 b - -",
    "8/4r3/2R2pk1/6pp/3P4/6P1/5K1P/8 b - -",
    "6k1/6p1/8/4K3/4NN2/8/8/8 w - -",
]


def canonical(board: chess.Board) -> chess.Board:
    b = board.copy(stack=False)
    if not b.has_legal_en_passant():
        b.ep_square = None
    return b


def identity(fen: str) -> dict | None:
    """python-chess's view of a source FEN, or None if it is not a valid standard position."""
    try:
        board = chess.Board(fen)
    except ValueError:
        return None
    if not board.is_valid():
        return None
    base = canonical(board)
    epd = base.epd()
    hashes = [zobrist_int64(base)]
    variants = set()
    if not base.has_legal_en_passant():
        them = not base.turn
        to_rank, ep_rank, from_rank = (4, 5, 6) if base.turn == chess.WHITE else (3, 2, 1)
        for f in range(8):
            to, ep, frm = chess.square(f, to_rank), chess.square(f, ep_rank), chess.square(f, from_rank)
            if base.piece_at(to) != chess.Piece(chess.PAWN, them):
                continue
            if base.piece_at(ep) is not None or base.piece_at(frm) is not None:
                continue
            v = base.copy(stack=False)
            v.ep_square = ep
            if not v.is_valid() or v.epd() != epd:
                continue
            h = zobrist_int64(v)
            if h != hashes[0]:
                variants.add(h)
    return {"fen": fen, "epd": epd, "hashes": hashes + sorted(variants)}


def played(moves: str, fen: str = chess.STARTING_FEN) -> chess.Board:
    b = chess.Board(fen)
    for m in moves.split():
        b.push_uci(m)
    return b


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--evals", type=Path, default=Path("H:/chess/evals"))
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    a = ap.parse_args()
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET enable_progress_bar=false")
    cloud = (a.evals / "cloud").as_posix()
    fish = (a.evals / "fishnet").as_posix()

    fens: list[str] = []
    q = lambda sql: [r[0] for r in con.execute(sql).fetchall()]  # noqa: E731
    # Real FENs: plain samples, printed ep squares, castling variety, promotions.
    for f in (f"{cloud}/data_0000.parquet", f"{cloud}/data_0013.parquet"):
        fens += q(f"SELECT DISTINCT fen FROM read_parquet('{f}') USING SAMPLE 300 ROWS")
        fens += q(f"SELECT DISTINCT fen FROM (SELECT fen FROM read_parquet('{f}') "
                  f"WHERE split_part(fen, ' ', 4) <> '-' LIMIT 60)")
        fens += q(f"SELECT DISTINCT fen FROM (SELECT fen FROM read_parquet('{f}') "
                  f"WHERE regexp_matches(split_part(fen, ' ', 1), 'Q.*Q|q.*q') LIMIT 30)")
    for f in ("standard_rated_2014_05", "standard_rated_2019_01", "standard_rated_2024_06"):
        p = f"{fish}/{f}.parquet"
        fens += q(f"SELECT fen FROM (SELECT fen FROM read_parquet('{p}') LIMIT 200000) USING SAMPLE 200 ROWS")
        fens += q(f"SELECT DISTINCT fen FROM (SELECT fen FROM read_parquet('{p}') "
                  f"WHERE split_part(fen, ' ', 4) <> '-' LIMIT 60)")
        fens += q(f"SELECT DISTINCT fen FROM (SELECT fen FROM read_parquet('{p}') "
                  f"WHERE split_part(fen, ' ', 3) IN ('K', 'Qk', 'kq', 'Kq') LIMIT 20)")
        fens += q(f"SELECT DISTINCT fen FROM (SELECT fen FROM read_parquet('{p}') "
                  f"WHERE regexp_matches(split_part(fen, ' ', 1), 'Q.*Q|q.*q|N.*N.*N') LIMIT 20)")

    cases = []
    fails = []
    # Hand cases.
    pinned = played("e2e4 a7a6 e4e5 a6a5 e1e2 a8a6 e2d3 a6h6 d3c4 h6h5 c4c5 f7f5")
    # Black to move: d4 is pinned to the king on a4 by the rook on h4.
    black_pinned = played("e2e4", "8/8/8/8/k2p3R/8/4P3/4K3 w - - 0 1")
    legal_ep = played("e2e4 d7d5 e4e5 f7f5")
    hand = [
        (pinned.epd(), zobrist_int64(pinned)),
        (pinned.fen(en_passant="fen"), zobrist_int64(pinned)),
        (pinned.fen(), zobrist_int64(pinned)),
        (black_pinned.epd(), zobrist_int64(black_pinned)),
        (black_pinned.fen(en_passant="fen"), zobrist_int64(black_pinned)),
        (legal_ep.epd(), zobrist_int64(legal_ep)),
        (legal_ep.fen(), zobrist_int64(legal_ep)),
        (legal_ep.board_fen() + " w KQkq -", None),
        (chess.STARTING_FEN, zobrist_int64(chess.Board())),
        ("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1", None),
        ("r3k2r/8/8/8/8/8/8/R3K2R b Kq - 0 1", None),
        ("4k3/1P6/8/8/8/8/6p1/4K3 w - -", None),
        ("QQQQkQQQ/8/8/8/8/8/8/4K3 b - -", None),
    ]
    for fen, ph in hand:
        c = identity(fen)
        if c is None:
            fails.append(fen)
            continue
        if ph is not None:
            c["played_hash"] = ph
            assert ph in c["hashes"], (fen, ph, c)
        cases.append(c)
    fails += [
        "bqnb1rkr/pp3ppp/3ppn2/2p5/5P2/P2P4/NPP1P1PP/BQ1BNRKR w HFhf - 2 9",
        "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq e3",
        "8/8/8/8/8/8/8/8 w - -",
        "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBN w KQkq -",
        "not a fen",
        "4k3/8/8/8/8/8/8/4K2R w KQ -",
    ]
    for fen in dict.fromkeys(fens):
        c = identity(fen)
        (cases.append(c) if c is not None else fails.append(fen))
    fails = [f for f in dict.fromkeys(fails) if identity(f) is None]
    n_var = sum(len(c["hashes"]) > 1 for c in cases)
    print(f"{len(cases)} cases ({n_var} with ep variants), {len(fails)} fails")
    if n_var < 3:
        raise SystemExit("FATAL: fewer than 3 ep-variant cases; the fixture would not test the rule")

    lit = ", ".join(f"'{f}'" for f in PICK_FENS)
    rows = con.execute(f"""
        SELECT fen, line, depth, knodes, cp, mate, filename, file_row_number
        FROM read_parquet('{cloud}/*.parquet', filename=true, file_row_number=true)
        WHERE fen IN ({lit}) ORDER BY filename, file_row_number""").fetchall()
    cloud_rows = [
        {"fen": r[0], "line": r[1], "depth": int(r[2]) if r[2] is not None else None, "knodes": r[3],
         "cp": r[4], "mate": r[5], "file": Path(r[6]).name, "row": r[7]} for r in rows]
    for f in PICK_FENS:
        if not any(r["fen"] == f for r in cloud_rows):
            raise SystemExit(f"FATAL: pick example {f!r} not found in the cloud dataset")
    print(f"{len(cloud_rows)} cloud rows for the {len(PICK_FENS)} pick examples")

    a.out.mkdir(parents=True, exist_ok=True)
    out = a.out / "eval_positions.json"
    out.write_text(json.dumps({"generator": "python/gen_eval_fixtures.py", "python_chess": chess.__version__,
                               "cases": cases, "fails": fails, "cloud_rows": cloud_rows}, indent=1) + "\n",
                   encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
