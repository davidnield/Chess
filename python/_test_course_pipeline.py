"""course_ingest.py + course_books.py on a synthetic course (2026-10-19).

One White course with every case the parser and classifier must handle:
  intro, three lines (1.e4 e5 2.Nf3 Nc6 3.Bb5 / 2...Nf6 3.Nxe5 / 1...c5 2.Nf3 d6 3.d4),
  an "Alternative" item (3.Bc4), a Quickstarter, a model game with Elo headers,
  a FEN endgame exercise, a line where OUR move carries "?" (refuted), and an
  ebook-style item whose header and movetext are split by two blank lines and
  which starts from a FEN after 1.e4 e5 2.Nf3 Nc6.

  A  item types: intro, line, alternative, quickstarter, model_game, endgame
  B  the split ebook item is re-joined: its FEN is honoured, no parse errors, and
     its first move leaves the position after 1.e4 e5 2.Nf3 Nc6
  C  the repertoire side is inferred as White
  D  the recommended move after 1.e4 e5 2.Nf3 Nc6 is Bb5 (lines, model game, the
     ebook item) with Bc4 kept as an alternative; the refuted 4.Nxe5? is never
     recommended
  E  the book covers Black's 1...e5 and 1...c5, follows 3.Bb5, and does not
     contain positions reachable only through the alternative 3.Bc4
  F  the intro's comment is stored at its position
  G  the vectorised weight equals edge_weight() on every type/depth/NAG case
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import course_books  # noqa: E402
import course_ingest as ci  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def h_after(*sans):
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


def item(white, black, moves, result="*", extra=""):
    return (f'[Event "?"]\n[Site "?"]\n[Date "????.??.??"]\n[Round "?"]\n'
            f'[White "{white}"]\n[Black "{black}"]\n[Result "{result}"]\n{extra}\n'
            f"{moves} {result}\n\n")


MODEL = ("1.e4 e5 2.Nf3 Nc6 3.Bb5 a6 4.Ba4 Nf6 5.O-O Be7 6.Re1 b5 7.Bb3 d6 8.c3 O-O "
         "9.h3 Nb8 10.d4 Nbd7 11.Nbd2 Bb7 12.Bc2 Re8 13.Nf1 Bf8 14.Ng3 g6 15.a4 c5 "
         "16.d5 c4 17.Bg5 h6 18.Be3 Nc5 19.Qd2 h5 20.Bg5 Be7 21.Ra3")
FEN_NC6 = "r1bqkbnr/pppp1ppp/2n5/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq - 2 3"
PGN = (
    item("Introduction", "Welcome", "{This course is about 1.e4.} 1.e4")
    + item("Ch1", "Line 1", "1.e4 e5 2.Nf3 Nc6 3.Bb5")
    + item("Ch1", "Line 2", "1.e4 e5 2.Nf3 Nf6 3.Nxe5")
    + item("Ch1", "Line 3", "1.e4 c5 2.Nf3 d6 3.d4")
    + item("Ch1", "Alternative 3.Bc4", "1.e4 e5 2.Nf3 Nc6 3.Bc4")
    + item("Quickstarter", "QS 1", "1.e4 e5 2.Nf3")
    + item("Carlsen, Magnus", "Anand, Viswanathan", MODEL, "1-0",
           '[WhiteElo "2850"]\n[BlackElo "2790"]')
    + item("Exercise 1", "Mate in one", "1.Rd8#", "*",
           '[SetUp "1"]\n[FEN "6k1/5ppp/8/8/8/8/5PPP/3R2K1 w - - 0 1"]')
    + item("Ch1", "Line 4", "1.e4 e5 2.Nf3 Nc6 3.Bc4 Nd4 4.Nxe5? Qg5")
    # ebook style: header block, two blank lines, then movetext from a FEN
    + f'[Event "Chapter 2-1"]\n[Site "Game 1"]\n[Date "????.??.??"]\n[Round "?"]\n'
      f'[White "Chapter 2-1"]\n[Black "Game 1"]\n[Result "*"]\n[SetUp "1"]\n'
      f'[FEN "{FEN_NC6}"]\n\n\n3.Bb5 a6 4.Ba4\n *\n\n'
)


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="course_test_"))
    try:
        root = tmp / "root"
        cdir = root / "White Repertoires" / "1.e4 e5" / "Test Course (2020) [TestPub]"
        cdir.mkdir(parents=True)
        (cdir / "course.pgn").write_text(PGN, encoding="utf-8")
        work = tmp / "work"
        meta = ci.course_meta(ci.course_dirs(root)[0], root)
        info = ci.process_course((meta, str(root), str(work)))
        check("ingest runs", info["status"] == "ok", info.get("error", "")[:300])
        ci.finalize(work)
        items = pl.read_parquet(work / "items.parquet")
        types = dict(zip(items["black"].to_list(), items["item_type"].to_list()))
        want = {"Welcome": "intro", "Line 1": "line", "Alternative 3.Bc4": "alternative",
                "QS 1": "quickstarter", "Anand, Viswanathan": "model_game",
                "Mate in one": "endgame", "Line 4": "line", "Game 1": "line"}
        bad = {k: (types.get(k), v) for k, v in want.items() if types.get(k) != v}
        check("A: item types", not bad, f"{bad}")
        eb = items.filter(pl.col("black") == "Game 1")
        edges = pl.read_parquet(work / "edges.parquet")
        eb_edges = edges.join(eb.select("file_idx", "item_idx"), on=["file_idx", "item_idx"])
        first = eb_edges.filter(pl.col("parent_node") == 0)
        check("B: the split ebook item is re-joined with its FEN",
              eb.height == 1 and eb["n_errors"][0] == 0 and first.height == 1
              and first["parent_hash"][0] == h_after("e4", "e5", "Nf3", "Nc6")
              and first["move_san"][0] == "Bb5",
              f"{eb.select('n_errors', 'mainline_plies').rows()}, {first.rows()}")

        sys.argv = ["course_books", "--work", str(work), "--no-evals"]
        course_books.main()
        stats = pl.read_parquet(work / "course_books.parquet")
        check("C: one White book", stats["side"].to_list() == ["white"], f"{stats.rows()}")
        book = pl.read_parquet(next((work / "books").glob("*_white.parquet")))
        bm = dict(zip(book["position_hash"].to_list(), book["best_move"].to_list()))
        alts = dict(zip(book["position_hash"].to_list(), book["alt_moves"].to_list()))
        p_nc6 = h_after("e4", "e5", "Nf3", "Nc6")
        check("D: 3.Bb5 recommended after 1.e4 e5 2.Nf3 Nc6, Bc4 an alternative",
              bm.get(p_nc6) == "Bb5" and "Bc4" in (alts.get(p_nc6) or ""),
              f"{bm.get(p_nc6)}, alts {alts.get(p_nc6)}")
        p_nd4 = h_after("e4", "e5", "Nf3", "Nc6", "Bc4", "Nd4")
        mv = pl.read_parquet(work / "course_moves.parquet")
        refuted = mv.filter((pl.col("parent_hash") == p_nd4) & (pl.col("move_san") == "Nxe5"))
        check("D: the refuted 4.Nxe5? scores 0", refuted.height == 1 and refuted["score"][0] == 0,
              f"{refuted.rows()}")
        p_e4 = h_after("e4")
        check("E: the book answers 1.e4 e5 and 1.e4 c5",
              bm.get(h_after("e4", "e5")) == "Nf3" and bm.get(h_after("e4", "c5")) == "Nf3"
              and p_e4 in bm, f"{bm.get(h_after('e4', 'e5'))}, {bm.get(h_after('e4', 'c5'))}")
        check("E: positions reachable only through the alternative are not booked",
              p_nd4 not in bm and h_after("e4", "e5", "Nf3", "Nc6", "Bb5") in bm)
        cases = [(it_, vd, ng) for it_ in ("line", "trap", "quickstarter", "model_game",
                                            "alternative", "intro") for vd in (0, 1, 2)
                 for ng in (None, "!", "!?", "?!", "?", "!! =")]
        df = pl.DataFrame({"item_type": [c[0] for c in cases], "var_depth": [c[1] for c in cases],
                           "nags": [c[2] for c in cases]})
        vec = df.select(course_books.weight_expr()).to_series().to_list()
        ref = [course_books.edge_weight(*c) for c in cases]
        check("G: the vectorised weight equals edge_weight on every case",
              vec == ref, f"{[(c, a, b) for c, a, b in zip(cases, vec, ref) if a != b][:3]}")
        com = pl.read_parquet(work / "comments.parquet")
        check("F: the intro comment is stored at the start position",
              com.filter(pl.col("text").str.contains("about 1.e4")).height == 1
              and com.filter(pl.col("text").str.contains("about 1.e4"))["position_hash"][0]
              == zobrist_int64(chess.Board()))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
