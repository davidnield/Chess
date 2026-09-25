"""Write the Rust explorer-extract's test fixtures from python-chess and the Python extract.

The Rust port is only worth having if it agrees with python-chess 1.11.2 and with
build_pooled_stats.extract_file on every input, so its fixtures are GENERATED
from them rather than written by hand. Re-run this after any change to the
Python side of the contract, then `cargo test`.

Writes to rust/explorer_extract/tests/fixtures/ (or --out):

  polyglot_keys.json  the 781 POLYGLOT_RANDOM_ARRAY keys and the start hash
  pyunicode.json      the code points str.split() splits on, and re's \\d class
  san_table.json      hand cases, every row of the spec's 2f table and more:
                      a setup (FEN + UCI moves), a token, and parse_san's verdict
                      (UCI, null, child hash + EPD) or its exception
  san_games.json      real games: each ply's parent hash and EPD, and token
                      variants (UCI, long forms, wrong suffixes, lowercase, ...)
                      with parse_san's verdict on each
  tokenizer.json      movetexts -> list(iter_san_moves(movetext))
  fuzz_regex.json     a seeded fuzz of SAN_REGEX over >= 1M tokens. The tokens
                      come from a splitmix64 generator both sides implement, so
                      only FNV-1a digests of Python's match groups (per 1,000
                      tokens) and the first 2,000 explicit results are stored,
                      keeping the fixture ~100 KB instead of tens of MB
  mini_extract.json   a mini game set and extract_file's ps/term rows for it at
                      --epd-max-ply 16 and 30: the Rust `selftest` and
                      `cargo test` hold the whole walk + aggregation to it

and rust/explorer_extract/src/pydigits.rs, Python's \\d as sorted inclusive
ranges, because Rust's own digit predicates are either ASCII-only or wider than
Unicode category Nd (and the regex crate's tables are a newer Unicode).

Usage:
    .venv/Scripts/python.exe python/gen_rust_fixtures.py [--games 40] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import tempfile
from pathlib import Path

import chess
import chess.polyglot
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import build_pooled_stats as bps
from stage1_extract_positions import iter_san_moves
from zobrist import zobrist_int64

REPO = Path(__file__).resolve().parent.parent
CRATE = REPO / "rust" / "explorer_extract"
MASK64 = (1 << 64) - 1

# ── generic ───────────────────────────────────────────────────────────────────


def dump(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(obj, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    path.write_text(txt + "\n", encoding="utf-8")
    print(f"  {path.relative_to(REPO) if path.is_relative_to(REPO) else path}: "
          f"{len(txt)/1e3:,.0f} KB")


def verdict(board: chess.Board, san: str, with_epd: bool = True) -> dict:
    """parse_san's outcome, as the Rust parser must reproduce it. A null move
    is pushed like any other: python-chess swaps the turn and drops the ep
    square, and the child hash/EPD record exactly that."""
    try:
        mv = board.parse_san(san)
    except (ValueError, AssertionError) as exc:
        return {"ok": False, "error": type(exc).__name__}
    board.push(mv)
    try:
        v = {"ok": True, "null": not mv, "uci": mv.uci(),
             "child_hash": zobrist_int64(board)}
        if with_epd:
            v["child_epd"] = board.epd()
        return v
    finally:
        board.pop()


# ── polyglot keys, whitespace and digits ──────────────────────────────────────

def gen_keys(out: Path) -> None:
    keys = list(chess.polyglot.POLYGLOT_RANDOM_ARRAY)
    assert len(keys) == 781
    dump(out / "polyglot_keys.json",
         {"keys": keys, "start_hash": zobrist_int64(chess.Board()),
          "start_epd": chess.Board().epd()})


def _ranges(cps: list[int]) -> list[list[int]]:
    out: list[list[int]] = []
    for c in cps:
        if out and out[-1][1] == c - 1:
            out[-1][1] = c
        else:
            out.append([c, c])
    return out


def gen_unicode(out: Path) -> None:
    # Exactly what the tokenizer relies on: str.split()'s separators and re's \d,
    # probed through the functions themselves rather than through isspace().
    space = [c for c in range(0x110000)
             if not 0xD800 <= c <= 0xDFFF and len(f"a{chr(c)}b".split()) == 2]
    digit_re = re.compile(r"\d")
    digits = [c for c in range(0x110000)
              if not 0xD800 <= c <= 0xDFFF and digit_re.fullmatch(chr(c))]
    import unicodedata
    dump(out / "pyunicode.json",
         {"python": sys.version.split()[0], "unicode": unicodedata.unidata_version,
          "whitespace": space, "digits": digits, "digit_ranges": _ranges(digits)})
    rs = CRATE / "src" / "pydigits.rs"
    body = ",\n".join(f"    (0x{a:04X}, 0x{b:04X})" for a, b in _ranges(digits))
    rs.parent.mkdir(parents=True, exist_ok=True)
    rs.write_text(
        "// GENERATED by python/gen_rust_fixtures.py -- do not edit.\n"
        f"// Python {sys.version.split()[0]} `re` \\d (Unicode {unicodedata.unidata_version} "
        f"category Nd): {len(digits)} code points.\n"
        "// Sorted, disjoint, inclusive ranges.\n"
        f"pub const PY_DIGIT_RANGES: [(u32, u32); {len(_ranges(digits))}] = [\n"
        f"{body},\n];\n", encoding="utf-8")
    print(f"  {rs.relative_to(REPO)}: {len(digits)} digits in "
          f"{len(_ranges(digits))} ranges; {len(space)} whitespace code points")


# ── SAN: the hand table ───────────────────────────────────────────────────────

START = chess.STARTING_FEN
# (label, fen, uci setup moves, accept, reject). Every row of the spec's 2f table
# comes first; the rest are the traps found reading parse_san and find_move.
SAN_CASES = [
    ("start", START, [],
     ["e2e4", "e2-e4", "e2xe4", "g1f3", "Ng1-f3", "Nxf3", "Nf3+", "Nf3#", "xe4",
      "--", "Z0", "0000", "@@@@", "e4", "Nf3", "N1f3", "Ngf3", "Ng1f3", "e2e3", "a3"],
     ["Pe4", "Nf3!", "e4?!", "4e4", "e5", "Ng3", "E4", "nf3", "e2e5", "g1g3",
      "Nf3=Q", "e4,e5", "O-O", "O-O-O", "0-0", "e1g1", "e1h1", "", "-", "Z", "00"]),
    ("castling", "r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1", [],
     ["O-O", "0-0", "O-O+", "0-0-0#", "e1g1", "e1h1", "O-O#", "0-0+", "0-0#",
      "O-O-O", "O-O-O+", "O-O-O#", "0-0-0", "0-0-0+", "e1c1", "e1a1", "Kf1", "Kd2"],
     ["Kg1", "Kh1", "Ke1g1", "o-o", "OO", "O-O++", "0-O", "O-0", "Kc1", "Kxh1",
      "Ke1h1", "e1g1q", "e1h1q", "O-O-O-O", "Rh1"]),
    ("castling-black", "r3k2r/8/8/8/8/8/8/R3K2R b KQkq - 0 1", [],
     ["O-O", "O-O-O", "e8g8", "e8h8", "e8c8", "e8a8", "0-0"],
     ["Kg8", "Kc8", "e1g1", "e8f8q"]),
    ("castling-in-check", "r3k2r/8/8/8/8/8/4r3/R3K2R w KQkq - 0 1", [],
     ["Kxe2", "Kf1", "Kd1"], ["O-O", "O-O-O", "e1g1", "e1h1", "e1c1", "e1a1"]),
    ("castling-through-check", "r3k2r/8/8/8/8/8/5r2/R3K2R w KQkq - 0 1", [],
     ["O-O-O", "e1c1", "e1a1"], ["O-O", "e1g1", "e1h1"]),
    ("pawn-capture", START, ["e2e4", "d7d5"],
     ["exd5", "ed5", "e4d5", "e4xd5", "e5", "exd5+"], ["xd5", "d5", "Pxd5", "dxe4"]),
    ("promotion", "1n5k/P7/8/8/8/8/8/K7 w - - 0 1", [],
     ["a8=Q", "a8Q", "a8=q", "a8q", "a7a8q", "axb8N", "ab8=Q", "a8=N", "a8=R",
      "a8=B", "a7a8=N", "a7a8Q", "axb8=Q+", "a8=Q+", "ab8q", "a7b8r"],
     ["a7a8", "a8", "a8=K", "a8=P", "a8=k", "a8P", "axb8", "a7b8", "b8=Q", "a8==Q",
      "a8=QQ"]),
    ("en-passant", START, ["e2e4", "g8f6", "e4e5", "d7d5"],
     ["exd6", "ed6", "e5d6", "exd6+", "e5xd6", "exf6"], ["xd6", "d6", "Pxd6", "exd5"]),
    ("pinned-ep", START,
     ["e2e4", "a7a6", "e4e5", "a6a5", "e1e2", "a8a6", "e2d3", "a6h6", "d3c4",
      "h6h5", "c4c5", "f7f5"],
     ["d4", "Kb5", "Kc4", "Kd4"], ["exf6", "ef6", "e5f6", "xf6"]),
    ("ambiguous", "4k3/8/8/8/8/5N2/8/1N2K3 w - - 0 1", [],
     ["Nbd2", "Nfd2", "N1d2", "N3d2", "Nb1d2", "b1d2", "f3d2", "Nh4"],
     ["Nd2", "Nxd2", "N2d2", "Nad2"]),
    ("pin-disambiguates", "4k3/4r3/8/8/8/8/4N3/1N2K3 w - - 0 1", [],
     ["Nc3", "Nbc3", "b1c3"], ["Nec3", "e2c3", "Ng3"]),
    ("black-to-move", START, ["e2e4"],
     ["e5", "e7e5", "Nf6", "g8f6", "Nc6", "--"], ["e4", "Nf3", "e2e4", "E5"]),
    ("in-check", "4k3/8/8/8/8/8/8/r3K3 w - - 0 1", [],
     ["Kd2", "Ke2", "Kf2", "--", "Z0"], ["Kd1", "Kf1", "O-O"]),
    ("find-move-pawn-default-queen", "4k3/2P5/8/8/8/8/8/4K3 w - - 0 1", [],
     ["c7c8q", "c7c8=Q", "c7c8Q", "c7c8n", "c8=Q", "c8Q"],
     ["c7c8", "c8", "c7c8k"]),
    ("black-promotion", "4k3/8/8/8/8/8/2p5/4K3 b - - 0 1", [],
     ["c1=Q+", "c1=N", "c2c1q", "c1Q"], ["c2c1", "c1", "c1=P"]),
]


def gen_san_table(out: Path) -> None:
    rows = []
    for label, fen, setup, accept, reject in SAN_CASES:
        board = chess.Board(fen)
        for u in setup:
            board.push(chess.Move.from_uci(u))
        for want_ok, toks in ((True, accept), (False, reject)):
            for tok in toks:
                v = verdict(board, tok)
                if v["ok"] != want_ok:
                    raise SystemExit(f"fixture expectation wrong: {label} {tok!r} -> {v}")
                rows.append({"label": label, "fen": fen, "moves": setup, "san": tok,
                             "parent_hash": zobrist_int64(board),
                             "parent_epd": board.epd(), "expect": v})
    dump(out / "san_table.json", {"cases": rows})


# ── SAN: real games, with variants ────────────────────────────────────────────

MAX_GAME_PLIES = 120

def _variants(board: chess.Board, mv: chess.Move, tok: str, rng) -> list[str]:
    """Tokens a Lichess dump could plausibly contain at this ply, most of them
    wrong -- the point is agreement with parse_san on every one."""
    frm, to = chess.square_name(mv.from_square), chess.square_name(mv.to_square)
    pc = board.piece_at(mv.from_square)
    letter = pc.symbol().upper() if pc and pc.piece_type != chess.PAWN else ""
    promo = chess.piece_symbol(mv.promotion) if mv.promotion else ""
    base = tok.rstrip("+#")
    legal = list(board.legal_moves)
    other = legal[rng.below(len(legal))]
    out = [tok, board.san(mv), mv.uci(), f"{letter}{frm}-{to}{promo}",
           f"{letter}{frm}x{to}", base, base + "+", base + "#", base.lower(),
           "P" + base, "x" + to, to, f"{frm}{to}", board.san(other), other.uci(),
           f"{letter}{to[0]}{to}", f"{frm[0]}{to}", "--", tok + "!"]
    if promo:
        out += [f"{base[:-1]}{promo}", f"{frm}{to}", f"{frm}{to}=k", base.replace("=", "")]
    if board.is_castling(mv):
        out += ["0-0", "O-O-O", "0-0-0+", "OO", "o-o", "e1h1", "e8h8", "e1a1", "e8a8"]
    picks = {out[0], out[1], out[2]}
    want = min(6, len(set(out)))
    while len(picks) < want:
        picks.add(out[rng.below(len(out))])
    return sorted(picks)


def gen_san_games(out: Path, n_games: int, rng) -> None:
    src = [Path("D:/data/chess/standard-chess-games-compressed") / p for p in (
        "year=2013/month=1/event=Blitz/part-0.parquet",
        "year=2017/month=6/event=Rapid/part-0.parquet",
        "year=2020/month=3/event=Bullet/part-0.parquet",
        "year=2024/month=6/event=Classical/part-0.parquet")]
    src = [p for p in src if p.exists()]
    games = []
    per = max(1, n_games // max(len(src), 1))
    for f in src:
        mts = pq.read_table(f, columns=["movetext"]).column(0).to_pylist()
        # Long games: late plies are where promotions and odd castling live.
        mts = [m for m in mts[:20_000] if m and len(list(iter_san_moves(m))) >= 80]
        for i in range(per):
            mt = mts[rng.below(len(mts))]
            board, plies = chess.Board(), []
            for tok in list(iter_san_moves(mt))[:MAX_GAME_PLIES]:
                try:
                    mv = board.parse_san(tok)
                except (ValueError, AssertionError):
                    break
                vs = {t: verdict(board, t, with_epd=False)
                      for t in _variants(board, mv, tok, rng)}
                plies.append({"token": tok, "uci": mv.uci(),
                              "parent_hash": zobrist_int64(board),
                              "parent_epd": board.epd(), "variants": vs})
                board.push(mv)
            games.append({"source": f"{f.parent.parent.parent.name}/"
                                    f"{f.parent.parent.name}/{f.parent.name}",
                          "plies": plies, "final_hash": zobrist_int64(board),
                          "final_epd": board.epd()})
    if not games:
        print("  SKIP san_games.json: no source data on D:")
        return
    dump(out / "san_games.json", {"games": games})


# ── tokenizer ─────────────────────────────────────────────────────────────────

LONG = " ".join(f"{i}. e4 e5" if i == 1 else f"{i}. Nf3 Nf6 {i}... Ng1" for i in range(1, 25))
TOKENIZER_CASES = [
    "1. e4 e5 2. Nf3 1-0",
    "",
    "   \t\n  ",
    "1-0",
    "1. e4 {comment} e5 {multi\nline} 2. Nf3 *",
    "1. e4 (1. d4 d5) e5",
    "1. e4 ((nested) x) e5",
    "1. e4 {a (b} c) e5",
    "1. e4 (a {b) c} e5",
    "1. e4 $1 e5 $23 2. Nf3 $",
    "e4 $\u0661\u0662 e5 $\uff11x",
    "\u0661. e4 \u0661\u0662... e5 \uff11\uff12. Nf3",
    "\u00b2. e4 \u2155. e5",
    "e4\u00a0e5\u1680Nf3\u2000Nc6\u2028Bb5\u3000a6\u0085Ba4\u202fNf6\u205fO-O",
    "e4\u001ce5\u001dNf3\u001eNc6\u001fBb5",
    "e4\u200be5 \ufeffNf3 \u180eNc6",
    "1-0? e4",
    "e4!! e5?? Nf3!? Nc6?! Bb5! ?! !! ??",
    "1... e5 12... Nc6 1.e4 1.. 1 . .",
    "{unclosed e4 e5",
    "e4 } e5 ) Nf3",
    "(e4 e5",
    "$ $$1 $1$2 e4$3 e5",
    "1-0 e4 0-1 e5 1/2-1/2 Nf3 * Nc6",
    "1/2 1-0-0 0-1 e4",
    "e4 e5 " * 20,
    LONG,
    "1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4 Nf6 5. O-O Be7 6. Re1 b5 7. Bb3 d6 "
    "8. c3 O-O 9. h3 Nb8 10. d4 Nbd7 11. c4 c6 12. cxb5 axb5 13. Nc3 Bb7 "
    "14. Bg5 b4 15. Nb1 h6 16. Bh4 c5 17. dxe5 Nxe4 1-0",
]


def gen_tokenizer(out: Path) -> None:
    cases = [{"movetext": mt, "tokens": list(iter_san_moves(mt))} for mt in TOKENIZER_CASES]
    dump(out / "tokenizer.json", {"cases": cases})


# ── the SAN_REGEX fuzz ────────────────────────────────────────────────────────

class SplitMix64:
    """splitmix64; the Rust test implements the same, so both sides draw the
    same token stream from the same seed."""

    def __init__(self, seed: int):
        self.s = seed & MASK64

    def next(self) -> int:
        self.s = (self.s + 0x9E3779B97F4A7C15) & MASK64
        z = self.s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        return self.next() % n


FUZZ_ALPHA = list("NBKRQPnbrqkpabcdefgh12345678x-=+#O0!?,@Z.9") + [
    "\u00e9", "\u0661", "\uff11", "\u00a0", "\u0000"]
PIECES, FILES, RANKS = "NBKRQPnk", "abcdefghi", "123456789"
SQ_RANKS, PROMOS = "0123456789", "nbrqkpNBRQKP"


def fuzz_token(r: SplitMix64) -> str:
    """One token. Mirrored step for step in the Rust test, INCLUDING the order
    in which random numbers are drawn -- keep every draw in its own statement."""
    if r.below(2) == 0:
        s: list[str] = []
        if r.below(3) == 0:
            s.append(PIECES[r.below(len(PIECES))])
        if r.below(3) == 0:
            s.append(FILES[r.below(len(FILES))])
        if r.below(3) == 0:
            s.append(RANKS[r.below(len(RANKS))])
        if r.below(3) == 0:
            s.append("-x"[r.below(2)])
        f = FILES[r.below(len(FILES))]
        k = SQ_RANKS[r.below(len(SQ_RANKS))]
        s += [f, k]
        k = r.below(6)
        if k == 0:
            s += ["=", PROMOS[r.below(len(PROMOS))]]
        elif k == 1:
            s.append(PROMOS[r.below(len(PROMOS))])
        k = r.below(4)
        if k == 0:
            s.append("+")
        elif k == 1:
            s.append("#")
        for _ in range(r.below(3)):
            op = r.below(3)
            if op == 0:
                i = r.below(len(s) + 1)
                c = FUZZ_ALPHA[r.below(len(FUZZ_ALPHA))]
                s.insert(i, c)
            elif op == 1:
                if s:
                    i = r.below(len(s))
                    del s[i]
            elif s:
                i = r.below(len(s))
                c = FUZZ_ALPHA[r.below(len(FUZZ_ALPHA))]
                s[i] = c
        return "".join(s)
    n = 1 + r.below(8)
    out = []
    for _ in range(n):
        out.append(FUZZ_ALPHA[r.below(len(FUZZ_ALPHA))])
    return "".join(out)


def fuzz_record(tok: str) -> str:
    """The canonical serialisation both sides digest: token NUL, then '0', or '1'
    and each of groups 1..5 (\\x01 for None) NUL-terminated, then newline."""
    m = chess.SAN_REGEX.match(tok)
    if not m:
        return tok + "\x000\n"
    return tok + "\x001" + "".join((g if g is not None else "\x01") + "\x00"
                                   for g in m.groups()) + "\n"


def fnv1a(data: bytes, h: int = 0xCBF29CE484222325) -> int:
    for b in data:
        h = ((h ^ b) * 0x100000001B3) & MASK64
    return h


def gen_fuzz(out: Path, n_chunks: int = 1000, per_chunk: int = 1000,
             seed: int = 0x5EED_C0DE_2026) -> None:
    r = SplitMix64(seed)
    digests, explicit, matched = [], [], 0
    for c in range(n_chunks):
        h = 0xCBF29CE484222325
        for _ in range(per_chunk):
            tok = fuzz_token(r)
            m = chess.SAN_REGEX.match(tok)
            matched += m is not None
            if len(explicit) < 2000:
                explicit.append({"tok": tok, "groups": list(m.groups()) if m else None})
            h = fnv1a(fuzz_record(tok).encode("utf-8"), h)
        digests.append(f"{h:016x}")
    dump(out / "fuzz_regex.json",
         {"seed": seed, "chunks": n_chunks, "per_chunk": per_chunk,
          "alpha": FUZZ_ALPHA, "pieces": PIECES, "files": FILES, "ranks": RANKS,
          "sq_ranks": SQ_RANKS, "promos": PROMOS, "matched": matched,
          "digests": digests, "explicit": explicit})
    print(f"    {n_chunks * per_chunk:,} tokens, {matched:,} match SAN_REGEX")


# ── the mini extract ──────────────────────────────────────────────────────────

RUY = ("e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3 Nb8 d4 "
       "Nbd7 c4 c6 cxb5 axb5 Nc3 Bb7 Bg5 b4 Nb1 h6 Bh4 c5 dxe5 Nxe4 Bxe7 Qxe7 "
       "exd6 Qf6 Nbd2 Nxd6").split()
SHUFFLE = ("Nf3 Nf6 Ng1 Ng8 " * 3).split() + RUY[:24]
PINNED = "e4 a6 e5 a5 Ke2 Ra6 Kd3 Rh6 Kc4 Rh5 Kc5 f5 d4 g5 Kb5 Nf6".split()


def _pgn(sans: list[str], result: str = "1-0") -> str:
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i // 2 + 1}.")
        out.append(s)
    return " ".join(out + [result])


# (movetext, white_score, termination, mean_elo, white_title, black_title)
MINI = [
    (_pgn(SHUFFLE), 1.0, "Normal", 1850, None, None),
    (_pgn(RUY), 0.5, "Normal", 1850, None, None),
    (_pgn(RUY[:19] + ["Qxz9"] + RUY[20:]), 0.0, "Time forfeit", 1500, None, None),
    ("1. e4 {best by test} e5 (1... c5 2. Nf3) 2. Nf3 $1 Nc6 3. Bb5!? a6?! 1-0",
     1.0, "Normal", 1650, None, None),
    ("1. e4 Nf6 2. e5 d5 3. exd6 cxd6 4. Nf3 g6 1-0", 1.0, "Normal", 1999, None, None),
    (_pgn(PINNED), 0.5, "Normal", 2000, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=Q Nb6 6. Qxb8 1-0",
     1.0, "Normal", 2600, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=N Nb6 0-1",
     0.0, "Normal", 1200, None, None),
    ("1. e4 -- 2. d4 Z0 3. Nf3 0000 4. c4 @@@@ 5. Nc3 e6 1-0", 1.0, "Normal", 1700, None, None),
    ("1. e4 e5 2. Nf3 Nc6 3. Bc4 Bc5 4. e1h1 Nf6 5. d3 O-O 1-0", 1.0, "Normal", 2100, None, None),
    ("1. e2-e4 e7e5 2. Ng1-f3 Nxc6 3. g1f3 1-0", 1.0, "Normal", 1350, None, None),
    ("1. d4 d5 2. Nc3 Nc6 3. Bf4 Bf5 4. Qd2 Qd7 5. O-O-O O-O-O 6. e3 e6 7. f3 f6 "
     "8. g4 Bg6 9. h4 h5 10. Kb1 Kb8 1/2-1/2", 0.5, "Normal", 2300, None, None),
    (None, 0.5, "Normal", 1800, None, None),
    ("", 1.0, "Normal", 1800, None, None),
    ("1-0", 1.0, "Time forfeit", 1400, None, None),
    ("*", 0.0, None, 1400, None, None),
    (_pgn(RUY[:10]), float("nan"), "Normal", 1800, None, None),
    (_pgn(RUY[:30]), 1.0, "Normal", 999, None, None),
    (_pgn(RUY[:31]), 0.0, "Normal", 1000, None, None),
    (_pgn(RUY[:6]), None, "Normal", 1800, None, None),
    (_pgn(RUY[:6]), 1.0, "Normal", None, None, None),
    (_pgn(RUY[:6]), 1.0, "Rules infraction", 1800, None, None),
    (_pgn(RUY[:6]), 1.0, "Abandoned", 1800, None, None),
    (_pgn(RUY[:6]), 1.0, "Normal", 1800, "BOT", None),
    (_pgn(RUY[:6]), 1.0, "Normal", 1800, None, "BOT"),
    (_pgn(RUY[:6]), 1.0, "Normal", 1800, "bot", "LM"),
    (_pgn(RUY[:6]), 0.25, "Normal", -5, None, None),
    (_pgn(RUY[:33]), 0.0, "Normal", 2800, None, "GM"),
    (_pgn(RUY[:12]), 1.0, "Normal", 2499, None, None),
    (_pgn(RUY[:12]), -0.0, "Normal", 2500, None, None),
]


def _ws(x) -> str | None:
    if x is None:
        return None
    return "nan" if isinstance(x, float) and math.isnan(x) else repr(float(x))


def mini_source(path: Path) -> Path:
    cols = list(zip(*MINI))
    pq.write_table(pa.table({
        "movetext": pa.array(cols[0], pa.string()),
        "white_score": pa.array(cols[1], pa.float64()),
        "termination": pa.array(cols[2], pa.string()),
        "move_count": pa.array([None] * len(MINI), pa.int16()),
        "mean_elo": pa.array(cols[3], pa.int16()),
        "white_title": pa.array(cols[4], pa.string()),
        "black_title": pa.array(cols[5], pa.string()),
        "white_elo": pa.array([None] * len(MINI), pa.int16()),
        "black_elo": pa.array([None] * len(MINI), pa.int16()),
    }), path)
    return path


PS_KEY = ["parent_hash", "move_san", "event", "elo_band"]
TERM_KEY = ["position_hash", "kind", "reason"]


def gen_mini(out: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="mini_extract_") as d:
        d = Path(d)
        src = mini_source(d / "mini.parquet")
        res = {}
        for epd in (16, 30):
            ps, tm = d / f"e{epd}.ps.parquet", d / f"e{epd}.term.parquet"
            r = bps.extract_file(src, ps, None, min_elo=0, max_ply=30, tiers=None,
                                 term_out=tm, with_child_eval=False, exclude_bots=True,
                                 excluded_terminations=frozenset({"Rules infraction",
                                                                   "Abandoned"}),
                                 event="Blitz", epd_max_ply=epd)
            res[f"ps_epd{epd}"] = pl.read_parquet(ps).sort(PS_KEY).rows()
            res[f"term_epd{epd}"] = pl.read_parquet(tm).sort(TERM_KEY).rows()
            res[f"counts_epd{epd}"] = {k: r[k] for k in ("games", "kept", "failed")} | {
                "drop": r["drop"]}
    games = [{"movetext": mt, "white_score": _ws(ws), "termination": t, "mean_elo": me,
              "white_title": wt, "black_title": bt} for mt, ws, t, me, wt, bt in MINI]
    dump(out / "mini_extract.json",
         {"event": "Blitz", "max_ply": 30, "min_elo": 0,
          "excluded_terminations": ["Abandoned", "Rules infraction"],
          "ps_columns": ["parent_hash", "move_san", "event", "elo_band", "parent_epd",
                         "child_hash", "child_eval", "ply", "white_wins", "draws",
                         "black_wins", "total"],
          "term_columns": ["position_hash", "kind", "reason", "white_wins", "draws",
                           "black_wins", "total"],
          "games": games, **res})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=CRATE / "tests" / "fixtures")
    ap.add_argument("--games", type=int, default=40,
                    help="Real games in san_games.json (skipped without D:).")
    ap.add_argument("--fuzz-chunks", type=int, default=1000,
                    help="Fuzz chunks of 1,000 tokens (default 1,000 = 1M tokens).")
    a = ap.parse_args()
    print(f"python-chess {chess.__version__}, Python {sys.version.split()[0]} -> {a.out}")
    rng = SplitMix64(20260925)
    gen_keys(a.out)
    gen_unicode(a.out)
    gen_san_table(a.out)
    gen_san_games(a.out, a.games, rng)
    gen_tokenizer(a.out)
    gen_fuzz(a.out, a.fuzz_chunks)
    gen_mini(a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
