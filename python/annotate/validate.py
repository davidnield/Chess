"""Annotation validator (Stage D).

Checks a generated annotation against its fact sheet so hallucinated moves,
invented percentages, or off-list themes are caught before the annotation is
trusted. Used by the generator's retry loop and runnable standalone.

Checks per annotation:
  1. Every SAN-shaped token in the prose/memory_rule is either in the fact
     sheet's known-SAN set, OR replays legally as a continuation from the card
     position / an engine-PV anchor (short tactical sequences the model quotes).
  2. Every "N%" cited matches a share/frequency present in the fact sheet
     (+-2.0 pp).
  3. Every returned theme token appears in the fact sheet's idea tokens.
Returns a list of error strings (empty = clean).
"""

from __future__ import annotations

import re

import chess

# Piece moves, pawn captures, castling — validated strictly (these unambiguously
# denote a move, never a square reference).
MOVE_RE = re.compile(
    r"\b(?:O-O-O|O-O|[NBRQK][a-h1-8]?x?[a-h][1-8](?:=[NBRQ])?|"
    r"[a-h]x[a-h][1-8](?:=[NBRQ])?)[+#]?")
PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _known_sans(f: dict) -> set[str]:
    out: set[str] = set()

    def add(s):
        if s:
            out.add(s.rstrip("+#"))

    add(f["our_move"]["san"])
    for c in f.get("candidates", []):
        add(c["san"])
    for r in f.get("replies", []):
        add(r["san"]); add(r.get("our_booked_response"))
    eng = f.get("engine") or {}
    for key in ("here_multipv", "after_move_multipv"):
        for line in eng.get(key) or []:
            for s in line.get("pv", []):
                add(s)
    if eng.get("threat"):
        for s in eng["threat"].get("pv", []):
            add(s)
    return out


def _board_from_line(line: str) -> chess.Board:
    b = chess.Board()
    for tok in line.replace(".", ". ").split():
        tok = re.sub(r"^\d+\.*$", "", tok)
        tok = re.sub(r"^\d+\.+", "", tok)
        if tok:
            b.push_san(tok)
    return b


def _known_pcts(f: dict) -> list[float]:
    out = []
    for r in f.get("replies", []):
        for k in ("share_pct", "gives_us_pct"):
            if r.get(k) is not None:
                out.append(float(r[k]))
    th = f.get("themes", {})
    for k in ("freq_in_ctx", "freq_global"):
        if th.get(k) is not None:
            out.append(round(float(th[k]) * 100, 1))
    return out


_PIECE_TYPE = {"N": chess.KNIGHT, "B": chess.BISHOP, "R": chess.ROOK,
               "Q": chess.QUEEN, "K": chess.KING}


def _cited_sans(text: str) -> set[str]:
    """SANs inside an explicit move-sequence citation that begins at move 1 and
    replays legally from the initial position — cross-references to other lines
    ("the Scandinavian after 1.e4 d5 2.exd5 Nf6 3.d4 Nxd5"). These are grounded
    even though they don't continue from THIS node, so they shouldn't be flagged
    as hallucinated moves. A citation with a fabricated move breaks the replay and
    stays flagged, so real hallucinations are still caught."""
    # glue-split move numbers: "1.e4" -> "1. e4", "2...Nf6" -> "2... Nf6". The
    # lookbehind keeps a SAN's own rank digit ("Nxd5.") from matching — only a
    # move counter (preceded by space/start, not a letter/digit) is split out.
    spaced = re.sub(r"(?<![A-Za-z0-9])(\d+)\.(\.\.)?", r" \1.\2 ",
                    text.replace("(", " ").replace(")", " "))
    toks = spaced.split()
    out: set[str] = set()
    i, n = 0, len(toks)
    while i < n:
        if re.fullmatch(r"1\.", toks[i]):          # citation starts at White's move 1
            board = chess.Board()
            played: list[str] = []
            j = i + 1
            while j < n:
                t = toks[j]
                if re.fullmatch(r"\d+\.(\.\.)?", t):   # bare move number — skip
                    j += 1
                    continue
                san = t.strip(".,;:!?").rstrip("+#")    # shed sentence punctuation ("Nxd5.")
                if not san:
                    j += 1
                    continue
                try:
                    board.push_san(san)
                except ValueError:
                    break                              # prose resumed; end of citation
                played.append(san)
                j += 1
            if len(played) >= 2:                       # a real line, not an incidental "1."
                out.update(played)
            i = max(j, i + 1)
        else:
            i += 1
    return out


def _is_piece_on_square(anchors: list[chess.Board], token: str) -> bool:
    """True when a piece-move token ("Qb6", "Nd4") actually names a piece sitting on
    that square in an anchor position — i.e. the prose is describing an existing
    piece by its square ("Qb6's pressure", "the Nd4 knight"), not quoting a move.
    A queen genuinely on b6 is board fact, not a hallucination."""
    body = token.rstrip("+#")
    if not body or body[0] not in _PIECE_TYPE:
        return False
    m = re.search(r"([a-h][1-8])(?:=[NBRQ])?$", body)
    if not m:
        return False
    sq = chess.parse_square(m.group(1))
    ptype = _PIECE_TYPE[body[0]]
    return any((pc := anc.piece_at(sq)) and pc.piece_type == ptype for anc in anchors)


def validate(annotation: str, memory_rule: str | None, themes: list[str],
             f: dict) -> list[str]:
    errors: list[str] = []
    text = annotation + " " + (memory_rule or "")

    known = _known_sans(f)
    # anchors from which a quoted sequence may legally continue
    anchors: list[chess.Board] = []
    try:
        base = _board_from_line(f["line"])
        anchors.append(base.copy())
        after = base.copy(); after.push_san(f["our_move"]["san"])
        anchors.append(after)
    except ValueError:
        pass

    def legal_from_anchor(s: str) -> bool:
        for anc in anchors:
            b = anc.copy()
            try:
                b.push_san(s)
                return True
            except ValueError:
                continue
        return False

    # SANs grounded by a cross-reference citation ("after 1.e4 d5 2.exd5 ...").
    cited = _cited_sans(text)

    # (1) piece moves / pawn captures / castling — strict
    for m in MOVE_RE.finditer(text):
        s = m.group(0)
        bare = s.rstrip("+#")
        if bare in known or bare in cited:
            continue
        if _is_piece_on_square(anchors, s):   # "Qb6's pressure" — a piece, not a move
            continue
        if not legal_from_anchor(s):
            errors.append(f"move '{s}' not in fact sheet and not a legal continuation")
    # (2) bare squares / pawn pushes: a lone algebraic square in prose is almost
    # always a square-NAME reference ("pressuring f2", "targets d3 and e2") — a real
    # square, not a hallucinated move — and these produced the bulk of the false
    # positives. Naming a square is inherently grounded, so bare squares are not
    # flagged; the strict hallucination check is loop (1) above, where piece moves
    # and pawn captures (the concrete tactical claims) are still validated.

    known_pcts = _known_pcts(f)
    for m in PCT_RE.finditer(text):
        v = float(m.group(1))
        if not any(abs(v - kp) <= 2.0 for kp in known_pcts):
            errors.append(f"percentage {v}% not supported by fact sheet")

    idea = {f["our_move"]["idea_token"], f.get("themes", {}).get("token")}
    idea.discard(None)
    for t in themes or []:
        if t not in idea:
            errors.append(f"theme '{t}' not among this position's idea tokens")

    return errors


def _revalidate_cli() -> None:
    """Re-check trainer_data/annotations/annotations.parquet against the fact
    sheets and rewrite the flagged/flag_reason columns. No model calls."""
    import argparse
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import polars as pl
    from trainer_app.config import resolve_data_dir

    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=None)
    args = ap.parse_args()
    ann_dir = resolve_data_dir(args.data) / "annotations"
    store_path = ann_dir / "annotations.parquet"
    df = pl.read_parquet(store_path)
    facts = {}
    for color in ("white", "black"):
        fp = ann_dir / f"facts_{color}.parquet"
        if fp.exists():
            for r in pl.read_parquet(fp).iter_rows(named=True):
                facts[r["position_hash"]] = json.loads(r["facts_json"])

    flags, reasons, nflag = [], [], 0
    for r in df.iter_rows(named=True):
        f = facts.get(r["position_hash"])
        if f is None or not r["annotation"]:
            flags.append(r["flagged"]); reasons.append(r["flag_reason"]); continue
        errs = validate(r["annotation"], r["memory_rule"], list(r["themes"] or []), f)
        flags.append(bool(errs))
        reasons.append("; ".join(errs[:3]) if errs else None)
        nflag += bool(errs)
    df = df.with_columns(pl.Series("flagged", flags),
                         pl.Series("flag_reason", reasons))
    tmp = store_path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(store_path)
    print(f"Re-validated {df.height} annotations: {nflag} flagged "
          f"({nflag/df.height*100:.1f}%)")


if __name__ == "__main__":
    _revalidate_cli()
