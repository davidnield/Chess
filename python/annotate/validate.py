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

from stage3_backwards_induction import idea_token as _idea_token

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
    for tok in (f.get("line") or "").split():             # the moves already played
        add(re.sub(r"^\d+\.+", "", tok))
    for c in f.get("candidates", []):
        add(c["san"])
    for r in f.get("replies", []):
        add(r["san"]); add(r.get("our_booked_response"))
    eng = f.get("engine") or {}
    for key in ("here_multipv", "after_move_multipv"):
        for line in eng.get(key) or []:
            for s in line.get("pv", []):
                add(s)
    for key in ("threat", "their_threat_if_we_pass"):
        for s in (eng.get(key) or {}).get("pv", []):
            add(s)
    add((f.get("engine_top") or {}).get("san"))
    plan = f.get("plan") or {}
    for s in plan.get("continuation", []):
        add(s)
    for b in plan.get("branches", []):
        for s in b.get("after", []):
            add(s)
        add(b.get("reply")); add(b.get("our_answer")); add(b.get("main_line_answer"))
    for r in plan.get("recurring_ideas", []):
        add(r["token"])
    be = f.get("book_end") or {}
    for ln in be.get("engine_lines", []):
        for s in ln.get("pv", []):
            add(s)
    for fu in be.get("what_players_did_next", []):
        add(fu["reply"])
        for x in fu["our_moves"]:
            add(x["san"])
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
    plan = f.get("plan") or {}
    out += [float(b["share_pct"]) for b in plan.get("branches", []) if b.get("share_pct") is not None]
    out += [float(r["reach_pct"]) for r in plan.get("recurring_ideas", [])]
    ru = f.get("replies_unseen") or {}
    out += [float(ru[k]) for k in ("other_pct", "ended_pct") if ru.get(k)]
    for fu in (f.get("book_end") or {}).get("what_players_did_next", []):
        out.append(float(fu["reply_share_pct"]))
        out += [float(x["share_pct"]) for x in fu["our_moves"]]
    # practical scores rendered as whole percentages (prompts._pct)
    for d in [f.get("our_move") or {}, f.get("engine_top") or {}, *f.get("candidates", [])]:
        if d.get("emp_score") is not None:
            out.append(float(round(d["emp_score"] * 100)))
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


# Present-tense balance statements only: "we are a piece up", "we're still a pawn
# down", "we are up the exchange", "we have an extra pawn", "they are a pawn up",
# "material is level". Phrasal verbs ("lines the queen up"), positions ("the rook
# behind the f-pawn") and results to come ("leaves us a piece up", "go a piece up")
# are not claims about the board after our move, and flagging them was noise
# (2026-10-26: 10 of 11 material flags in the first full run).
_UNITS = {"pawn": 1, "piece": 3, "exchange": 2, "rook": 5, "queen": 9}
_SUBJ = r"(we are|we're|we remain|we stay|we have|they are|they're|they have)\s+(?:now\s+|still\s+|already\s+|just\s+)?"
_AMT = r"(?:(?:a|an|one|two|three|\d+)\s+)?(?:the\s+)?"
MATERIAL_RE = re.compile(
    _SUBJ + _AMT + r"(pawn|piece|exchange|rook|queen)s?(?:\s+and\s+" + _AMT
    + r"(?:pawn|piece|exchange|rook|queen)s?)?\s+(up|down|ahead|behind)\b"
    r"|" + _SUBJ + r"(up|down)\s+(?:a|an|one|two|the)\s+(pawn|piece|exchange|rook|queen)\b"
    r"|" + _SUBJ + r"(?:an?\s+|two\s+)?(extra)\s+(pawn|piece|rook|queen)\b"
    r"|\b(material is (?:level|equal|even))\b", re.I)


def _material_errors(text: str, f: dict) -> list[str]:
    """A note may only claim the material balance the board facts state (the
    position after our move), when it states it as the present balance."""
    pos = f.get("position_after_our_move")
    if not pos:
        return []
    bal = pos["material"]["balance_for_us"]
    errs = []
    for m in MATERIAL_RE.finditer(text):
        g = m.groups()
        if g[9]:                                          # material is level
            ok = bal == 0
        else:
            subj = (g[0] or g[3] or g[6]).lower()
            unit = (g[1] or g[5] or g[8]).lower()
            sign = "up" if g[7] else (g[2] or g[4]).lower()
            want_up = sign in ("up", "ahead")
            ours = subj.startswith("we")
            # the balance must point the same way, and be at least about that size
            ok = (bal != 0 and ((bal > 0) == want_up) == ours
                  and abs(bal) >= _UNITS[unit] - 1)
        if not ok:
            errs.append(f"material claim '{m.group(0)}' contradicts the board "
                        f"({pos['material']['summary']})")
    return errs


ONLY_RE = re.compile(
    r"\b(?:only (?:practical |real |serious |sensible |active )?(?:try|reply|option|move|defen[cs]e)"
    r"|(?:always|invariably) (?:played|plays|answers|replies|chosen))\b", re.I)


def _only_errors(text: str, f: dict) -> list[str]:
    """'White's only practical try' needs the facts to show no alternative: no other
    listed reply, no rarer replies pooled below the floor, and at most one engine
    reply. (Our own book move being the only one we play is not such a claim, so
    the check only fires when the facts do show alternatives.)"""
    alternatives = (len(f.get("replies") or []) > 1
                    or bool((f.get("replies_unseen") or {}).get("other_pct"))
                    or len(((f.get("engine") or {}).get("after_move_multipv")) or []) > 1)
    if not alternatives:
        return []
    return [f"claims '{m.group(0)}' but the facts show other replies"
            for m in ONLY_RE.finditer(text)]


# Material claims about a later position: "11.Bxf7+ leaves us the exchange and a pawn
# up", "after 11.Qxb4 we go a piece up", "we end up a rook ahead". Checked against
# the material along the lines the fact sheet holds, replayed on a board.
_RESULT = (r"(?:leav(?:es|ing|e)|left)\s+(?:us|them)\s+(?:with\s+)?"
           r"|(?:go|goes|going|went|end|ends|ending|come|comes|coming|finish|finishes)"
           r"\s+(?:out\s+|up\s+)?")
FUTURE_RE = re.compile(
    r"(" + _RESULT + r")" + _AMT + r"(pawn|piece|exchange|rook|queen)s?(?:\s+and\s+" + _AMT
    + r"(?:pawn|piece|exchange|rook|queen)s?)?\s+(up|down|ahead|behind)\b"
    r"|(" + _RESULT + r")(?:an?\s+|two\s+)?(extra)\s+(pawn|piece|rook|queen)\b", re.I)
_VAL = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9}


def _balance(b: chess.Board, us: chess.Color) -> int:
    return sum(v * (len(b.pieces(p, us)) - len(b.pieces(p, not us))) for p, v in _VAL.items())


def _line_balances(f: dict) -> list[tuple[str, int]]:
    """(SAN just played, our material balance after it) along every line in the
    fact sheet: the plan continuation and its branches, the engine lines here and
    after our move, both null-move threat lines and the book-end lines."""
    try:
        base = _board_from_line(f["line"])
    except ValueError:
        return []
    us = base.turn
    after = base.copy()
    try:
        after.push_san(f["our_move"]["san"])
    except ValueError:
        return []
    eng = f.get("engine") or {}
    plan = f.get("plan") or {}
    starts: list[tuple[chess.Board, list[str]]] = [(base, plan.get("continuation", []))]
    for br in plan.get("branches", []):
        starts.append((base, br.get("after", []) + [br["reply"]]
                       + ([br["our_answer"]] if br.get("our_answer") else [])))
    starts += [(base, ln.get("pv", [])) for ln in eng.get("here_multipv") or []]
    starts += [(after, ln.get("pv", [])) for ln in eng.get("after_move_multipv") or []]
    starts += [(after, ln.get("pv", [])) for ln in (f.get("book_end") or {}).get("engine_lines", [])]
    for key, b0 in (("threat", after), ("their_threat_if_we_pass", base)):
        t = eng.get(key)
        if t and t.get("pv"):
            nb = b0.copy()
            nb.push(chess.Move.null())
            starts.append((nb, t["pv"]))
    out = [(f["our_move"]["san"].rstrip("+#"), _balance(after, us))]
    for b0, sans in starts:
        b = b0.copy()
        for s in sans:
            try:
                b.push_san(s)
            except ValueError:
                break
            out.append((s.rstrip("+#"), _balance(b, us)))
    return out


def _future_material_errors(text: str, f: dict) -> list[str]:
    """A claim about the material a line leads to must match the board somewhere on
    a line the facts hold: at (or the move after) the last move named before the
    claim in its sentence, or anywhere on the lines when no move is named."""
    if not FUTURE_RE.search(text):
        return []
    seq = _line_balances(f)
    if not seq:
        return []
    errs = []
    for m in FUTURE_RE.finditer(text):
        g = m.groups()
        lead, unit, sign = (g[0], g[1], g[2]) if g[1] else (g[3], g[5], "up")
        want_up = sign.lower() in ("up", "ahead")
        ours = "them" not in lead.lower()
        need = _UNITS[unit.lower()] - 1

        def fits(bal: int) -> bool:
            return bal != 0 and ((bal > 0) == want_up) == ours and abs(bal) >= need

        start = max(text.rfind(". ", 0, m.start()), text.rfind("; ", 0, m.start()), 0)
        named = [x.group(0).rstrip("+#") for x in MOVE_RE.finditer(text[start:m.start()])]
        if named:
            idx = [i for i, (s, _) in enumerate(seq) if s == named[-1]]
            # seq[0] = right after our move, the note's usual subject ("it leaves us ...")
            cands = [seq[0][1]] + [seq[j][1] for i in idx for j in (i, i + 1) if j < len(seq)]
        else:
            cands = [bal for _, bal in seq]
        if not any(fits(b) for b in cands):
            errs.append(f"material claim '{m.group(0).strip()}' matches no position on the lines "
                        f"in the facts" + (f" after {named[-1]}" if named else ""))
    return errs


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

    errors += _material_errors(text, f)
    errors += _only_errors(text, f)
    errors += _future_material_errors(text, f)

    # themes may name this move's idea or the plan's: the recurring ideas below and
    # the idea tokens of our moves in the continuation
    plan = f.get("plan") or {}
    idea = {f["our_move"]["idea_token"], f.get("themes", {}).get("token")}
    idea |= {r["token"] for r in plan.get("recurring_ideas", [])}
    idea |= {_idea_token(s) for s in plan.get("continuation", [])[::2] if s}
    idea.discard(None)
    for t in themes or []:
        if t not in idea:
            errors.append(f"theme '{t}' not among this position's idea tokens")

    return errors


def validate_chapter(text: str, facts_list: list[dict]) -> list[str]:
    """A chapter intro may use any move grounded in any of its chunk's fact sheets
    (or legal from one of their positions) and any of their percentages."""
    known: set[str] = set()
    pcts: list[float] = []
    anchors: list[chess.Board] = []
    for f in facts_list:
        known |= _known_sans(f)
        pcts += _known_pcts(f)
        try:
            b = _board_from_line(f["line"])
            anchors.append(b.copy())
            b.push_san(f["our_move"]["san"])
            anchors.append(b)
        except ValueError:
            pass
    cited = _cited_sans(text)
    errors = []
    for m in MOVE_RE.finditer(text):
        s = m.group(0)
        bare = s.rstrip("+#")
        if bare in known or bare in cited or _is_piece_on_square(anchors, s):
            continue
        ok = False
        for anc in anchors:
            try:
                anc.copy().push_san(s)
                ok = True
                break
            except ValueError:
                continue
        if not ok:
            errors.append(f"chapter: move '{s}' not in the fact sheets")
    for m in PCT_RE.finditer(text):
        if not any(abs(float(m.group(1)) - kp) <= 2.0 for kp in pcts):
            errors.append(f"chapter: percentage {m.group(1)}% not supported")
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
