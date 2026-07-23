"""Prompt construction for annotation generation.

Renders each chunk's fact sheets into a compact, grounded prompt for a single
`claude -p` invocation. The model may ONLY phrase facts that appear here — the
validator enforces it afterwards. Positions carry short ids (p1, p2, ...) so
the model never has to echo 19-digit position hashes.

Bump PROMPT_VERSION whenever the system text or rendering changes materially —
it is stored per annotation and forces regeneration of stale-version rows.
"""

from __future__ import annotations

import json

PROMPT_VERSION = "v1"

SYSTEM = """\
You are writing study annotations for a personal chess opening repertoire, in \
the style of a high-quality Chessable course author. Your reader is an adult \
club player learning THIS repertoire and wants to UNDERSTAND the positions, not \
just memorize moves.

For each position you are given a FACT SHEET containing everything true about \
it that you may use. Write a short annotation (2-4 sentences) that explains:
- the plan or idea behind our move (what it fights for, what it threatens);
- if relevant, how the opponent typically goes wrong here and what that gives us;
- how this idea connects to other lines in the repertoire (only via the theme \
data provided).
Also write, when it applies, ONE memory rule in the form "If they <do/omit X>, \
we <do Y>" — a concrete trigger->response the reader can recall at the board. \
If no crisp rule fits, set it to null.

STRICT GROUNDING RULES — you will be automatically checked against these:
- Only mention moves that appear in the fact sheet (our move, listed candidates, \
listed opponent replies, our booked responses, or moves inside the provided \
engine lines). NEVER invent a move or a variation.
- Only cite numbers (percentages) that appear in the fact sheet, rounded as given.
- Describe evaluations in words using the provided assessments \
("slightly better", "winning", etc.). NEVER cite raw centipawn or decimal \
evaluation numbers.
- Draw thematic connections ONLY from the provided theme data; do not assert \
connections that are not supported there.
- If the fact sheet lists past mistakes the reader has made here, you may gently \
flag the correct move as a personal reminder.
- Be concrete and instructive; avoid empty phrases like "this is a good move".

Return ONLY a JSON object, no prose around it, of the exact form:
{"annotations": [{"id": "p1", "annotation": "...", "memory_rule": "..." or null, \
"themes": ["idea-token", ...]}, ...]}
Include every position id given, in order. themes must be drawn from the \
idea tokens shown in each position's fact sheet.
"""


def _assess_phrase(a: str | None) -> str:
    return a or "unclear"


def render_position(pid: str, f: dict) -> str:
    """Compact human-readable rendering of one fact sheet for the model."""
    om = f["our_move"]
    L = [f"### {pid}  (after {f['line']})   we play: {om['san']}"]
    L.append(f"- Our move {om['san']}: {_assess_phrase(om['assessment'])} for us"
             + (f", played in {om['games']:,} games" if om.get("games") else "")
             + f". Idea token: {om['idea_token']}.")
    if f.get("transpositions"):
        L.append(f"- Also reached via: {'; '.join(f['transpositions'])}.")

    cands = [c for c in f["candidates"] if c["san"] != om["san"]][:4]
    if cands:
        parts = []
        for c in cands:
            d = c.get("delta_es_vs_chosen")
            rel = ("similar" if d is None or abs(d) < 0.02
                   else "better" if d > 0 else "worse")
            parts.append(f"{c['san']} ({_assess_phrase(c['assessment'])}, {rel})")
        L.append("- Alternatives we did NOT pick: " + ", ".join(parts) + ".")

    if f.get("replies"):
        rp = []
        for r in f["replies"][:6]:
            piece = f"{r['san']} ({r['share_pct']}%)"
            if r.get("our_booked_response"):
                piece += f" -> we answer {r['our_booked_response']}"
            conc = r.get("gives_us_pct")
            if conc is not None and conc >= 3.0:
                piece += f", a slip that leaves us {_assess_phrase(r['assessment'])}"
            rp.append(piece)
        L.append("- Opponent replies (by frequency): " + "; ".join(rp) + ".")

    nm = f.get("node_metrics", {})
    if nm.get("opponent_error") is not None and nm["opponent_error"] >= 0.03:
        L.append(f"- Opponents err noticeably often here (opponent-error index "
                 f"{nm['opponent_error']}).")

    eng = f.get("engine")
    if eng:
        if eng.get("threat") and eng["threat"].get("pv"):
            L.append(f"- If the opponent does nothing, our threat runs: "
                     f"{' '.join(eng['threat']['pv'][:5])}.")
        if eng.get("here_multipv"):
            top = eng["here_multipv"][0]
            if top.get("pv"):
                L.append(f"- Main engine line here: {' '.join(top['pv'][:6])}.")

    th = f.get("themes", {})
    if th.get("other_positions_same_idea"):
        extra = (f" e.g. {th['sample_lines_same_idea'][0]}"
                 if th.get("sample_lines_same_idea") else "")
        L.append(f"- Theme '{th['token']}' recurs in {th['other_positions_same_idea']} "
                 f"other repertoire position(s).{extra}")

    if f.get("past_mistakes"):
        pm = ", ".join(f"{m['san']} ({m['times']}x)" for m in f["past_mistakes"])
        L.append(f"- YOU have gone wrong here before, playing: {pm}. The book move "
                 f"is {om['san']}.")
    return "\n".join(L)


def build_chunk_prompt(color: str, chapter_line: str,
                       positions: list[tuple[str, dict]]) -> str:
    """positions: list of (pid, facts_dict) in order."""
    header = (f"REPERTOIRE: {color.capitalize()}. "
              f"CHAPTER LINE: {chapter_line or '(from the start)'}.\n"
              f"Annotate the following {len(positions)} positions from this "
              f"variation, in order.\n\n")
    body = "\n\n".join(render_position(pid, f) for pid, f in positions)
    reminder = ("\n\nReturn ONLY the JSON object described in the system "
                "instructions, covering every id above.")
    return SYSTEM + "\n\n" + header + body + reminder
