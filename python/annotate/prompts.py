"""Prompt construction for annotation generation.

Renders each chunk's fact sheets into a compact, grounded prompt for a single
`claude -p` invocation. The model may ONLY phrase facts that appear here — the
validator enforces it afterwards. Positions carry short ids (p1, p2, ...) so
the model never has to echo 19-digit position hashes.

v2 (2026-10-26) is plan-first: each note explains what our move deals with and
threatens (Stage B's null-move analyses), the plan our book follows next
(annotate.plan), where that plan changes, and why our move when the engine
prefers another; each chunk also gets a chapter intro.

Bump PROMPT_VERSION whenever the system text or rendering changes materially —
it is stored per annotation and forces regeneration of stale-version rows.
"""

from __future__ import annotations

PROMPT_VERSION = "v2"

SYSTEM = """\
You are writing study notes for a personal chess opening repertoire. The reader \
is a club player (about 1800 online) learning THIS repertoire move by move in a \
trainer. They see your note when a move is first taught and after they answer \
it, so each note must make the move feel like the natural continuation of a \
plan, not a fact to memorise. Our repertoire often leaves mainstream theory for \
practical reasons, so explain OUR plan, not general theory.

For each position you get a FACT SHEET. Write a note of 2-4 short sentences \
that covers, in this order, whatever the facts support:
1. What our move does: what it attacks, prepares or prevents. Use "if we did \
nothing, they would ..." and "after our move we threaten ..." facts when they \
are marked as real threats.
2. The plan: where we are heading over the next moves (the continuation and the \
recurring ideas), stated as goals ("we castle and bring the rook to e1"), not as \
a list of moves.
3. The key exception, if there is one: "If they play X instead, we switch to Y" \
(only from the listed branches where the plan changes).
4. Only if the engine's first choice differs from ours: one clause on why ours \
is the practical choice, using the given practical scores.
Also write ONE memory rule when a crisp one exists, in the form "If they <X>, we \
<Y>", else null.

Then write a CHAPTER INTRO for the whole variation (3-5 sentences): our setup \
and recurring plans across these positions, the opponent's main tries, and the \
one thing to watch out for. It is shown when the reader starts learning the \
variation.

STRICT GROUNDING RULES — you will be automatically checked against these:
- Only mention moves that appear in the fact sheets (our move, candidates, \
replies, booked answers, plan continuation, branches, recurring ideas, threat \
lines, engine lines). NEVER invent a move or a variation.
- Course commentary is background on the IDEAS behind our line: paraphrase its \
ideas in your own words, never quote it, and do not cite moves from it unless \
they also appear elsewhere in the fact sheet.
- Only cite percentages that appear in the fact sheet, rounded as given.
- Describe evaluations in words ("slightly better", "winning"); NEVER give \
centipawns or decimal evaluations.
- Use move numbers as given in the fact sheet's lines (e.g. "6.exd5", "6...Na5").
- Be concrete; avoid empty phrases like "this is a good move" or "clearly best".

Return ONLY a JSON object, no prose around it, of the exact form:
{"chapter": "...", "annotations": [{"id": "p1", "annotation": "...", \
"memory_rule": "..." or null, "themes": ["idea-token", ...]}, ...]}
Include every position id given, in order. themes must be drawn from the idea \
tokens shown in each position's fact sheet.
"""


def _assess_phrase(a: str | None) -> str:
    return a or "unclear"


def _ply_of(line: str) -> int:
    """Plies played before the position (line is '1.e4 e5 2.Nf3 ...')."""
    return sum(1 for t in line.split() if not t.endswith(".") and t)


def numbered(sans: list[str], ply0: int) -> str:
    """SANs from ply index ply0 (0 = White's first move) with move numbers."""
    out = []
    for i, s in enumerate(sans):
        p = ply0 + i
        if p % 2 == 0:
            out.append(f"{p // 2 + 1}.{s}")
        elif i == 0:
            out.append(f"{p // 2 + 1}...{s}")
        else:
            out.append(s)
    return " ".join(out)


def _pct(x: float | None) -> str | None:
    return None if x is None else f"{round(x * 100)}%"


def render_position(pid: str, f: dict) -> str:
    """Compact human-readable rendering of one fact sheet for the model."""
    om = f["our_move"]
    ply0 = _ply_of(f["line"])
    L = [f"### {pid}  (after {f['line'] or 'the start'})   we play: {numbered([om['san']], ply0)}"]
    L.append(f"- Our move {om['san']}: {_assess_phrase(om['assessment'])} for us"
             + (f", scoring {_pct(om['emp_score'])} in {om['games']:,} games"
                if om.get("games") and om.get("emp_score") is not None else "")
             + f". Idea token: {om['idea_token']}.")
    if f.get("transpositions"):
        L.append(f"- Also reached via: {'; '.join(f['transpositions'])}.")

    eng = f.get("engine") or {}
    th = eng.get("their_threat_if_we_pass")
    if th and th.get("real"):
        what = f" ({', '.join(th['what'])})" if th.get("what") else ""
        L.append(f"- If we did nothing, they would play {th['move']}{what}, leaving us "
                 f"{_assess_phrase(th['assessment_if_free_move'])}: "
                 f"{numbered(th['pv'][:4], ply0 + 1)}.")
    ours = eng.get("threat")
    if ours and ours.get("real"):
        what = f" ({', '.join(ours['what'])})" if ours.get("what") else ""
        L.append(f"- After {om['san']} we threaten {ours['move']}{what}; if they ignored it "
                 f"we would be {_assess_phrase(ours['assessment_if_free_move'])}: "
                 f"{numbered(ours['pv'][:4], ply0 + 2)}.")

    et = f.get("engine_top")
    if et:
        prac = (f"; in practice {et['san']} scores {_pct(et['emp_score'])} over {et['games']:,} games"
                if et.get("games") and et.get("emp_score") is not None else
                "; it is rarely played in practice")
        L.append(f"- The engine's first choice is {et['san']} ({_assess_phrase(et['assessment'])} "
                 f"for us){prac}, against {_pct(om['emp_score'])} for our {om['san']}.")

    plan = f.get("plan")
    if plan and plan.get("continuation"):
        L.append(f"- Our plan if play goes the usual way: {numbered(plan['continuation'], ply0)}.")
        for b in plan.get("branches", []):
            if not b.get("plan_changes") or not b.get("our_answer"):
                continue
            k = len(b["after"])
            L.append(f"  - Plan change: after {numbered(b['after'], ply0)}, if "
                     f"{numbered([b['reply']], ply0 + k)} ({b['share_pct']}%), we play "
                     f"{numbered([b['our_answer']], ply0 + k + 1)} instead of "
                     f"{b['main_line_answer']}.")
        if plan.get("recurring_ideas"):
            L.append("- Ideas that recur across this variation (share of the lines below): "
                     + ", ".join(f"{r['token']} ({r['reach_pct']}%)" for r in plan["recurring_ideas"])
                     + ".")

    cands = [c for c in f["candidates"] if c["san"] != om["san"]][:4]
    if cands:
        parts = []
        for c in cands:
            d = c.get("delta_es_vs_chosen")
            rel = ("similar" if d is None or abs(d) < 0.02
                   else "better" if d > 0 else "worse")
            parts.append(f"{c['san']} ({_assess_phrase(c['assessment'])}, {rel} by the engine)")
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
        L.append("- Their replies to our move (by frequency): " + "; ".join(rp) + ".")

    for n in f.get("course_notes") or []:
        L.append(f"- Course commentary on {om['san']} (from a course that follows our line "
                 f"for {n['follows_our_line_plies']} plies; paraphrase ideas only): "
                 f"\"{n['text']}\"")

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
              f"variation, in order, and write the chapter intro.\n\n")
    body = "\n\n".join(render_position(pid, f) for pid, f in positions)
    reminder = ("\n\nReturn ONLY the JSON object described in the system "
                "instructions: the chapter intro and every id above.")
    return SYSTEM + "\n\n" + header + body + reminder
