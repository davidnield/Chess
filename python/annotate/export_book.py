"""
Export annotations as a self-contained HTML "book" — one chapter per chunk,
each position shown as a wooden board diagram (Lichess colours) with:
  - the opponent's last move tinted olive (how we reached the position),
  - our move to play drawn as an orange arrow + ring,
  - the course note + memory-rule callout,
  - an expandable "facts behind this note" grounding panel (candidate moves
    with expected-score, opponent replies with our booked answer).

This is the review surface for generated annotations. It renders board
diagrams that export_course.py (markdown) cannot. The board geometry —
FENs, our from/to squares, and the opponent's last-move squares — is derived
deterministically here with python-chess from each card's line + best move.

Only chapters with at least one non-empty, non-flagged annotation are emitted
(pass --include-flagged to show flagged ones too, marked).

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.export_book
        [--data DIR] [--color white|black] [--out PATH] [--include-flagged]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import chess
import polars as pl

from annotate.chunks import build_chunks
from trainer_app.config import resolve_data_dir
from trainer_app.pack import TrainingPack

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

START_FEN = chess.STARTING_FEN.split()[0]


def _sans(line: str) -> list[str]:
    """Bare SANs from a numbered line string ('1.e4 e5 2.Nf3 ...')."""
    return [t.split(".")[-1] for t in line.split() if t.split(".")[-1]]


def _geometry(line: str, our_san: str) -> dict:
    """Board FEN + our move from/to + opponent's last-move from/to for a card.
    `line` is the numbered move sequence up to (and including) the opponent's
    last move; `our_san` is the move we play from that position."""
    sans = _sans(line)
    board = chess.Board()
    opp_from = opp_to = None
    for i, san in enumerate(sans):
        if i == len(sans) - 1:                 # the opponent's last move
            mv = board.parse_san(san)
            opp_from = chess.square_name(mv.from_square)
            opp_to = chess.square_name(mv.to_square)
        board.push_san(san)
    fen = board.fen().split()[0]
    our_from = our_to = None
    try:
        omv = board.parse_san(our_san)
        our_from = chess.square_name(omv.from_square)
        our_to = chess.square_name(omv.to_square)
    except ValueError:
        pass
    return {"fen": fen, "from": our_from, "to": our_to,
            "oppFrom": opp_from, "oppTo": opp_to}


def _card(facts: dict) -> dict | None:
    """Build one render-card dict from a fact sheet (or None if unusable)."""
    om = facts.get("our_move") or {}
    our_san = om.get("san")
    line = facts.get("line", "")
    if not our_san:
        return None
    geo = _geometry(line, our_san)
    if not geo["to"]:
        return None
    cands = facts.get("candidates") or []
    verdict, win = "", False
    for c in cands:
        if c.get("san") == our_san:
            verdict = c.get("assessment", "")
            win = verdict == "winning"
            break
    cand_rows = [[c.get("san"), c.get("games") or 0, c.get("es") or 0.0,
                  c.get("assessment", ""), c.get("delta_es_vs_chosen") or 0.0,
                  c.get("san") == our_san, c.get("source", "")]
                 for c in cands]
    reply_rows = [[r.get("san"), r.get("share_pct") or 0.0, r.get("games") or 0,
                   r.get("es_after") or 0.0, r.get("assessment", ""),
                   r.get("gives_us_pct") or 0.0, r.get("our_booked_response")]
                  for r in (facts.get("replies") or [])]
    return {"line": line, "move": our_san, **geo,
            "verdict": verdict or "playable", "win": win,
            "reach_pct": facts.get("reach_pct", 0.0),  # already a percentage (0-100)
            "candidates": cand_rows, "replies": reply_rows}


def build(data_dir: Path, color: str, include_flagged: bool) -> tuple[list, dict]:
    """Return (chapters, stats). chapters = [{title, tabiya, cards:[...]}]."""
    ann_dir = data_dir / "annotations"
    store = {(r["color"], r["position_hash"]): r
             for r in pl.read_parquet(ann_dir / "annotations.parquet").iter_rows(named=True)}
    facts = {r["position_hash"]: json.loads(r["facts_json"])
             for r in pl.read_parquet(ann_dir / f"facts_{color}.parquet").iter_rows(named=True)}
    pack = TrainingPack(data_dir / "pack")
    chunks = build_chunks(pack, color)

    def chapter_reach(ch):
        return facts.get(ch.card_hashes[0], {}).get("reach_pct", 0.0)

    chapters, n_cards, n_flagged = [], 0, 0
    for ch in sorted(chunks, key=lambda c: -chapter_reach(c)):
        cards = []
        for h in ch.card_hashes:
            a = store.get((color, h))
            f = facts.get(h)
            if f is None or a is None or not a["annotation"]:
                continue
            if a["flagged"] and not include_flagged:
                continue
            card = _card(f)
            if card is None:
                continue
            card["annotation"] = a["annotation"]
            card["memory"] = a["memory_rule"]
            card["flagged"] = bool(a["flagged"])
            cards.append(card)
            n_cards += 1
            n_flagged += bool(a["flagged"])
        if not cards:
            continue
        title = " ".join((f"{j//2+1}.{s}" if j % 2 == 0 else s)
                         for j, s in enumerate(ch.chapter_sans)) or "Starting position"
        chapters.append({"title": title, "cards": cards})
    return chapters, {"chapters": len(chapters), "cards": n_cards, "flagged": n_flagged}


# ── page template (CSS + JS reused verbatim from the reviewed pilot page) ────

CSS = r"""
  :root {
    --paper:#f2f3ef; --panel:#fbfbf8; --ink:#1a201e; --soft-ink:#3c4642;
    --muted:#6a726c; --line:#dcded6; --accent:#2f5d50; --accent-soft:#e2ece7;
    --gold:#8a6d16; --gold-soft:#f3ebd3; --rust:#a2472f; --rust-soft:#f3e2db;
    --sq-light:#f0d9b5; --sq-dark:#b58863; --piece-w:#f8f6ef; --piece-k:#26211c;
    --hl:#d0700f; --arrow:rgba(208,112,15,.92); --last:rgba(155,199,0,.47);
    --serif:"Iowan Old Style","Palatino Linotype",Palatino,Georgia,"Times New Roman",serif;
    --sans:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
    --mono:ui-monospace,"SF Mono","Cascadia Mono","Consolas",monospace;
  }
  @media (prefers-color-scheme:dark){
    :root{
      --paper:#141816; --panel:#1c211e; --ink:#e7e9e3; --soft-ink:#c4c9c2;
      --muted:#8b938c; --line:#2c332e; --accent:#69ad98; --accent-soft:#1e2b26;
      --gold:#cba93f; --gold-soft:#2a2617; --rust:#d5795e; --rust-soft:#2c1d18;
      --sq-light:#e4c79b; --sq-dark:#a3743f; --piece-w:#f6f3ea; --piece-k:#1c1712;
      --hl:#e6841a; --arrow:rgba(232,140,40,.95); --last:rgba(165,205,25,.42);
    }
  }
  :root[data-theme="light"]{
    --paper:#f2f3ef; --panel:#fbfbf8; --ink:#1a201e; --soft-ink:#3c4642;
    --muted:#6a726c; --line:#dcded6; --accent:#2f5d50; --accent-soft:#e2ece7;
    --gold:#8a6d16; --gold-soft:#f3ebd3; --rust:#a2472f; --rust-soft:#f3e2db;
    --sq-light:#f0d9b5; --sq-dark:#b58863; --piece-w:#f8f6ef; --piece-k:#26211c; --hl:#d0700f;
    --arrow:rgba(208,112,15,.92); --last:rgba(155,199,0,.47);
  }
  :root[data-theme="dark"]{
    --paper:#141816; --panel:#1c211e; --ink:#e7e9e3; --soft-ink:#c4c9c2;
    --muted:#8b938c; --line:#2c332e; --accent:#69ad98; --accent-soft:#1e2b26;
    --gold:#cba93f; --gold-soft:#2a2617; --rust:#d5795e; --rust-soft:#2c1d18;
    --sq-light:#e4c79b; --sq-dark:#a3743f; --piece-w:#f6f3ea; --piece-k:#1c1712; --hl:#e6841a;
    --arrow:rgba(232,140,40,.95); --last:rgba(165,205,25,.42);
  }

  *{box-sizing:border-box;}
  body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--serif);
    line-height:1.62;-webkit-font-smoothing:antialiased;}
  .wrap{max-width:720px;margin:0 auto;padding:clamp(1.5rem,4vw,3.5rem) 1.25rem 5rem;}

  .eyebrow{font-family:var(--sans);text-transform:uppercase;letter-spacing:.16em;
    font-size:.7rem;font-weight:600;color:var(--muted);}
  h1{font-size:clamp(1.9rem,5vw,2.7rem);line-height:1.12;margin:.5rem 0 .3rem;
    font-weight:600;text-wrap:balance;letter-spacing:-.01em;}
  .lede{color:var(--soft-ink);font-size:1.08rem;margin:1.1rem 0 0;}
  .meta-row{font-family:var(--sans);font-size:.8rem;color:var(--muted);
    margin-top:1.1rem;display:flex;flex-wrap:wrap;gap:.4rem .9rem;}
  .meta-row b{color:var(--soft-ink);font-weight:600;}

  .note{font-family:var(--sans);font-size:.83rem;line-height:1.55;color:var(--soft-ink);
    background:var(--accent-soft);border:1px solid var(--line);border-radius:8px;
    padding:.85rem 1rem;margin:1.6rem 0 0;}
  .note b{color:var(--ink);}
  .legend{display:flex;flex-wrap:wrap;gap:.35rem 1.2rem;margin:.9rem 0 0;
    font-family:var(--sans);font-size:.78rem;color:var(--muted);}
  .legend span{display:inline-flex;align-items:center;gap:.45rem;}
  .sw{width:1.05rem;height:1.05rem;border-radius:3px;flex:0 0 auto;
    background:var(--sq-light);position:relative;}
  .sw.last{background:var(--sq-light);
    background-image:linear-gradient(var(--last),var(--last));
    box-shadow:inset 0 0 0 1px rgba(0,0,0,.15);}
  .sw.hl{box-shadow:inset 0 0 0 3px var(--hl);}
  .sw.arw{background:none;}
  .sw.arw::after{content:"";position:absolute;left:2px;top:50%;width:11px;height:0;
    border-top:2px solid var(--arrow);}
  .sw.arw::before{content:"";position:absolute;right:1px;top:50%;
    border-left:5px solid var(--arrow);border-top:3px solid transparent;
    border-bottom:3px solid transparent;transform:translateY(-50%);}

  .rule{border:none;border-top:1px solid var(--line);margin:2.4rem 0;}

  .chapter{font-size:1.35rem;font-weight:600;margin:3.2rem 0 0;letter-spacing:-.01em;
    padding-top:1.6rem;border-top:2px solid var(--line);text-wrap:balance;}
  .chapter .cnum{font-family:var(--sans);font-size:.72rem;font-weight:700;
    letter-spacing:.14em;color:var(--accent);display:block;margin-bottom:.25rem;}

  .card{margin:2.2rem 0;}
  .card .idx{font-family:var(--sans);font-size:.72rem;letter-spacing:.14em;
    text-transform:uppercase;color:var(--muted);font-weight:600;}
  .card .path{font-family:var(--mono);font-size:.82rem;color:var(--muted);
    margin:.15rem 0 1rem;overflow-x:auto;white-space:nowrap;padding-bottom:.15rem;}
  .card .path b{color:var(--soft-ink);}

  .diagram{display:flex;gap:clamp(1rem,3vw,1.6rem);align-items:flex-start;flex-wrap:wrap;}
  .board{--sz:min(280px,72vw);width:var(--sz);height:var(--sz);flex:0 0 auto;
    position:relative;
    display:grid;grid-template-columns:repeat(8,1fr);grid-template-rows:repeat(8,1fr);
    border:1px solid var(--ink);border-radius:4px;overflow:hidden;
    box-shadow:0 1px 0 var(--line),0 6px 18px rgba(0,0,0,.10);}
  .board .arrows{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:2;}
  .sq{display:flex;align-items:center;justify-content:center;
    font-size:calc(var(--sz)/9.2);line-height:1;position:relative;}
  .sq.l{background:var(--sq-light);} .sq.d{background:var(--sq-dark);}
  .sq.last::before{content:"";position:absolute;inset:0;background:var(--last);z-index:0;}
  .sq.hl::after{content:"";position:absolute;inset:0;z-index:1;
    box-shadow:inset 0 0 0 3px var(--hl);}
  .pc{position:relative;z-index:1;paint-order:stroke fill;}
  .pc.w{color:var(--piece-w);-webkit-text-stroke:1.3px #2c2620;
    text-shadow:0 1px 1px rgba(0,0,0,.30);}
  .pc.k{color:var(--piece-k);-webkit-text-stroke:0.7px #000;
    text-shadow:0 0 2px rgba(245,240,225,.35);}

  .decide{flex:1 1 220px;min-width:210px;}
  .decide .lbl{font-family:var(--sans);font-size:.72rem;letter-spacing:.12em;
    text-transform:uppercase;color:var(--muted);font-weight:600;}
  .move{font-size:2rem;font-weight:600;margin:.1rem 0 .1rem;letter-spacing:.01em;}
  .move .cap{color:var(--rust);}
  .verdict{display:inline-block;font-family:var(--sans);font-size:.74rem;font-weight:600;
    letter-spacing:.03em;padding:.2rem .55rem;border-radius:999px;
    background:var(--accent-soft);color:var(--accent);border:1px solid var(--line);}
  .verdict.win{background:var(--gold-soft);color:var(--gold);}
  .flag{display:inline-block;font-family:var(--sans);font-size:.72rem;color:var(--rust);
    margin-left:.5rem;}
  .reachline{font-family:var(--sans);font-size:.78rem;color:var(--muted);margin-top:.55rem;}

  .prose{margin:1.1rem 0 0;font-size:1.06rem;}
  .memory{display:flex;gap:.6rem;align-items:flex-start;margin:1.05rem 0 0;
    background:var(--gold-soft);border:1px solid var(--line);
    border-left:3px solid var(--gold);border-radius:0 7px 7px 0;padding:.7rem .85rem;}
  .memory .k{font-size:1rem;flex:0 0 auto;filter:saturate(.8);}
  .memory .t{font-family:var(--sans);font-size:.92rem;line-height:1.5;color:var(--soft-ink);}
  .memory .t b{color:var(--ink);}

  details.ground{margin:1rem 0 0;border-top:1px dashed var(--line);}
  details.ground>summary{font-family:var(--sans);font-size:.78rem;font-weight:600;
    letter-spacing:.03em;color:var(--muted);cursor:pointer;list-style:none;
    padding:.6rem 0 .2rem;display:flex;align-items:center;gap:.4rem;}
  details.ground>summary::-webkit-details-marker{display:none;}
  details.ground>summary .chev{transition:transform .15s;}
  details.ground[open]>summary .chev{transform:rotate(90deg);}
  .g-body{font-family:var(--sans);font-size:.82rem;padding:.3rem 0 .4rem;}
  .g-h{font-size:.7rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);
    font-weight:700;margin:.6rem 0 .35rem;}
  table.g{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;}
  table.g th{text-align:right;font-size:.68rem;text-transform:uppercase;letter-spacing:.05em;
    color:var(--muted);font-weight:600;padding:.25rem .5rem;border-bottom:1px solid var(--line);}
  table.g th.l,table.g td.l{text-align:left;}
  table.g td{padding:.28rem .5rem;border-bottom:1px solid var(--line);color:var(--soft-ink);}
  table.g td.mv{font-family:var(--mono);color:var(--ink);font-weight:600;}
  table.g tr.chosen td{background:var(--accent-soft);}
  table.g tr.chosen td.mv{color:var(--accent);}
  .bar{display:inline-block;height:.5rem;border-radius:2px;background:var(--accent);
    vertical-align:middle;opacity:.65;}
  .tag{font-size:.66rem;padding:.05rem .4rem;border-radius:4px;background:var(--panel);
    border:1px solid var(--line);color:var(--muted);}
  .es-good{color:var(--accent);font-weight:600;} .es-win{color:var(--gold);font-weight:600;}
  .booked{font-family:var(--mono);color:var(--accent);font-weight:600;}
  .unbooked{color:var(--muted);font-style:italic;}

  footer{font-family:var(--sans);font-size:.8rem;color:var(--muted);
    border-top:1px solid var(--line);margin-top:3rem;padding-top:1.2rem;line-height:1.6;}
"""

JS_FUNCS = r"""
const GLYPH={p:"♟",n:"♞",b:"♝",r:"♜",q:"♛",k:"♚"};
function sqName(f,r){return "abcdefgh"[f]+(8-r);}
function sqXY(sq){
  let f=sq.charCodeAt(0)-97, r=8-(+sq[1]);
  if(FLIP){f=7-f; r=7-r;}                    // black repertoire → board flips
  return [f+0.5,r+0.5];
}
function arrow(from,to){
  if(!from||!to) return "";
  const [x1,y1]=sqXY(from), [x2,y2]=sqXY(to);
  const dx=x2-x1, dy=y2-y1, L=Math.hypot(dx,dy)||1;
  const ux=dx/L, uy=dy/L, px=-uy, py=ux;
  const head=0.46, halfW=0.30, shaftW=0.17;
  const tx=x2-ux*0.05, ty=y2-uy*0.05;
  const bx=tx-ux*head, by=ty-uy*head;
  const sx=x1+ux*0.32, sy=y1+uy*0.32;
  const poly=`${tx},${ty} ${bx+px*halfW},${by+py*halfW} ${bx-px*halfW},${by-py*halfW}`;
  return `<svg class="arrows" viewBox="0 0 8 8" preserveAspectRatio="none" aria-hidden="true">`
    +`<line x1="${sx}" y1="${sy}" x2="${bx}" y2="${by}" stroke="var(--arrow)" `
    +`stroke-width="${shaftW}" stroke-linecap="round"/>`
    +`<polygon points="${poly}" fill="var(--arrow)"/></svg>`;
}
function board(fen,from,to,oppFrom,oppTo){
  const rows=fen.split(" ")[0].split("/");   // FEN ranks 8..1
  const grid=[];                             // grid[r][f]: r=0 is rank 8, f=0 is file a
  for(let r=0;r<8;r++){
    const line=[]; let f=0;
    for(const ch of rows[r]){
      if(/\d/.test(ch)){for(let k=0;k<+ch;k++){line.push(null);f++;}}
      else{line.push(ch);f++;}
    }
    grid.push(line);
  }
  const ourSet=new Set([from,to]);
  const lastSet=new Set([oppFrom,oppTo]);
  let h='<div class="board" role="img" aria-label="chess position">';
  for(let dr=0;dr<8;dr++){                    // display rows/cols (flip for black)
    for(let df=0;df<8;df++){
      const r=FLIP?7-dr:dr, f=FLIP?7-df:df;
      h+=cell(f,r,ourSet,lastSet,grid[r][f]);
    }
  }
  return h+arrow(from,to)+"</div>";
}
function cell(f,r,ourSet,lastSet,ch){
  const light=(f+r)%2===0, sq=sqName(f,r);    // true coords → a1 stays dark when flipped
  const cls=`sq ${light?'l':'d'}${lastSet.has(sq)?' last':''}${ourSet.has(sq)?' hl':''}`;
  let inner="";
  if(ch){const white=ch===ch.toUpperCase();
    inner=`<span class="pc ${white?'w':'k'}">${GLYPH[ch.toLowerCase()]}</span>`;}
  return `<div class="${cls}">${inner}</div>`;
}
function numLine(line){
  if(!line) return "starting position";
  const toks=line.split(" "), last=toks.length-1;
  return toks.map((t,i)=>i===last?`<b>${t}</b>`:t).join(" ");
}
function esClass(es){return es>=0.78?"es-win":es>=0.62?"es-good":"";}
// p is already a percentage (0-100). Show one decimal below 10%, integer above.
function fmtReach(p){return (p<0.1?"<0.1":p>=10?p.toFixed(0):p.toFixed(1))+"%";}
function moveHTML(m){return m.replace(/x/,'<span class="cap">x</span>');}
function candTable(cands){
  let rows=cands.map(c=>{
    const [san,games,es,ass,delta,chosen]=c;
    const w=Math.round(es*100);
    const g=games>0?games.toLocaleString():`<span class="unbooked">eval-only</span>`;
    return `<tr class="${chosen?'chosen':''}">
      <td class="mv l">${san}</td>
      <td class="${esClass(es)}">${es.toFixed(3)}</td>
      <td class="l"><span class="bar" style="width:${w*0.7}px"></span></td>
      <td>${chosen?'—':delta.toFixed(3)}</td>
      <td>${g}</td>
      <td class="l"><span class="tag">${ass}</span></td></tr>`;
  }).join("");
  return `<table class="g"><thead><tr>
    <th class="l">move</th><th>ES</th><th class="l"></th><th>Δ vs ours</th>
    <th>games</th><th class="l">verdict</th></tr></thead><tbody>${rows}</tbody></table>`;
}
function replyTable(reps){
  if(!reps.length) return `<div class="g-h">Opponent replies</div>
    <div style="color:var(--muted)">Terminal line — no further opponent branch in book.</div>`;
  let rows=reps.map(r=>{
    const [san,share,games,es,ass,gives,booked]=r;
    return `<tr>
      <td class="mv l">${san}</td>
      <td>${share.toFixed(1)}%</td>
      <td>${games.toLocaleString()}</td>
      <td class="${esClass(es)}">${es.toFixed(3)}</td>
      <td>${gives.toFixed(1)}%</td>
      <td class="l">${booked?`<span class="booked">${booked}</span>`:`<span class="unbooked">off-book</span>`}</td></tr>`;
  }).join("");
  return `<div class="g-h">Opponent replies &amp; our booked answer</div>
    <table class="g"><thead><tr>
    <th class="l">reply</th><th>share</th><th>games</th><th>ES after</th>
    <th>gives us</th><th class="l">we play</th></tr></thead><tbody>${rows}</tbody></table>`;
}
const host=document.getElementById("chapters");
CHAPTERS.forEach((chap,ci)=>{
  const h2=document.createElement("h2");
  h2.className="chapter";
  h2.innerHTML=`<span class="cnum">Chapter ${ci+1} of ${CHAPTERS.length}</span>${chap.title}`;
  host.appendChild(h2);
  chap.cards.forEach((d,i)=>{
    const el=document.createElement("section");
    el.className="card";
    el.innerHTML=`
      <div class="idx">Position ${i+1} of ${chap.cards.length}</div>
      <div class="path">${numLine(d.line)}</div>
      <div class="diagram">
        ${board(d.fen,d.from,d.to,d.oppFrom,d.oppTo)}
        <div class="decide">
          <div class="lbl">We play</div>
          <div class="move">${moveHTML(d.move)}${d.flagged?'<span class="flag">⚠ unverified</span>':''}</div>
          <span class="verdict ${d.win?'win':''}">${d.verdict}</span>
          <div class="reachline">Reached in ≈ ${fmtReach(d.reach_pct)} of games via this move order</div>
        </div>
      </div>
      <p class="prose">${d.annotation}</p>
      ${d.memory?`<div class="memory"><span class="k">🔑</span>
          <span class="t">${d.memory}</span></div>`:''}
      <details class="ground">
        <summary><span class="chev">›</span> The facts behind this note</summary>
        <div class="g-body">
          <div class="g-h">Candidate moves (what the model could choose from)</div>
          ${candTable(d.candidates)}
          ${replyTable(d.replies)}
        </div>
      </details>`;
    host.appendChild(el);
  });
});
"""

FOOTER = (
    "Expected score (ES) runs 0–1 for the side to move: <b>.50</b> even, "
    "<b>.66</b> “clearly better”, <b>.78+</b> “winning”. It is a sigmoid "
    "of the eval-DB centipawn score, so mates saturate — that's why the notes speak in "
    "words, not centipawns. “Gives us” = the share of games where a reply is the one "
    "that hands us a decisive edge. Engine principal-variation facts (multi-PV lines, threat "
    "probes) are attached once the Stockfish pass has run."
)


def render(color: str, chapters: list, stats: dict, engine_ready: bool) -> str:
    title = f"{color.capitalize()} Repertoire — Course (review build)"
    meta = (f'<span><b>{stats["chapters"]}</b> chapters</span>'
            f'<span><b>{stats["cards"]}</b> positions</span>'
            f'<span><b>{stats["flagged"]}</b> flagged</span>'
            f'<span>engine facts: <b>{"attached" if engine_ready else "not yet attached"}</b></span>')
    return f"""<title>{title}</title>
<style>{CSS}</style>
<div class="wrap">
  <header>
    <div class="eyebrow">Sharp {color.capitalize()} Repertoire · Auto-generated notes</div>
    <h1>{title}</h1>
    <p class="lede">Course-style study notes generated from the repertoire's own data.
      Each position shows the move to play, why, and a memory rule where one applies.</p>
    <div class="meta-row">{meta}</div>
    <div class="note"><b>How to read this as an evaluation.</b> Each note is written by Claude
      from a deterministic fact sheet — it may only phrase moves and numbers that were handed
      to it. Open <b>“The facts behind this note”</b> under any position to see exactly
      what it was given. Judge two things: is the prose <b>accurate to those facts</b>, and is it
      <b>useful to learn from</b>?</div>
    <div class="legend">
      <span><span class="sw last"></span> opponent's last move (how we got here)</span>
      <span><span class="sw hl"></span> the move to play</span>
      <span><span class="sw arw"></span> our move, from piece to square</span>
    </div>
  </header>
  <hr class="rule">
  <div id="chapters"></div>
  <footer>{FOOTER}</footer>
</div>
<script>
const FLIP = {json.dumps(color == "black")};
const CHAPTERS = {json.dumps(chapters, ensure_ascii=False)};
{JS_FUNCS}
</script>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None)
    ap.add_argument("--color", choices=["white", "black"], default="white")
    ap.add_argument("--out", default=None)
    ap.add_argument("--include-flagged", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    chapters, stats = build(data_dir, args.color, args.include_flagged)
    # engine facts present iff any fact sheet has a non-null engine block
    facts = pl.read_parquet(data_dir / "annotations" / f"facts_{args.color}.parquet")
    engine_ready = any(json.loads(r).get("engine") is not None
                       for r in facts["facts_json"].to_list()[:50])
    html = render(args.color, chapters, stats, engine_ready)
    out = Path(args.out) if args.out else (
        data_dir / "annotations" / f"book_{args.color}.html")
    out.write_text(html, encoding="utf-8")
    print(f"{args.color}: {stats['chapters']} chapters, {stats['cards']} positions "
          f"({stats['flagged']} flagged) -> {out}")


if __name__ == "__main__":
    main()
