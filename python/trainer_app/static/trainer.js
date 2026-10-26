// Trainer tab: session flow, drill playback, board-input grading.
//
// Drill contract (GET /api/trainer/drill): { drill_id, rep, kind, start_index,
// steps: [{san, our, new, note}], target_ply }. steps[:start_index] = lead-in
// (animated, not graded); from start_index on, opponent moves auto-play and our
// moves await board input, graded server-side via POST /api/trainer/answer.
//
// Learn mode: a drill with new moves (`new` = never reviewed) runs twice.
//   show   — each new move is drawn as an arrow with its note; the user plays
//            it (not graded). Known moves on the way are quizzed as usual.
//   recall — the line restarts at start_index; known moves auto-play and the
//            new ones are quizzed from memory: these answers grade the cards.

(() => {
  const $ = (id) => document.getElementById(id);
  let board = null;          // chessboard.js instance
  let game = null;           // chess.js state
  let sessionId = null;
  let rep = "white";
  let drill = null;          // active drill payload
  let stepIdx = 0;           // next step to execute
  let awaiting = false;      // waiting for user's move at stepIdx
  let retrying = false;      // wrong answer given; waiting for the correct move
  let teaching = false;      // learn mode: waiting for the user to play the shown move
  let phase = "drill";       // drill | show | recall (learn mode)
  let moveStart = 0;         // perf.now() when the prompt appeared

  // Replay pace: the lead-in delay per ply is user-set (slider, remembered in
  // this browser's localStorage); opponent replies keep their old 450:250 ratio.
  const DELAY_KEY = "trainer.replayDelayMs", DEFAULT_DELAY = 500, OPP_RATIO = 1.8;
  let leadinDelay = DEFAULT_DELAY;
  try {
    const saved = Number(localStorage.getItem(DELAY_KEY));
    if (saved >= 100 && saved <= 2000) leadinDelay = saved;
  } catch (e) { /* storage blocked: use the default */ }
  const oppDelay = () => Math.round(leadinDelay * OPP_RATIO);

  function showDelay() {
    $("replayDelay").value = leadinDelay;
    $("replayDelayVal").textContent = `${(leadinDelay / 1000).toFixed(2)} s/move`;
  }
  $("replayDelay").addEventListener("input", (e) => {
    leadinDelay = Number(e.target.value);
    showDelay();
    try { localStorage.setItem(DELAY_KEY, String(leadinDelay)); } catch (err) { /* not remembered */ }
  });
  showDelay();

  function ensureBoard() {
    if (board) return;
    board = Chessboard("trainerBoard", {
      position: "start",
      draggable: true,
      pieceTheme: PIECE_THEME,
      onDragStart: () => drill !== null && (awaiting || retrying || teaching),
      onDrop: onDrop,
      onSnapEnd: () => board.position(game.fen()),
    });
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.id = "boardArrow";
    $("trainerBoard").appendChild(svg);
  }

  // Draw our move to play as an arrow over the board (learn mode).
  function drawArrow(san) {
    const svg = $("boardArrow");
    const mv = new Chess(game.fen()).move(san, { sloppy: true });
    if (!svg || !mv) return;
    const box = $("trainerBoard").getBoundingClientRect();
    const centre = (sq) => {
      const r = document.querySelector(`#trainerBoard [data-square="${sq}"]`).getBoundingClientRect();
      return [r.left - box.left + r.width / 2, r.top - box.top + r.height / 2, r.width];
    };
    const [x1, y1, w] = centre(mv.from), [x2, y2] = centre(mv.to);
    const len = Math.hypot(x2 - x1, y2 - y1), ux = (x2 - x1) / len, uy = (y2 - y1) / len;
    const head = w * 0.42, shaft = w * 0.16;
    const bx = x2 - ux * head, by = y2 - uy * head, px = -uy, py = ux;
    svg.setAttribute("viewBox", `0 0 ${box.width} ${box.height}`);
    svg.innerHTML =
      `<line x1="${x1}" y1="${y1}" x2="${bx}" y2="${by}" stroke-width="${shaft}" stroke-linecap="round"/>` +
      `<polygon points="${x2},${y2} ${bx + px * head * 0.6},${by + py * head * 0.6} ${bx - px * head * 0.6},${by - py * head * 0.6}"/>`;
  }
  function clearArrow() {
    const svg = $("boardArrow");
    if (svg) svg.innerHTML = "";
  }

  async function refreshStatus() {
    try {
      const r = await fetch(`/api/trainer/status?rep=${rep}`);
      if (!r.ok) {
        $("trainerStatus").textContent = (await r.json()).detail || "pack missing";
        $("startSession").disabled = true;
        return;
      }
      const s = await r.json();
      $("trainerStatus").textContent =
        `${s.cards_total} cards · ${s.due} due · ${s.new_remaining}/${s.new_today + s.new_remaining} new remaining today`;
      $("startSession").disabled = false;
      if (s.active_session) {
        sessionId = s.active_session.session_id;
        $("endSession").disabled = false;
        $("startSession").textContent = "Next drill";
      }
    } catch (e) {
      $("trainerStatus").textContent = "server unreachable";
    }
  }

  async function startOrNext() {
    ensureBoard();
    if (sessionId === null) {
      let r;
      try {
        r = await api("/api/trainer/session/start", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rep }),
        });
      } catch (e) { showError(e.message); return; }
      sessionId = r.session_id;
      $("endSession").disabled = false;
      $("startSession").textContent = "Next drill";
    }
    await nextDrill();
  }

  // fetch + JSON with readable failures: a stopped server or an error reply
  // must say so on screen, never leave the page silently waiting.
  async function api(url, opts) {
    let r;
    try {
      r = await fetch(url, opts);
    } catch (e) {
      throw Object.assign(new Error("Can't reach the trainer server — is it still running?"), { status: 0 });
    }
    let body = null;
    try { body = await r.json(); } catch (e) { /* not JSON */ }
    if (!r.ok) {
      throw Object.assign(new Error((body && body.detail) || `trainer server error ${r.status}`),
                          { status: r.status });
    }
    return body;
  }

  function showError(msg) {
    const span = document.createElement("span");
    span.className = "bad";
    span.textContent = msg;
    $("feedback").replaceChildren(span);
  }

  async function nextDrill() {
    $("feedback").textContent = "";
    let d;
    try {
      d = await api(`/api/trainer/drill?rep=${rep}&session_id=${sessionId}`);
    } catch (e) {
      if (e.status === 404) {
        $("drillInfo").innerHTML = "<b>Queue empty 🎉</b> — no due or new cards right now.";
        drill = null;
        await refreshStatus();
      } else {
        showError(e.message);
      }
      return;
    }
    drill = d;
    stepIdx = 0;
    awaiting = retrying = teaching = false;
    clearArrow();
    showAnnotation(null);
    const nNew = drill.steps.filter((s, i) => s.new && i >= drill.start_index).length;
    phase = nNew ? "show" : "drill";
    $("chapterText").textContent = drill.chapter || "";
    $("chapterBox").hidden = !drill.chapter;
    game = new Chess();
    board.orientation(drill.rep);
    board.position("start", false);
    $("drillInfo").textContent =
      (drill.family ? `${drill.family} · drill ${drill.family_drill} of ${drill.drills_per_family}\n` : "") +
      (nNew ? `NEW line · ${nNew} new move${nNew > 1 ? "s" : ""} to learn` : "review") +
      ` · ${drill.steps.length} plies` +
      (drill.start_index > 0 ? ` · starting mid-line (ply ${drill.start_index + 1})` : "");
    renderLine();
    advance();
  }

  // Learn mode, after the show pass: replay the line from start_index.
  function startRecall() {
    phase = "recall";
    game = new Chess();
    for (let i = 0; i < drill.start_index; i++) game.move(drill.steps[i].san, { sloppy: true });
    stepIdx = drill.start_index;
    board.position(game.fen());
    showAnnotation(null);
    $("feedback").innerHTML = "<b>Now play it from memory</b>";
    renderLine();
    setTimeout(advance, oppDelay());
  }

  function renderLine() {
    if (!drill) { $("drillLine").textContent = ""; return; }
    const parts = [];
    for (let i = 0; i < stepIdx; i++) {
      if (i % 2 === 0) parts.push(`${i / 2 + 1}.`);
      parts.push(drill.steps[i].san);
    }
    $("drillLine").textContent = parts.join(" ");
  }

  function advance() {
    renderLine();
    if (!drill || stepIdx >= drill.steps.length) {
      if (drill && phase === "show") {
        $("feedback").innerHTML = "<b>That's the line.</b> Now from memory…";
        setTimeout(startRecall, 1500);
      } else if (drill) {
        $("feedback").innerHTML = "<b>Line complete ✓</b>";
        drill = null;
        setTimeout(nextDrill, 900);
      }
      return;
    }
    const step = drill.steps[stepIdx];
    const leadIn = stepIdx < drill.start_index;
    // recall pass: moves already handled in the show pass replay by themselves
    const replayed = phase === "recall" && step.our && !step.new;
    if (leadIn || !step.our || replayed) {
      setTimeout(() => {
        game.move(step.san, { sloppy: true });
        board.position(game.fen());
        stepIdx++;
        advance();
      }, leadIn || replayed ? leadinDelay : oppDelay());
    } else if (phase === "show" && step.new) {
      teaching = true;
      drawArrow(step.san);
      $("feedback").innerHTML = `<b>New move:</b> play <b>${step.san}</b>`;
      showAnnotation(step.note);
    } else {
      awaiting = true;
      moveStart = performance.now();
      $("feedback").textContent = "Your move…";
      showAnnotation(null);              // clear previous card's note
    }
  }

  function onDrop(source, target) {
    if (!awaiting && !retrying && !teaching) return "snapback";
    const mv = game.move({ from: source, to: target, promotion: "q" });
    if (mv === null) return "snapback";
    const step = drill.steps[stepIdx];

    if (teaching) {
      // learn mode: only the shown move continues; nothing is graded yet
      if (mv.san !== step.san) {
        game.undo();
        return "snapback";
      }
      teaching = false;
      clearArrow();
      stepIdx++;
      $("feedback").textContent = "";
      setTimeout(advance, 150);
      return;
    }

    if (retrying) {
      // Grading already happened (wrong first answer); require the book move.
      if (mv.san === step.san) {
        retrying = false;
        stepIdx++;
        $("feedback").textContent = "";
        setTimeout(advance, 150);
      } else {
        game.undo();
        return "snapback";
      }
      return;
    }

    awaiting = false;
    const duration = Math.round(performance.now() - moveStart);
    const answered = drill;
    api("/api/trainer/answer", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ drill_id: drill.drill_id, ply: stepIdx,
                             san: mv.san, duration_ms: duration }),
    }).catch((e) => {
      // not graded: take the move back so it can be played again
      if (drill !== answered) return null;
      game.undo();
      board.position(game.fen());
      if (e.status === 400 && /unknown or expired drill/.test(e.message)) {
        drill = null;                    // the server restarted and forgot this drill
        showError("This drill was lost when the trainer restarted — click Next drill.");
      } else {
        awaiting = true;
        moveStart = performance.now();
        showError(`${e.message} Your move was not recorded; play it again.`);
      }
      return null;
    }).then((res) => {
      if (res === null || drill !== answered) return;
      if (res.correct) {
        $("feedback").innerHTML = "<span class='good'>✓</span>";
        stepIdx++;
        setTimeout(advance, 150);
      } else {
        game.undo();
        board.position(game.fen());
        retrying = true;
        $("feedback").innerHTML =
          `<span class='bad'>✗ book move is <b>${res.expected_san}</b> — play it to continue</span>`;
      }
      showAnnotation(res.annotation);
      refreshStatus();
    });
    // keep the piece where it was dropped for the correct case; snapback is
    // handled by re-syncing to game.fen() in onSnapEnd after an undo.
  }

  $("startSession").addEventListener("click", startOrNext);
  $("endSession").addEventListener("click", async () => {
    if (sessionId !== null) {
      await fetch("/api/trainer/session/end", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId }),
      });
    }
    sessionId = null;
    drill = null;
    teaching = false;
    clearArrow();
    showAnnotation(null);
    $("chapterBox").hidden = true;
    $("startSession").textContent = "Start session";
    $("endSession").disabled = true;
    $("drillInfo").textContent = "";
    $("feedback").textContent = "";
    $("drillLine").textContent = "";
    refreshStatus();
  });
  $("repSelect").addEventListener("change", (e) => {
    rep = e.target.value;
    sessionId = null;
    $("startSession").textContent = "Start session";
    $("endSession").disabled = true;
    refreshStatus();
  });

  function showAnnotation(a) {
    const el = $("annotation");
    if (!el) return;
    if (!a || !a.annotation) { el.innerHTML = ""; return; }
    let html = `<div class="annot-text">${a.annotation}</div>`;
    if (a.memory_rule) html += `<div class="annot-rule">🔑 ${a.memory_rule}</div>`;
    if (a.flagged) html += `<div class="annot-flag">⚠ unverified — check against the board</div>`;
    el.innerHTML = html;
  }

  async function loadStats() {
    const r = await fetch(`/api/trainer/stats?rep=${rep}`);
    if (!r.ok) return;
    const s = await r.json();
    const ret = s.retention_30d == null ? "–" : (s.retention_30d * 100).toFixed(1) + "%";
    const days = s.days.slice(-7).map((d) => `${d.date}: ${d.correct}/${d.reviews}`).join("<br>");
    const fc = s.due_forecast.map((d) => `${d.date}: ${d.count}`).join("<br>") || "nothing due";
    const hard = s.hardest.map((h) => `${h.best_move} (${h.lapses}✗/${h.reps})`).join(", ") || "–";
    $("statsContent").innerHTML =
      `<b>${s.cards_started}</b> cards started · retention (30d): <b>${ret}</b> · ` +
      `${s.total_lapses} lapses / ${s.total_reps} reviews` +
      `<br><br><b>Last days</b><br>${days || "–"}` +
      `<br><br><b>Due forecast</b><br>${fc}` +
      `<br><br><b>Hardest cards</b>: ${hard}`;
  }
  $("statsBox").addEventListener("toggle", (e) => { if (e.target.open) loadStats(); });

  ensureBoard();                         // show the board before the first drill
  refreshStatus();
  window._trainerRefresh = refreshStatus;
})();
