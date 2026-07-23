// Trainer tab: session flow, drill playback, board-input grading.
//
// Drill contract (GET /api/trainer/drill): { drill_id, rep, kind, start_index,
// steps: [{san, our}], target_ply }. steps[:start_index] = lead-in (animated,
// not graded); from start_index on, opponent moves auto-play and our moves
// await board input, graded server-side via POST /api/trainer/answer.

(() => {
  const $ = (id) => document.getElementById(id);
  let board = null;          // chessboard.js instance (created lazily)
  let game = null;           // chess.js state
  let sessionId = null;
  let rep = "white";
  let drill = null;          // active drill payload
  let stepIdx = 0;           // next step to execute
  let awaiting = false;      // waiting for user's move at stepIdx
  let retrying = false;      // wrong answer given; waiting for the correct move
  let moveStart = 0;         // perf.now() when the prompt appeared

  const OPP_DELAY = 450, LEADIN_DELAY = 250;

  function ensureBoard() {
    if (board) return;
    board = Chessboard("trainerBoard", {
      position: "start",
      draggable: true,
      pieceTheme: PIECE_THEME,
      onDragStart: () => awaiting || retrying,
      onDrop: onDrop,
      onSnapEnd: () => board.position(game.fen()),
    });
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
      const r = await fetch("/api/trainer/session/start", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rep }),
      }).then((x) => x.json());
      sessionId = r.session_id;
      $("endSession").disabled = false;
      $("startSession").textContent = "Next drill";
    }
    await nextDrill();
  }

  async function nextDrill() {
    $("feedback").textContent = "";
    const r = await fetch(`/api/trainer/drill?rep=${rep}&session_id=${sessionId}`);
    if (r.status === 404) {
      $("drillInfo").innerHTML = "<b>Queue empty 🎉</b> — no due or new cards right now.";
      drill = null;
      await refreshStatus();
      return;
    }
    drill = await r.json();
    stepIdx = 0;
    awaiting = retrying = false;
    game = new Chess();
    board.orientation(drill.rep);
    board.position("start", false);
    $("drillInfo").textContent =
      `${drill.kind === "new" ? "NEW line" : "review"} · ${drill.steps.length} plies` +
      (drill.start_index > 0 ? ` · starting mid-line (ply ${drill.start_index + 1})` : "");
    renderLine();
    advance();
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
      if (drill) {
        $("feedback").innerHTML = "<b>Line complete ✓</b>";
        drill = null;
        setTimeout(nextDrill, 900);
      }
      return;
    }
    const step = drill.steps[stepIdx];
    const leadIn = stepIdx < drill.start_index;
    if (leadIn || !step.our) {
      setTimeout(() => {
        game.move(step.san, { sloppy: true });
        board.position(game.fen());
        stepIdx++;
        advance();
      }, leadIn ? LEADIN_DELAY : OPP_DELAY);
    } else {
      awaiting = true;
      moveStart = performance.now();
      $("feedback").textContent = "Your move…";
      showAnnotation(null);              // clear previous card's note
    }
  }

  function onDrop(source, target) {
    if (!awaiting && !retrying) return "snapback";
    const mv = game.move({ from: source, to: target, promotion: "q" });
    if (mv === null) return "snapback";
    const step = drill.steps[stepIdx];

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
    fetch("/api/trainer/answer", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ drill_id: drill.drill_id, ply: stepIdx,
                             san: mv.san, duration_ms: duration }),
    }).then((r) => r.json()).then((res) => {
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

  refreshStatus();
  window._trainerRefresh = refreshStatus;
})();
