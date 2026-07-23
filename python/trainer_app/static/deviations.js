// Deviations tab: import trigger + polling, deviation table, board replay.

(() => {
  const $ = (id) => document.getElementById(id);
  let devBoard = null;
  let replay = null;        // {sans, ply, game, idx, color, gameId, lineToDev}
  let pollTimer = null;

  function fmtPct(x) {
    return x == null ? "–" : (x * 100).toFixed(2) + "%";
  }

  async function refreshImportStatus(keepPolling) {
    const s = await fetch("/api/deviations/import/status").then((r) => r.json());
    let txt = s.status === "never run" ? "no imports yet"
      : `last import: ${s.status} · ${s.n_games ?? 0} games · ${s.n_deviations ?? 0} deviations`
        + (s.error ? ` · ${s.error}` : "");
    $("devImportStatus").textContent = txt;
    $("devImportBtn").disabled = s.status === "running";
    if (s.status === "running" && keepPolling) {
      pollTimer = setTimeout(() => refreshImportStatus(true), 2500);
    } else if (s.status !== "running") {
      loadTable();
    }
  }

  $("devImportBtn").addEventListener("click", async () => {
    $("devImportBtn").disabled = true;
    const r = await fetch("/api/deviations/import", { method: "POST" });
    if (!r.ok) {
      $("devImportStatus").textContent = (await r.json()).detail;
      $("devImportBtn").disabled = false;
      return;
    }
    refreshImportStatus(true);
  });
  $("devRepFilter").addEventListener("change", loadTable);

  async function loadTable() {
    const rep = $("devRepFilter").value;
    const rows = await fetch(`/api/deviations${rep ? "?rep=" + rep : ""}`)
      .then((r) => r.json());
    const t = $("devTable");
    t.innerHTML =
      "<tr><th>date</th><th>col</th><th>opponent</th><th>expected</th>" +
      "<th>played</th><th>reach</th><th>memo</th><th>×here</th><th></th></tr>";
    for (const d of rows) {
      const tr = document.createElement("tr");
      tr.className = "clickable";
      tr.innerHTML =
        `<td>${(d.played_at || "").slice(0, 10)}</td><td>${d.rep[0]}</td>` +
        `<td>${d.opponent || "?"} (${d.speed}, ${d.result})</td>` +
        `<td><b>${d.expected_move}</b></td><td class="bad">${d.played_move}</td>` +
        `<td>${fmtPct(d.reach)}</td><td>${d.memo_cost == null ? "–" : d.memo_cost.toFixed(3)}</td>` +
        `<td>${d.times_here}</td><td><button data-dismiss="${d.id}">✕</button></td>`;
      tr.addEventListener("click", (e) => {
        if (e.target.dataset.dismiss) return;
        showDeviation(d);
      });
      tr.querySelector("[data-dismiss]").addEventListener("click", async (e) => {
        await fetch(`/api/deviations/${e.target.dataset.dismiss}/dismiss`,
                    { method: "POST" });
        loadTable();
      });
      t.appendChild(tr);
    }
    if (!rows.length) {
      t.innerHTML += "<tr><td colspan=9 class='muted'>no deviations 🎉</td></tr>";
    }
  }

  async function showDeviation(d) {
    const g = await fetch(`/api/deviations/${d.id}/game`).then((r) => r.json());
    if (!devBoard) {
      devBoard = Chessboard("devBoard", { position: "start", pieceTheme: PIECE_THEME });
    }
    replay = {
      sans: g.moves_san.split(/\s+/), ply: g.ply, game: new Chess(),
      idx: 0, color: g.color, gameId: g.game_id,
    };
    devBoard.orientation(g.color);
    // jump to the deviation ply (position BEFORE the deviating move)
    while (replay.idx < replay.ply) step(1, false);
    render();
  }

  function step(dir, doRender = true) {
    if (!replay) return;
    if (dir > 0 && replay.idx < replay.sans.length) {
      replay.game.move(replay.sans[replay.idx], { sloppy: true });
      replay.idx++;
    } else if (dir < 0 && replay.idx > 0) {
      replay.game.undo();
      replay.idx--;
    }
    if (doRender) render();
  }

  function render() {
    devBoard.position(replay.game.fen());
    $("devMoveNo").textContent =
      `ply ${replay.idx}/${replay.sans.length}` +
      (replay.idx === replay.ply ? " (deviation here)" : "");
    $("devLichess").href = `https://lichess.org/${replay.gameId}`;
    const line = replay.sans.slice(0, replay.ply).join(" ");
    $("devExplorer").href = `/explorer#line=${encodeURIComponent(line)}`;
  }

  $("devPrev").addEventListener("click", () => step(-1));
  $("devNext").addEventListener("click", () => step(1));

  document.querySelector('[data-tab="deviations"]').addEventListener("click", () => {
    refreshImportStatus(false);
  });
})();
