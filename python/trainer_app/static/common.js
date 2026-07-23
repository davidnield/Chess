// Shell: tab switching, settings form, pack info, M0 board smoke-render.

const PIECE_THEME = "/static/vendor/img/chesspieces/wikipedia/{piece}.png";

// ---- tabs ----
document.querySelectorAll("#tabs button").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("#tabs button").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
  });
});

// ---- explorer lazy load ----
async function pollExplorer() {
  const s = await fetch("/api/explorer/status").then(r => r.json());
  const el = document.getElementById("explorerStatus");
  if (s.status === "ready") {
    document.getElementById("explorerGate").style.display = "none";
    const f = document.getElementById("explorerFrame");
    f.style.display = "block";
    if (!f.src) f.src = "/explorer";
  } else if (s.status === "loading") {
    el.textContent = "Loading repertoires + stats… (~1 min on first open)";
    setTimeout(pollExplorer, 2000);
  } else if (s.status === "error") {
    el.textContent = "Explorer load failed: " + s.error;
    document.getElementById("explorerLoadBtn").disabled = false;
  }
}
document.getElementById("explorerLoadBtn").addEventListener("click", async () => {
  document.getElementById("explorerLoadBtn").disabled = true;
  await fetch("/api/explorer/load", { method: "POST" });
  pollExplorer();
});

// ---- settings ----
const SETTING_LABELS = {
  lichess_username: "Lichess username",
  lichess_token: "Lichess API token (optional)",
  import_since: "Import games since (YYYY-MM-DD)",
  priority_mode: "Priority mode",
  reach_exp_a: "Reach exponent (a)",
  memo_exp_b: "Memo-cost exponent (b)",
  new_per_day: "New cards per day",
  desired_retention: "Desired retention",
  review_once_per_day: "Grade each card at most once per day",
  extend_past_cap: "Introduce mid-line new cards past the daily cap",
  white_rep: "White repertoire parquet",
  black_rep: "Black repertoire parquet",
  stats: "Position stats parquet",
  crush_totals: "Crush edge totals parquet",
};

async function loadSettings() {
  const s = await fetch("/api/settings").then(r => r.json());
  const form = document.getElementById("settingsForm");
  form.innerHTML = "";
  for (const [key, label] of Object.entries(SETTING_LABELS)) {
    const v = s[key];
    const lab = document.createElement("label");
    lab.textContent = label;
    lab.htmlFor = "set_" + key;
    let input;
    if (key === "priority_mode") {
      input = document.createElement("select");
      for (const m of ["frequency", "value", "balanced", "custom"]) {
        const o = document.createElement("option");
        o.value = o.textContent = m;
        if (m === v) o.selected = true;
        input.appendChild(o);
      }
    } else if (typeof v === "boolean") {
      input = document.createElement("input");
      input.type = "checkbox";
      input.checked = v;
    } else {
      input = document.createElement("input");
      input.type = "text";
      input.value = v;
    }
    input.id = "set_" + key;
    input.dataset.key = key;
    input.dataset.type = typeof v;
    form.appendChild(lab);
    form.appendChild(input);
  }
}

document.getElementById("saveSettings").addEventListener("click", async () => {
  const updates = {};
  document.querySelectorAll("#settingsForm [data-key]").forEach(el => {
    const k = el.dataset.key;
    if (el.type === "checkbox") updates[k] = el.checked;
    else if (el.dataset.type === "number") updates[k] = Number(el.value);
    else updates[k] = el.value;
  });
  const r = await fetch("/api/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(updates),
  });
  document.getElementById("settingsMsg").textContent =
    r.ok ? "saved ✓" : "save failed: " + (await r.text());
  if (r.ok) loadSettings();  // reflect preset-driven exponent changes
});

async function loadPackInfo() {
  const info = await fetch("/api/pack/info").then(r => r.json());
  document.getElementById("packInfo").textContent = JSON.stringify(info, null, 2);
  if (info.stale && info.stale.length) {
    const b = document.createElement("div");
    b.id = "staleBanner";
    b.textContent = `⚠ Training pack is older than the ${info.stale.join(" + ")} ` +
      `repertoire parquet — rerun build_training_pack.py to train the current repertoire.`;
    document.querySelector("main").prepend(b);
  }
}

loadSettings();
loadPackInfo();
