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
  lichess_token: "Lichess API token (optional; write-only: blank keeps it, - clears it)",
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
    } else if (key === "lichess_token") {
      input = document.createElement("input");
      input.type = "password";
      input.value = "";
      input.placeholder = s.lichess_token_set ? "saved (hidden)" : "not set";
      input.autocomplete = "off";
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

async function loadAccess() {
  let box = document.getElementById("remoteAccess");
  if (!box) {
    box = document.createElement("div");
    box.id = "remoteAccess";
    box.style.marginTop = "1rem";
    document.getElementById("settingsMsg").after(box);
  }
  const r = await fetch("/api/access");
  if (r.ok) {
    const a = await r.json();
    box.innerHTML = "";
    const h = document.createElement("h3");
    h.textContent = "Remote access";
    box.appendChild(h);
    const p = document.createElement("p");
    p.textContent = a.remote
      ? "On for this home network. On a phone or laptop, open one of these and enter the key:"
      : "Off (this PC only). Start the trainer with run_trainer_remote.bat to use it from a phone or laptop.";
    box.appendChild(p);
    if (a.remote) {
      for (const u of a.urls) {
        const d = document.createElement("div");
        d.textContent = u;
        d.style.fontFamily = "monospace";
        box.appendChild(d);
      }
      const k = document.createElement("p");
      k.textContent = "Access key: " + a.key;
      k.style.fontFamily = "monospace";
      box.appendChild(k);
    }
  } else if (r.status === 403) {
    // another device: settings, file paths and explorer loading stay on the PC
    box.textContent = "Settings can only be changed on the PC running the trainer.";
    document.getElementById("saveSettings").disabled = true;
    document.querySelectorAll("#settingsForm [data-key]").forEach(el => el.disabled = true);
    const lb = document.getElementById("explorerLoadBtn");
    if (lb) lb.disabled = true;
  }
}

loadSettings().then(loadAccess);
loadPackInfo();
