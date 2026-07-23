"""
Stage C CLI: generate course-style annotations with headless Claude Code.

Runs one `claude -p` invocation per chunk under your existing Claude Code /
Pro subscription (no API key, no anthropic dependency). Each chunk is a
coherent variation (~8-25 positions) so annotations stay thematically
consistent. Output is validated (annotate.validate); failures get one retry
with feedback, then are stored flagged.

Resumable + skip-gated: a chunk is (re)generated only if any member's
fact_hash changed, a member is new, the stored prompt_version differs, or
--force. Interrupt any time (Ctrl-C or a usage limit) and rerun to continue.

Writes trainer_data/annotations/annotations.parquet and raw responses under
trainer_data/annotations/runs/.

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.generate_annotations
        [--data DIR] [--color white|black] [--model sonnet|opus]
        [--limit N]  (first N chunks — pilot) [--force] [--chunk CHUNK_ID]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from annotate.chunks import build_chunks
from annotate.prompts import PROMPT_VERSION, build_chunk_prompt
from annotate.validate import validate
from trainer_app.config import resolve_data_dir
from trainer_app.pack import TrainingPack

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ANN_COLS = ["color", "position_hash", "chunk_id", "fact_hash", "prompt_version",
            "model", "annotation", "memory_rule", "themes", "flagged",
            "flag_reason", "created_at"]

USAGE_LIMIT_MARKERS = ("usage limit", "rate limit", "quota", "exceeded your",
                       "please try again later")


class UsageLimit(Exception):
    pass


def run_claude(prompt: str, model: str, timeout: int = 900) -> str:
    """Invoke `claude -p` reading the prompt from stdin; return the model text.
    Raises UsageLimit on a limit/quota signal, RuntimeError on other failure.
    Tools are disabled — this is a pure text-generation task, no agentic loop."""
    cmd = ["claude", "-p", "--output-format", "json",
           "--allowed-tools", "", "--max-turns", "1"]
    if model:
        cmd += ["--model", model]
    try:
        proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True,
                              timeout=timeout, encoding="utf-8")
    except FileNotFoundError:
        raise RuntimeError("`claude` CLI not found on PATH")
    out = (proc.stdout or "") + (proc.stderr or "")
    low = out.lower()
    if proc.returncode != 0 and any(m in low for m in USAGE_LIMIT_MARKERS):
        raise UsageLimit(out[:300])
    if proc.returncode != 0:
        raise RuntimeError(f"claude exit {proc.returncode}: {out[:300]}")
    try:
        env = json.loads(proc.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"unparseable claude output: {proc.stdout[:300]}")
    if env.get("is_error"):
        if any(m in low for m in USAGE_LIMIT_MARKERS):
            raise UsageLimit(str(env)[:300])
        raise RuntimeError(f"claude error: {str(env)[:300]}")
    return env.get("result", "")


def extract_json(text: str) -> dict | None:
    """Pull the {annotations:[...]} object out of the model text (tolerating
    code fences / surrounding prose)."""
    t = text.strip()
    t = re.sub(r"^```(?:json)?", "", t).strip()
    t = re.sub(r"```$", "", t).strip()
    start = t.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(t)):
        if t[i] == "{":
            depth += 1
        elif t[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(t[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def load_store(path: Path) -> dict[int, dict]:
    if not path.exists():
        return {}
    return {r["position_hash"]: r for r in pl.read_parquet(path).iter_rows(named=True)}


def save_store(store: dict[int, dict], path: Path) -> None:
    df = pl.DataFrame(list(store.values()),
                      schema={c: (pl.List(pl.Utf8) if c == "themes"
                                  else pl.Boolean if c == "flagged"
                                  else pl.Int64 if c == "position_hash"
                                  else pl.Utf8) for c in ANN_COLS})
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None)
    ap.add_argument("--color", choices=["white", "black"], default=None)
    ap.add_argument("--model", default="sonnet", help="claude -p --model value")
    ap.add_argument("--limit", type=int, default=None, help="first N eligible chunks")
    ap.add_argument("--chunk", default=None, help="regenerate one chunk_id")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--timeout", type=int, default=900, help="per-chunk claude timeout (s)")
    ap.add_argument("--smallest", action="store_true",
                    help="pilot: process eligible chunks smallest-first")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    ann_dir = data_dir / "annotations"
    runs_dir = ann_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    store_path = ann_dir / "annotations.parquet"
    store = load_store(store_path)
    pack = TrainingPack(data_dir / "pack")

    colors = [args.color] if args.color else ["white", "black"]
    total_gen = 0
    for color in colors:
        facts_path = ann_dir / f"facts_{color}.parquet"
        if not facts_path.exists():
            print(f"{color}: no facts — run build_annotation_facts first"); continue
        facts = {r["position_hash"]: r for r in pl.read_parquet(facts_path).iter_rows(named=True)}
        chunks = build_chunks(pack, color)

        # prune store rows whose positions vanished from facts
        for h in [h for h, r in store.items() if r["color"] == color and h not in facts]:
            del store[h]

        eligible = []
        for ch in chunks:
            if args.chunk and ch.chunk_id != args.chunk:
                continue
            need = args.force
            for h in ch.card_hashes:
                s = store.get(h)
                if (s is None or s["fact_hash"] != facts[h]["fact_hash"]
                        or s["prompt_version"] != PROMPT_VERSION):
                    need = True
                    break
            if need:
                eligible.append(ch)
        if args.smallest:
            eligible.sort(key=lambda c: len(c.card_hashes))
        if args.limit:
            eligible = eligible[:args.limit]
        print(f"\n{color}: {len(eligible)}/{len(chunks)} chunks to generate", flush=True)

        for ci, ch in enumerate(eligible, 1):
            positions = [(f"p{i+1}", json.loads(facts[h]["facts_json"]))
                         for i, h in enumerate(ch.card_hashes)]
            id_to_hash = {f"p{i+1}": h for i, h in enumerate(ch.card_hashes)}
            chapter = " ".join(
                (f"{j//2+1}.{s}" if j % 2 == 0 else s)
                for j, s in enumerate(ch.chapter_sans))
            prompt = build_chunk_prompt(color, chapter, positions)

            try:
                t0 = time.time()
                text = run_claude(prompt, args.model, args.timeout)
            except UsageLimit as e:
                print(f"  usage limit hit ({e}); saving and stopping — rerun to resume",
                      flush=True)
                save_store(store, store_path)
                sys.exit(2)
            (runs_dir / f"{ch.chunk_id}.txt").write_text(text, encoding="utf-8")
            parsed = extract_json(text)
            if not parsed or "annotations" not in parsed:
                print(f"  [{ci}/{len(eligible)}] {ch.chunk_id}: unparseable — flagged",
                      flush=True)
                _store_flagged(store, ch, color, facts, args.model,
                               "unparseable model output")
                save_store(store, store_path)
                continue

            by_id = {a.get("id"): a for a in parsed["annotations"]}
            # collect validation errors for a possible single retry
            retry_notes = []
            results = {}
            for pid, f in positions:
                a = by_id.get(pid)
                if not a:
                    retry_notes.append(f"{pid}: missing from your output")
                    continue
                errs = validate(a.get("annotation", ""), a.get("memory_rule"),
                                a.get("themes", []), f)
                results[pid] = (a, errs)
                if errs:
                    retry_notes.append(f"{pid}: " + "; ".join(errs[:3]))

            if retry_notes:
                fix = (prompt + "\n\nYour previous answer had these problems — "
                       "return the full corrected JSON for ALL ids, fixing:\n- "
                       + "\n- ".join(retry_notes[:20]))
                try:
                    text2 = run_claude(fix, args.model, args.timeout)
                    p2 = extract_json(text2)
                    if p2 and "annotations" in p2:
                        by_id = {a.get("id"): a for a in p2["annotations"]}
                        results = {}
                        for pid, f in positions:
                            a = by_id.get(pid)
                            if a:
                                results[pid] = (a, validate(
                                    a.get("annotation", ""), a.get("memory_rule"),
                                    a.get("themes", []), f))
                except (UsageLimit, RuntimeError):
                    pass

            n_flagged = 0
            for pid, f in positions:
                h = id_to_hash[pid]
                a, errs = results.get(pid, (None, ["no annotation produced"]))
                store[h] = {
                    "color": color, "position_hash": h, "chunk_id": ch.chunk_id,
                    "fact_hash": facts[h]["fact_hash"], "prompt_version": PROMPT_VERSION,
                    "model": args.model,
                    "annotation": (a or {}).get("annotation", "") if a else "",
                    "memory_rule": (a or {}).get("memory_rule") if a else None,
                    "themes": (a or {}).get("themes", []) if a else [],
                    "flagged": bool(errs),
                    "flag_reason": "; ".join(errs[:3]) if errs else None,
                    "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                n_flagged += bool(errs)
            total_gen += len(positions)
            save_store(store, store_path)
            print(f"  [{ci}/{len(eligible)}] {ch.chunk_id}: {len(positions)} positions, "
                  f"{n_flagged} flagged ({time.time()-t0:.0f}s)", flush=True)

    print(f"\nDone. {total_gen} annotations generated/updated -> {store_path}")


def _store_flagged(store, ch, color, facts, model, reason):
    for h in ch.card_hashes:
        store[h] = {
            "color": color, "position_hash": h, "chunk_id": ch.chunk_id,
            "fact_hash": facts[h]["fact_hash"], "prompt_version": PROMPT_VERSION,
            "model": model, "annotation": "", "memory_rule": None, "themes": [],
            "flagged": True, "flag_reason": reason,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


if __name__ == "__main__":
    main()
