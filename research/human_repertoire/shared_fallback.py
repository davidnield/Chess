"""Engine-free shared move prior and strictly separated tuning/final scoring.

python -B -m research.human_repertoire.shared_fallback --tune
python -B -m research.human_repertoire.shared_fallback --test
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from math import isfinite, log
import random
import time

import chess

from .date_cohort import CONFIG
from .io import ROOT, output_path, sha256, write_json


def move_token(board, move):
    """Color-symmetric, context-poor move habit; no engine or outcome feature."""
    destination = move.to_square if board.turn else chess.square_mirror(move.to_square)
    return ":".join(map(str, (board.piece_type_at(move.from_square), destination,
        int(board.is_capture(move)), move.promotion or 0, int(board.is_castling(move)))))


def fit(games):
    positions, tokens = defaultdict(Counter), Counter()
    for game in games:
        for i, san in enumerate(game["moves"]):
            p = game["positions"][i]
            board = chess.Board(p["fen"])
            move = board.parse_san(san)
            positions[p["key"]][move.uci()] += 1
            tokens[move_token(board, move)] += 1
    return {"positions": {k: dict(v) for k, v in positions.items()}, "tokens": dict(tokens)}


def distribution(moves, tokens, counts, token_counts, spec, pseudocount=1):
    if not moves or len(moves) != len(tokens) or pseudocount <= 0:
        raise ValueError("invalid legal move support")
    if set(counts)-set(moves):
        raise ValueError("training move absent from legal support")
    if spec["prior"] == "uniform":
        prior = [1/len(moves)] * len(moves)
    elif spec["prior"] == "token":
        weights = [(token_counts.get(t, 0)+pseudocount)**spec["temperature"] for t in tokens]
        total = sum(weights)
        prior = [w/total for w in weights]
    else:
        raise ValueError("unknown prior")
    if spec["only_prior"]:
        return prior
    alpha = spec["alpha"]
    if alpha <= 0:
        raise ValueError("positive smoothing required")
    n = sum(counts.values())
    result = [(counts.get(m, 0)+alpha*q)/(n+alpha) for m, q in zip(moves, prior)]
    if abs(sum(result)-1) > 1e-10 or any(p <= 0 or not isfinite(p) for p in result):
        raise AssertionError("invalid move distribution")
    return result


def spec(prior, alpha=1, temperature=0, only_prior=False):
    return {"prior": prior, "alpha": alpha, "temperature": temperature, "only_prior": only_prior}


def grid(c):
    models = {"uniform_legal": spec("uniform", only_prior=True)}
    for a in c["prior_strengths"]:
        models[f"position_uniform_a{a:g}"] = spec("uniform", a)
    for t in c["token_temperatures"]:
        models[f"token_only_t{t:g}"] = spec("token", temperature=t, only_prior=True)
        for a in c["prior_strengths"]:
            models[f"position_token_a{a:g}_t{t:g}"] = spec("token", a, t)
    return models


def score(fitted, games, models, c):
    started = time.monotonic()
    cache, rows = {}, []
    totals = {name: defaultdict(Counter) for name in models}
    decisions = 0
    for game in games:
        if time.monotonic()-started > c["model_runtime_seconds"]:
            raise TimeoutError("prediction runtime guard")
        per_game = {name: Counter() for name in models}
        for i, san in enumerate(game["moves"]):
            decisions += 1
            if decisions > c["max_decisions_per_split"]:
                raise ValueError("decision cap exceeded")
            p = game["positions"][i]
            key = p["key"]
            if key not in cache:
                if len(cache) >= c["max_cached_positions"]:
                    raise ValueError("position cache cap exceeded")
                board = chess.Board(p["fen"])
                legal = list(board.legal_moves)
                ucis = [move.uci() for move in legal]
                sans = [board.san(move) for move in legal]
                tokens = [move_token(board, move) for move in legal]
                counts = fitted["positions"].get(key, {})
                probabilities = {name: distribution(ucis, tokens, counts, fitted["tokens"], setting,
                    c["move_token_pseudocount"]) for name, setting in models.items()}
                cached_scores = {}
                for name, ps in probabilities.items():
                    norm2 = sum(x*x for x in ps)
                    unseen_mass = sum(x for m, x in zip(ucis, ps) if m not in counts)
                    cached_scores[name] = ([-log(x) for x in ps], [1-2*x+norm2 for x in ps], unseen_mass)
                # Same Zobrist key excludes counters. Legal SAN/support depend
                # only on the first four FEN fields, which are audited here.
                cache[key] = (dict(zip(sans, range(len(sans)))), ucis, counts,
                    cached_scores, board.turn, " ".join(p["fen"].split()[:4]))
            indices, ucis, counts, predictions, white, fen_core = cache[key]
            if " ".join(p["fen"].split()[:4]) != fen_core:
                raise ValueError("inconsistent FEN for position key")
            if san not in indices:
                raise ValueError("held-out move is illegal")
            index = indices[san]
            n = sum(counts.values())
            groups = ("pooled", f"event:{game['event']}", "side:white" if white else "side:black",
                      "support:seen" if n else "support:unseen")
            for name, (ll, brier, unseen_mass) in predictions.items():
                value = {"decisions": 1, "log_loss": ll[index], "brier": brier[index],
                    "unseen_position": int(n == 0), "unseen_move": int(ucis[index] not in counts),
                    "thin_position": int(n < 5), "predicted_unseen_move_mass": unseen_mass}
                for group in groups:
                    totals[name][group].update(value)
                per_game[name].update(value)
        rows.append({"game_id": game["game_id"], "event": game["event"],
            "white": game["white"].casefold(), "black": game["black"].casefold(),
            "models": {name: dict(value) for name, value in per_game.items()}})
    summary = {}
    for name, groups in totals.items():
        summary[name] = {}
        for group, values in groups.items():
            n = values["decisions"]
            summary[name][group] = {k: v/n for k, v in values.items() if k != "decisions"}
            summary[name][group]["decisions"] = n
    return summary, rows


def choose(models, summary, prior, only_prior=False):
    names = [name for name, s in models.items() if s["prior"] == prior and s["only_prior"] == only_prior]
    return min(names, key=lambda n: (summary[n]["pooled"]["log_loss"], models[n]["alpha"], models[n]["temperature"], n))


def paired_bootstrap(rows, candidate, baseline, draws, seed):
    """Connected players as units; ratio of summed loss difference to decisions."""
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for row in rows:
        parent[find(row["white"])] = find(row["black"])
    blocks = defaultdict(lambda: [0.0, 0, 0])
    for row in rows:
        a, b = row["models"][candidate], row["models"][baseline]
        if a.get("decisions", 0) != b.get("decisions", 0):
            raise ValueError("paired decision counts differ")
        group = blocks[find(row["white"])]
        group[0] += a.get("log_loss", 0)-b.get("log_loss", 0)
        group[1] += a.get("decisions", 0)
        group[2] += 1
    blocks = list(blocks.values())
    if not blocks or sum(b[1] for b in blocks) == 0:
        raise ValueError("no scored decisions")
    rng = random.Random(seed)
    samples = []
    for _ in range(draws):
        chosen = rng.choices(blocks, k=len(blocks))
        n = sum(b[1] for b in chosen)
        if n == 0:
            raise ValueError("bootstrap draw contains no decisions")
        samples.append(sum(b[0] for b in chosen)/n)
    samples.sort()
    return {"estimate": sum(b[0] for b in blocks)/sum(b[1] for b in blocks),
        "percentile_95": [samples[int(.025*draws)], samples[min(draws-1, int(.975*draws))]],
        "components": len(blocks), "largest_component_games": max(b[2] for b in blocks),
        "games": len(rows), "decisions": sum(b[1] for b in blocks), "draws": draws, "seed": seed,
        "scope": "conditional on fitted/tuned model and selected frame; not training, selection, population or causal uncertainty"}


def inputs(c, splits):
    manifest = json.loads(output_path("m3/cohort_manifest.json").read_text())
    if manifest["identity"]["config"] != c:
        raise ValueError("cohort config drift")
    result = {}
    for split in splits:
        name = f"m3/{split}_games.json"
        path = output_path(name)
        if sha256(path) != manifest["snapshots"][name]:
            raise ValueError("cohort snapshot drift")
        result[split] = json.loads(path.read_text())
    return result


def identity(c):
    return {"config": c, "code_sha256": sha256(__file__),
        "cohort_manifest_sha256": sha256(output_path("m3/cohort_manifest.json")),
        "python_chess_version": chess.__version__,
        "plan_sha256": sha256(ROOT / "docs/human_repertoire/milestone3_plan.md")}


def tune(c):
    started = time.monotonic()
    current_identity = identity(c)
    path = output_path("m3/frozen_model.json")
    if path.exists():
        frozen = json.loads(path.read_text())
        if frozen["identity"] != current_identity:
            raise ValueError("frozen model identity drift")
        print("Verified frozen tuning; final test not opened", flush=True)
        return
    data = inputs(c, ("train", "tune"))
    fitted = fit(data["train"])
    models = grid(c)
    summary, rows = score(fitted, data["tune"], models, c)
    selected = {"uniform_legal": "uniform_legal", "milestone2_position": "position_uniform_a1",
        "tuned_position": choose(models, summary, "uniform"),
        "token_only": choose(models, summary, "token", True),
        "shared_position": choose(models, summary, "token")}
    tuning_hash = sha256(write_json("m3/tuning_scores.json", {"models": models, "summary": summary}))
    fitted_hash = sha256(write_json("m3/fitted_counts.json", fitted))
    write_json("m3/frozen_model.json", {"identity": current_identity,
        "selected_names": selected, "selected_specs": {k: models[v] for k, v in selected.items()},
        "tuning_scores_sha256": tuning_hash, "fitted_counts_sha256": fitted_hash,
        "elapsed_seconds": time.monotonic()-started, "frozen_utc": datetime.now(timezone.utc).isoformat()})
    print(json.dumps({k: {"model": v, "tuning": summary[v]["pooled"]} for k, v in selected.items()}, indent=2), flush=True)


def final_test(c):
    started = time.monotonic()
    frozen_path = output_path("m3/frozen_model.json")
    frozen = json.loads(frozen_path.read_text())
    if frozen["identity"] != identity(c):
        raise ValueError("frozen model identity drift before final test")
    for file, key in (("fitted_counts.json", "fitted_counts_sha256"), ("tuning_scores.json", "tuning_scores_sha256")):
        if sha256(output_path(f"m3/{file}")) != frozen[key]:
            raise ValueError("fitted/tuned snapshot drift")
    result_path = output_path("m3/test_results.json")
    if result_path.exists():
        previous = json.loads(result_path.read_text())
        if previous["frozen_model_sha256"] != sha256(frozen_path):
            raise ValueError("test result model drift")
        print("Verified frozen final test", flush=True)
        return
    # The final cohort is opened only after immutable selection checks above.
    games = inputs(c, ("test",))["test"]
    fitted = json.loads(output_path("m3/fitted_counts.json").read_text())
    summary, rows = score(fitted, games, frozen["selected_specs"], c)
    candidate, baseline = "shared_position", "tuned_position"
    paired = paired_bootstrap(rows, candidate, baseline, c["bootstrap_replicates"], c["bootstrap_seed"])
    differences = {group: {metric: summary[candidate][group][metric]-summary[baseline][group][metric]
        for metric in ("log_loss", "brier")} for group in summary[candidate]}
    passes = (paired["percentile_95"][1] < 0 and
        all(differences[f"event:{e}"]["log_loss"] < 0 for e in c["events"]) and
        differences["support:unseen"]["log_loss"] < 0)
    row_hash = sha256(write_json("m3/test_game_scores.json", rows))
    result = {"frozen_model_sha256": sha256(frozen_path), "summary": summary,
        "paired_difference": differences, "paired_bootstrap": paired,
        "progression_rule_passed": passes, "test_game_scores_sha256": row_hash,
        "elapsed_seconds": time.monotonic()-started, "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m3/test_results.json", result)
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--tune", action="store_true")
    action.add_argument("--test", action="store_true")
    args = parser.parse_args()
    c = json.loads(CONFIG.read_text())
    (tune if args.tune else final_test)(c)


if __name__ == "__main__":
    main()
