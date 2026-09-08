"""Score-independent case sampling and paired engine-loss diagnostics for M7."""
from collections import Counter, defaultdict
import hashlib
import json
import random
import time

import chess
import numpy as np
from python.zobrist import zobrist_int64

from .common_graph import Kernels
from .model import Edge
from .trajectory import categorical, uniform

METRICS = ("loss_primary", "loss_secondary", "capped_loss_cp")


def rank(seed, *parts):
    return hashlib.sha256(json.dumps([seed, *parts], separators=(",", ":")).encode()).hexdigest()


def select_cases(games, fitted, c):
    if len(games) > c["max_games"] or len({g["game_id"] for g in games}) != len(games):
        raise ValueError("game cap or duplicate game")
    strata = [event+"/"+support for event in c["events"] for support in c["supports"]]
    buckets, frame = {s: [] for s in strata}, {s: {"eligible_games": 0, "eligible_decisions": 0} for s in strata}
    decisions, started = 0, time.monotonic()
    for game in games:
        if time.monotonic()-started > c["prepare_runtime_seconds"]:
            raise TimeoutError("case frame runtime guard")
        if game["event"] not in c["events"] or not game["utc_date"].startswith(c["test_month"]):
            raise ValueError("unexpected event or held-out period")
        if min(game["white_elo"], game["black_elo"]) < c["min_rating"] or abs(game["white_elo"]-game["black_elo"]) > c["max_rating_gap"]:
            raise ValueError("case frame rating mismatch")
        choices = defaultdict(list)
        for ply, san in enumerate(game["moves"]):
            decisions += 1
            if decisions > c["max_decisions"]:
                raise ValueError("decision cap exceeded")
            p = game["positions"][ply]
            n = sum(fitted["positions"].get(p["key"], {}).values())
            support = "seen" if n else "unseen"
            choices[support].append((rank(c["within_game_seed"], game["game_id"], ply), ply, san, n))
        for support, eligible in choices.items():
            stratum = game["event"]+"/"+support
            _, ply, san, n = min(eligible)
            p = game["positions"][ply]
            case = {"case_id": game["game_id"]+":"+str(ply), "game_id": game["game_id"],
                "event": game["event"], "support": support, "stratum": stratum,
                "ply": ply, "parent": p["key"], "fen": p["fen"], "human_san": san,
                "training_move_count": n, "within_game_eligible_decisions": len(eligible),
                "selection_rank": rank(c["selection_seed"], stratum, game["game_id"]),
                **{k: game[k] for k in ("white", "black", "white_elo", "black_elo", "utc_date", "time_control")}}
            buckets[stratum].append(case)
            frame[stratum]["eligible_games"] += 1
            frame[stratum]["eligible_decisions"] += len(eligible)
    selected = []
    for stratum in strata:
        pool = sorted(buckets[stratum], key=lambda x: (x["selection_rank"], x["case_id"]))
        if len(pool) < c["cases_per_stratum"]:
            raise ValueError("short stratum; no replacement design")
        for index, case in enumerate(pool[:c["cases_per_stratum"]]):
            selected.append({**case, "stratum_index": index, "deeper": index < c["deeper_cases_per_stratum"],
                "case_inclusion_probability": c["cases_per_stratum"]/len(pool)/case["within_game_eligible_decisions"]})
    if len({r["case_id"] for r in selected}) != len(selected):
        raise ValueError("duplicate case identifier")
    return selected, {"games": len(games), "decisions": decisions, "strata": frame}


def propose(cases, fitted, specs, c):
    kernels, fens, out = Kernels(fitted, specs, c["max_base_positions"]), {}, []
    def register(board):
        key = str(zobrist_int64(board))
        if key in fens and fens[key].split()[:4] != board.fen().split()[:4]:
            raise ValueError("case position hash/FEN mismatch")
        fens.setdefault(key, board.fen())
        if len(fens) > c["max_base_positions"]:
            raise ValueError("position cap exceeded; cases retained")
        return key
    for case in cases:
        board = chess.Board(case["fen"])
        if register(board) != case["parent"] or board.outcome(claim_draw=False) is not None:
            raise ValueError("invalid recorded decision")
        moves, probabilities = kernels.get(case["parent"], case["fen"])
        if case["human_san"] not in moves:
            raise ValueError("recorded human move is illegal")
        edges = [Edge(san, moves[san], probabilities[c["model"]][san]) for san in sorted(moves)]
        sampled = categorical(edges, uniform(c["draw_seed"], case["case_id"], 0, "move")).move
        children = {}
        for role, san in (("human", case["human_san"]), ("model", sampled)):
            child = board.copy(stack=False)
            child.push_san(san)
            children[role+"_child"] = register(child)
        out.append({**case, **children, "model_san": sampled, "white_to_move": board.turn,
            "human_move_probability": probabilities[c["model"]][case["human_san"]],
            "model_move_probability": probabilities[c["model"]][sampled]})
    return out, fens


def exact_terminal_cp(fen):
    board = chess.Board(fen)
    if board.outcome(claim_draw=False) is None:
        return None
    return (-100000 if board.turn else 100000) if board.is_checkmate() else 0


def targets(cases, fens, records, c):
    out = {}
    for phase in ("base", "deeper"):
        subset = [r for r in cases if phase == "base" or r["deeper"]]
        ordered = list(dict.fromkeys(r[k] for r in subset for k in ("parent", "human_child", "model_child")))
        if len(ordered) > c["max_"+phase+"_positions"]:
            raise ValueError("engine position cap exceeded")
        exact = {k: exact_terminal_cp(fens[k]) for k in ordered if exact_terminal_cp(fens[k]) is not None}
        required = [k for k in ordered if k not in exact]
        reused = [k for k in required if k in records[phase]]
        for key in reused:
            old, new = chess.Board(records[phase][key]["fen"]), chess.Board(fens[key])
            if old.fen().split()[:4] != new.fen().split()[:4] or max(old.halfmove_clock, new.halfmove_clock)+12 >= 100:
                raise ValueError("cache FEN/counter mismatch")
        out[phase] = {"cases": len(subset), "required": required, "reused": reused,
                      "targets": [k for k in required if k not in records[phase]], "exact": exact}
    return out


def loss_rows(cases, scores, c):
    rows = []
    for case in cases:
        keys = [case[k] for k in ("parent", "human_child", "model_child")]
        if any(scores.get(k) is None for k in keys):
            raise ValueError("incomplete paired engine evidence")
        parent, human, model = [scores[k] for k in keys]
        row = {k: case[k] for k in ("case_id", "game_id", "white", "black", "stratum", "event", "support")}
        row.update(parent_cp=parent, human_cp=human, model_cp=model,
                   same_move=case["human_san"] == case["model_san"], mate_involved=any(abs(v) >= 90000 for v in (parent, human, model)))
        for role, child in (("human", human), ("model", model)):
            signed = (parent-child)*(1 if case["white_to_move"] else -1)
            loss = max(0, signed)
            row.update({role+"_signed_loss_cp": signed, role+"_loss_cp": loss,
                role+"_loss_primary": int(loss >= c["primary_loss_cp"]),
                role+"_loss_secondary": int(loss >= c["secondary_loss_cp"]),
                role+"_capped_loss_cp": min(c["mean_loss_cap_cp"], loss)})
        rows.append(row)
    return rows


def components(rows):
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for row in rows:
        a, b = find(row["white"].casefold()), find(row["black"].casefold())
        parent[max(a, b)] = min(a, b)
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        groups[find(row["white"].casefold())].append(i)
    return [groups[k] for k in sorted(groups)]


def summaries(rows, c, bootstrap=True):
    strata = sorted({r["stratum"] for r in rows})
    memberships = {"balanced": strata}
    for field in ("event", "support", "stratum"):
        for value in sorted({r[field] for r in rows}):
            memberships[field+":"+value] = sorted({r["stratum"] for r in rows if r[field] == value})
    by_stratum = {s: [r for r in rows if r["stratum"] == s] for s in strata}
    groups = {}
    for name, group_strata in memberships.items():
        selected = [r for s in group_strata for r in by_stratum[s]]
        metrics = {}
        for metric in METRICS:
            means = {role: sum(sum(r[role+"_"+metric] for r in by_stratum[s])/len(by_stratum[s]) for s in group_strata)/len(group_strata)
                     for role in ("human", "model")}
            metrics[metric] = {**means, "difference": means["model"]-means["human"]}
        groups[name] = {"cases": len(selected), "strata": group_strata, "metrics": metrics,
            "same_move_fraction": sum(r["same_move"] for r in selected)/len(selected),
            "mate_cases": sum(r["mate_involved"] for r in selected),
            "human_negative_signed": sum(r["human_signed_loss_cp"] < 0 for r in selected),
            "model_negative_signed": sum(r["model_signed_loss_cp"] < 0 for r in selected),
            "human_over_cap": sum(r["human_loss_cp"] > c["mean_loss_cap_cp"] for r in selected),
            "model_over_cap": sum(r["model_loss_cp"] > c["mean_loss_cap_cp"] for r in selected)}
    if not bootstrap:
        return {"groups": groups}
    blocks = components(rows)
    rng = random.Random(c["bootstrap_seed"])
    samples = {name: {metric: [] for metric in METRICS} for name in groups}
    empty = Counter()
    for _ in range(c["bootstrap_draws"]):
        counts, totals = Counter(), {s: Counter() for s in strata}
        for block, weight in Counter(rng.choices(range(len(blocks)), k=len(blocks))).items():
            for i in blocks[block]:
                row, s = rows[i], rows[i]["stratum"]
                counts[s] += weight
                for metric in METRICS:
                    totals[s][metric] += weight*(row["model_"+metric]-row["human_"+metric])
        for name, group_strata in memberships.items():
            if any(counts[s] == 0 for s in group_strata):
                empty[name] += 1
                continue
            for metric in METRICS:
                samples[name][metric].append(sum(totals[s][metric]/counts[s] for s in group_strata)/len(group_strata))
    for name, group in groups.items():
        group["empty_stratum_draws"] = empty[name]
        for metric in METRICS:
            group["metrics"][metric]["component_percentile_95"] = None if empty[name] else np.quantile(samples[name][metric], [.025, .975]).tolist()
    return {"groups": groups, "bootstrap": {"components": len(blocks), "largest_component_cases": max(map(len, blocks)),
        "largest_component_games": max(len({rows[i]["game_id"] for i in b}) for b in blocks),
        "unique_games": len({r["game_id"] for r in rows}), "draws": c["bootstrap_draws"], "seed": c["bootstrap_seed"],
        "scope": "Approximate paired component resampling conditional on fitted model and selected diagnostic frame; no exact population coverage claim."}}
