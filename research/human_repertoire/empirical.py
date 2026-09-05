"""Small empirical kernels, uncertainty accounting and validation diagnostics.

No source scans. Data come from frozen, legally replayed cohort snapshots.
Unknown legal moves retain a prior-probability aggregate frontier. This is an
observation boundary, not a chess move with a fabricated resulting board.
"""
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from math import log, sqrt

import chess
from python.zobrist import zobrist_int64
from .model import Edge, Graph, Metrics, Node, eligible


def stats(games):
    moves = defaultdict(Counter)
    outcomes = defaultdict(lambda: [0, 0, 0])
    endings = defaultdict(lambda: [0, 0, 0])
    fens = {}
    for g in games:
        for i, p in enumerate(g["positions"]):
            key = p["key"]
            fens[key] = p["fen"]
            for j, x in enumerate(g["wdl"]):
                outcomes[key][j] += x
            if i < len(g["moves"]):
                moves[key][g["moves"][i]] += 1
            elif g["observed_end"]:
                for j, x in enumerate(g["wdl"]):
                    endings[key][j] += x
    return {"moves": dict(moves), "outcomes": dict(outcomes), "endings": dict(endings), "fens": fens}


def wdl(counts, c):
    n, strength = sum(counts), c["wdl_prior_strength"]
    return tuple((x + strength*p)/(n+strength) for x, p in zip(counts, c["wdl_prior"]))


def rule_outcome(board):
    outcome = board.outcome(claim_draw=False)
    if outcome is None:
        return None
    return (0, 1, 0) if outcome.winner is None else ((1, 0, 0) if outcome.winner else (0, 0, 1))


def position_cp(key, fen, evals):
    board = chess.Board(fen)
    if board.is_checkmate():
        return -100000 if board.turn else 100000
    if board.is_stalemate() or board.is_insufficient_material():
        return 0
    return evals.get(key)


def candidates(train, root, reference, c):
    own = [k for k, fen in train["fens"].items() if chess.Board(fen).turn and sum(train["moves"].get(k, {}).values()) >= 20]
    own.sort(key=lambda k: (-sum(train["moves"][k].values()), k))
    positions = [root] + [k for k in own if k != root][:c["study_positions"]-1]
    allowed = {k: [m for m, _ in sorted(train["moves"].get(k, {}).items(), key=lambda item: (-item[1], item[0]))[:c["moves_per_position"]]] for k in positions}
    for k, m in reference.items():
        if k in allowed and m not in allowed[k]:
            board = chess.Board(train["fens"][k])
            board.parse_san(m)
            allowed[k].append(m)
    return allowed


def build_graph(s, train, evals, allowed, root, c):
    fens = dict(s["fens"])
    # Frozen study moves must exist even if absent in this cohort's games.
    for key, choices in allowed.items():
        fens.setdefault(key, train["fens"][key])
        for move in choices:
            board = chess.Board(fens[key])
            board.push_san(move)
            fens.setdefault(str(zobrist_int64(board)), board.fen())
    nodes, audit = {}, {"unobserved_prescriptions": [], "unobserved_study_positions": []}
    prior = c["move_prior_strength"]
    for key, fen in list(fens.items()):
        board = chess.Board(fen)
        cp = position_cp(key, fen, evals)
        exact = rule_outcome(board)
        move_counts = s["moves"].get(key, {})
        ending = s["endings"].get(key, [0, 0, 0])
        ending_n = sum(ending)
        counts = s["outcomes"].get(key, [0, 0, 0])
        prediction = wdl(counts, c)
        if exact:
            nodes[key] = Node("terminal", cp=cp, wdl=exact)
            continue
        if not move_counts:
            # Never invent a fallback policy at an entirely unsupported position.
            # A prepared card there is flagged and excluded from the estimable
            # book projection; traversal stops at this explicit boundary.
            if key in allowed:
                audit["unobserved_study_positions"].append(key)
            if ending_n and ending_n == sum(counts):
                nodes[key] = Node("human_terminal", cp=cp, wdl=tuple(x/ending_n for x in ending))
            else:
                nodes[key] = Node("frontier", cp=cp, wdl=prediction)
            continue
        legal = {board.san(m): m for m in board.legal_moves}
        explicit = sorted(set(move_counts) | set(allowed.get(key, [])))
        if not set(explicit) <= set(legal):
            raise ValueError("illegal empirical or prescribed move")
        total = sum(move_counts.values())
        edges = []
        for move in explicit:
            child_board = board.copy(stack=False)
            child_board.push(legal[move])
            child = str(zobrist_int64(child_board))
            if child not in fens:
                raise ValueError("missing replayed child")
            if move not in move_counts:
                audit["unobserved_prescriptions"].append({"position": key, "move": move})
            probability = (move_counts.get(move, 0) + prior/len(legal)) / (total + prior)
            edges.append(Edge(move, child, probability, position_cp(child, fens[child], evals)))
        missing = len(legal) - len(explicit)
        if missing:
            unknown = "unknown:" + key
            nodes[unknown] = Node("frontier", cp=None, wdl=tuple(c["wdl_prior"]))
            edges.append(Edge("<unobserved legal move>", unknown, prior*missing/len(legal)/(total+prior)))
        nodes[key] = Node("us" if board.turn else "opp", tuple(edges), cp, prediction,
                          end_probability=ending_n/(ending_n+total),
                          end_wdl=tuple(x/ending_n for x in ending) if ending_n else (0, 1, 0))
    if len(nodes) > c["max_graph_nodes"]:
        raise ValueError("empirical graph cap exceeded")
    g = Graph(root, nodes)
    g.validate()
    return g, audit


def combine(weighted):
    return Metrics(**{name: sum(weight*getattr(m, name) for weight, m in weighted) for name in Metrics.__dataclass_fields__})


def first_hit_bounds(m):
    # Conditional on the chosen empirical transition model. These only bound
    # unknown evals and unresolved frontier continuations, not sampling error,
    # confounding, engine error or probability-model misspecification.
    return {"advantage_lower": m.certain_advantage_mass,
            "advantage_upper": min(1, m.certain_advantage_mass+m.unresolved_first_hit_mass),
            "discounted_lower": m.certain_early_advantage,
            "discounted_upper": min(1, m.certain_early_advantage+m.unresolved_first_hit_mass)}


def audit_metrics(m):
    if abs(m.win+m.draw+m.loss-1) > 1e-9 or abs(m.advantage+m.disadvantage+m.no_hit-1) > 1e-9:
        raise AssertionError("outcome/hit mass not conserved")
    if abs(m.terminal_mass+m.frontier_mass+m.horizon_mass+m.repetition_mass+m.human_end_mass-1) > 1e-9:
        raise AssertionError("exit mass not conserved")
    return {**m.report(), "observation_bounds": first_hit_bounds(m)}


def observed_hits(games, evals, threshold=300):
    counts = Counter()
    for g in games:
        event, unknown = None, False
        for p in g["positions"]:
            cp = position_cp(p["key"], p["fen"], evals)
            if cp is None:
                unknown = True
            elif event is None and abs(cp) >= threshold:
                event = "advantage" if cp > 0 else "disadvantage"
        counts[event or "no_observed_hit"] += 1
        counts["unknown_eval_games"] += unknown
        counts["games"] += 1
    return dict(counts)


def move_diagnostics(train, games, c):
    rows = []
    for game in games:
        values = defaultdict(float)
        for i, move in enumerate(game["moves"]):
            position = game["positions"][i]
            nlegal = chess.Board(position["fen"]).legal_moves.count()
            frequencies = train["moves"].get(position["key"], {})
            n = sum(frequencies.values())
            alpha = c["move_prior_strength"]
            p = (frequencies.get(move, 0) + alpha/nlegal)/(n+alpha)
            norm2 = sum(((x+alpha/nlegal)/(n+alpha))**2 for x in frequencies.values()) + (nlegal-len(frequencies))*(alpha/nlegal/(n+alpha))**2
            values["log_loss"] -= log(p)
            values["uniform_log_loss"] += log(nlegal)
            values["brier"] += 1-2*p+norm2
            values["uniform_brier"] += 1-1/nlegal
            values["unseen_position"] += n == 0
            values["unseen_move"] += move not in frequencies
            values["thin_position"] += n < 5
            values["decisions"] += 1
        if values["decisions"]:
            rows.append({"game_id": game["game_id"], "event": game["event"], "white": game["white"].casefold(),
                         "black": game["black"].casefold(), **dict(values)})
    def aggregate(items):
        n = sum(r["decisions"] for r in items)
        return {"games": len(items), "decisions": int(n), **{key: sum(r.get(key, 0) for r in items)/n if n else None for key in ("log_loss", "uniform_log_loss", "brier", "uniform_brier", "unseen_position", "unseen_move", "thin_position")}}
    return {"pooled": aggregate(rows), "by_event": {event: aggregate([r for r in rows if r["event"] == event]) for event in ("Blitz", "Rapid", "Classical")}}, rows


def outcome_diagnostics(train_games, validation_games, c):
    counts = defaultdict(lambda: [0, 0, 0])
    global_counts = [0, 0, 0]
    for game in train_games:
        move = game["moves"][0] if game["moves"] else "<end>"
        for i, x in enumerate(game["wdl"]):
            counts[move][i] += x
            global_counts[i] += x
    baseline = wdl(global_counts, c)
    rows = []
    for game in validation_games:
        move = game["moves"][0] if game["moves"] else "<end>"
        prediction = wdl(counts.get(move, [0, 0, 0]), c)
        actual = game["wdl"]
        predicted_points = prediction[0] + .5*prediction[1]
        actual_points = actual[0] + .5*actual[1]
        rows.append({"event": game["event"], "white": game["white"].casefold(), "black": game["black"].casefold(),
                     "game_id": game["game_id"], "root_move": move,
                     "brier": sum((p-x)**2 for p, x in zip(prediction, actual)),
                     "global_brier": sum((p-x)**2 for p, x in zip(baseline, actual)),
                     "points_error": predicted_points - actual_points,
                     "predicted_points": predicted_points, "actual_points": actual_points})
    def aggregate(items):
        return {"games": len(items), **{k: sum(r[k] for r in items)/len(items) if items else None for k in ("brier", "global_brier", "points_error", "predicted_points", "actual_points")}}
    return {"pooled": aggregate(rows), "by_event": {e: aggregate([r for r in rows if r["event"] == e]) for e in ("Blitz", "Rapid", "Classical")}, "by_root_move": {m: aggregate([r for r in rows if r["root_move"] == m]) for m in sorted({r["root_move"] for r in rows})}}, rows


def player_component_bootstrap(rows, field, seed=20240905, draws=500):
    """Descriptive interval conditional on this frame, clustering shared players.

    Connected components ensure a shared player never spans resampling units.
    One giant component can make this uninformative; report it, do not silently
    replace it by a convenient independence assumption.
    """
    import random
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for r in rows:
        a, b = find(r["white"]), find(r["black"])
        parent[a] = b
    groups = defaultdict(list)
    for r in rows:
        groups[find(r["white"])].append(r[field])
    blocks = list(groups.values())
    rng = random.Random(seed)
    values = []
    for _ in range(draws):
        sample = [rng.choice(blocks) for _ in blocks]
        values.append(sum(map(sum, sample))/sum(map(len, sample)))
    values.sort()
    return {"estimate": sum(r[field] for r in rows)/len(rows), "percentile_95": [values[int(.025*draws)], values[int(.975*draws)]],
            "components": len(blocks), "largest_component_games": max(map(len, blocks)), "games": len(rows),
            "draws": draws, "seed": seed, "scope": "descriptive frame-conditional shared-player-component bootstrap; not population or causal uncertainty"}
