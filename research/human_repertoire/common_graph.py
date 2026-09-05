"""Common graph adapter for frozen M3 priors; preserves omitted legal-move mass."""
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import replace

import chess
from python.zobrist import zobrist_int64

from .empirical import position_cp, rule_outcome, wdl
from .model import Edge, Graph, Node
from .shared_fallback import distribution, move_token


class Kernels:
    def __init__(self, fitted, specs, cap=20000):
        self.fitted, self.specs, self.cap = fitted, specs, cap
        self.cache = {}

    def get(self, key, fen):
        if key not in self.cache:
            if len(self.cache) >= self.cap:
                raise ValueError("legal-kernel cache cap exceeded")
            board = chess.Board(fen)
            legal = sorted(board.legal_moves, key=lambda m: m.uci())
            if not legal:
                raise ValueError("terminal position has no move kernel")
            sans = [board.san(m) for m in legal]
            moves = {s: m.uci() for s, m in zip(sans, legal)}
            tokens = [move_token(board, m) for m in legal]
            counts = self.fitted["positions"].get(key, {})
            probabilities = {name: dict(zip(sans, distribution(list(moves.values()), tokens,
                counts, self.fitted["tokens"], spec))) for name, spec in self.specs.items()}
            self.cache[key] = (moves, probabilities, " ".join(fen.split()[:4]))
        moves, probabilities, core = self.cache[key]
        if core != " ".join(fen.split()[:4]):
            raise ValueError("inconsistent FEN for shared position")
        return moves, probabilities


def add_edge(template, key, san):
    board = chess.Board(template["fens"][key])
    board.push_san(san)
    child = str(zobrist_int64(board))
    template["fens"].setdefault(child, board.fen())
    template["explicit"].setdefault(key, {})[san] = child
    return child


def base_template(train, root, books):
    template = {"root": root, "fens": dict(train["fens"]), "explicit": {}, "new_expanded": []}
    for key, moves in train["moves"].items():
        for san in sorted(moves):
            add_edge(template, key, san)
    for book in books.values():
        for key, san in book.items():
            add_edge(template, key, san)
    return template


def materialize(template, kernels, train, evals, name, c):
    nodes = {}
    for key, fen in template["fens"].items():
        board = chess.Board(fen)
        exact = rule_outcome(board)
        cp = position_cp(key, fen, evals)
        if exact is not None and cp is None:
            cp = 0  # non-checkmate automatic board draws
        prediction = wdl(train["outcomes"].get(key, (0, 0, 0)), c)
        ending = train["endings"].get(key, (0, 0, 0))
        end_n = sum(ending)
        total = sum(train["moves"].get(key, {}).values())
        if exact is not None:
            nodes[key] = Node("terminal", cp=cp, wdl=exact)
        elif end_n and not total and end_n == sum(train["outcomes"].get(key, (0, 0, 0))):
            nodes[key] = Node("human_terminal", cp=cp, wdl=tuple(x/end_n for x in ending))
        elif not template["explicit"].get(key):
            nodes[key] = Node("frontier", cp=cp, wdl=prediction)
        else:
            _, all_probs = kernels.get(key, fen)
            probs = all_probs[name]
            explicit = template["explicit"][key]
            if not set(explicit) <= set(probs):
                raise ValueError("explicit move outside legal kernel")
            edges = [Edge(san, child, probs[san], position_cp(child, template["fens"][child], evals))
                     for san, child in sorted(explicit.items())]
            omitted = set(probs)-set(explicit)
            if omitted:
                unknown = "unknown:"+key
                nodes[unknown] = Node("frontier", cp=None, wdl=tuple(c["wdl_prior"]))
                edges.append(Edge("<unobserved legal move>", unknown, sum(probs[m] for m in sorted(omitted))))
            nodes[key] = Node("us" if board.turn else "opp", tuple(edges), cp, prediction,
                end_probability=end_n/(end_n+total) if end_n+total else 0,
                end_wdl=tuple(x/end_n for x in ending) if end_n else (0, 1, 0))
    if len(nodes) > c["max_graph_nodes"]:
        raise ValueError("common graph node cap exceeded")
    graph = Graph(template["root"], nodes)
    graph.validate()
    return graph


def forward_visits(graph, book, horizon, recall):
    """Allocation heuristic: finite-horizon expected visits, ignoring repetition.

    Prescriptions are used without engine gates solely to allocate audit work.
    Returns arrival and pre-move continuation mass, not first-hit probabilities.
    """
    current = {graph.root: 1.0}
    arrivals, departures = Counter(), Counter()
    for ply in range(horizon+1):
        following = defaultdict(float)
        for key, mass in current.items():
            arrivals[key] += mass
            node = graph.nodes[key]
            if ply == horizon or node.side not in {"us", "opp"}:
                continue
            live = mass*(1-node.end_probability)
            departures[key] += live
            r = recall if node.side == "us" and key in book else 0
            if r and book[key] not in {e.move for e in node.edges}:
                raise ValueError("allocation prescription missing from graph")
            for edge in node.edges:
                p = (1-r)*edge.probability + r*(edge.move == book.get(key))
                following[edge.child] += live*p
        current = following
    return arrivals, departures


def missing_edge_ranking(template, kernels, graphs, books, c):
    scores = {}
    for name, graph in graphs.items():
        for book in books.values():
            _, departures = forward_visits(graph, book, c["config"]["horizon"], c["config"]["recall"])
            for key, live in departures.items():
                if live <= 0:
                    continue
                _, all_probs = kernels.get(key, template["fens"][key])
                probs = all_probs[name]
                r = c["config"]["recall"] if graph.nodes[key].side == "us" and key in book else 0
                for san in set(probs)-set(template["explicit"].get(key, {})):
                    priority = live*((1-r)*probs[san]+r*(san == book.get(key)))
                    pair = key, san
                    scores[pair] = max(scores.get(pair, 0), priority)
    return [{"position": key, "move": san, "priority": priority}
            for (key, san), priority in sorted(scores.items(), key=lambda item: (-item[1], item[0])) if priority > 0]


def expand_template(base, seeds, kernels, train, c):
    template = deepcopy(base)
    added = set()
    def expand(key, depth):
        if not depth or train["moves"].get(key):
            return
        board = chess.Board(template["fens"][key])
        if rule_outcome(board) is not None or train["endings"].get(key):
            return
        if key not in added:
            if len(added) >= c["max_new_expanded_positions"]:
                raise ValueError("new expansion cap exceeded")
            added.add(key)
        _, all_probs = kernels.get(key, template["fens"][key])
        moves = set()
        for probs in all_probs.values():
            moves.update(sorted(probs, key=lambda m: (-probs[m], m))[:c["moves_per_model"]])
        for san in sorted(moves):
            child = add_edge(template, key, san)
            expand(child, depth-1)
        if len(template["fens"]) > c["max_graph_nodes"]:
            raise ValueError("new position cap exceeded")
    for seed in seeds:
        child = add_edge(template, seed["position"], seed["move"])
        expand(child, c["extra_plies"])
    template["new_expanded"] = sorted(added)
    template["seeds"] = seeds
    return template


def position_priorities(graphs, books, c):
    scores = Counter()
    for graph in graphs.values():
        for book in books.values():
            arrivals, _ = forward_visits(graph, book, c["config"]["horizon"], c["config"]["recall"])
            for key, priority in arrivals.items():
                if not key.startswith("unknown:"):
                    scores[key] = max(scores[key], priority)
    return scores


def select_engine_targets(fens, scores, source, mandatory, count, missing_quota, deeper_count):
    ranked = sorted((k for k in fens if scores.get(k, 0) > 0 and rule_outcome(chess.Board(fens[k])) is None),
                    key=lambda k: (-scores[k], k))
    chosen = sorted(mandatory, key=lambda k: (-scores.get(k, 0), k))
    if len(chosen) > count:
        raise ValueError("engine quota cannot cover every gate")
    missing = [k for k in ranked if k not in chosen and source.get(k) is None][:missing_quota]
    chosen.extend(missing[:count-len(chosen)])
    known = [k for k in ranked if k not in chosen and source.get(k) is not None]
    chosen.extend(known[:count-len(chosen)])
    remaining = [k for k in ranked if k not in chosen]
    chosen.extend(remaining[:count-len(chosen)])
    deeper = sorted(mandatory, key=lambda k: (-scores.get(k, 0), k))
    deeper.extend([k for k in ranked if k in chosen and k not in deeper][:max(0, deeper_count-len(deeper))])
    if len(deeper) > deeper_count:
        raise ValueError("deeper quota cannot cover every gate")
    return {"targets": chosen, "deeper": deeper, "mandatory": sorted(mandatory),
            "missing_targets": sum(source.get(k) is None for k in chosen),
            "priorities": {k: scores.get(k, 0) for k in chosen}}


def apply_evals(graph, template, evals):
    """Keep topology/probabilities fixed while changing only evaluation evidence."""
    def cp(key):
        if key.startswith("unknown:"):
            return None
        value = position_cp(key, template["fens"][key], evals)
        if value is None and graph.nodes[key].side == "terminal":
            return 0
        return value
    return Graph(graph.root, {key: replace(node, cp=cp(key), edges=tuple(
        replace(edge, sound_cp=cp(edge.child)) for edge in node.edges)) for key, node in graph.nodes.items()})
