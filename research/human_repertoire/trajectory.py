"""Paired legal-trajectory sampling, shared scoring, and explicit MC error.

No file reads or engine searches occur in this module. Backends supply either a
frozen finite graph or on-demand legal chess positions under frozen move priors.
"""
from dataclasses import dataclass
import hashlib
from math import log, sqrt

import chess
from python.zobrist import zobrist_int64

from .common_graph import Kernels
from .empirical import rule_outcome, wdl
from .model import Edge, Metrics, Node


def uniform(seed, index, ply, channel):
    data = hashlib.sha256(f"{seed}:{index}:{ply}:{channel}".encode()).digest()
    return (int.from_bytes(data[:8], "big") >> 11) / (1 << 53)


def categorical(edges, u):
    if not 0 <= u < 1 or not edges:
        raise ValueError("invalid categorical draw")
    cumulative = 0.0
    for edge in edges:
        cumulative += edge.probability
        if u < cumulative:
            return edge
    if abs(cumulative-1) > 1e-10:
        raise ValueError("move mass not conserved")
    return edges[-1]


@dataclass(frozen=True)
class View:
    key: str
    node: Node
    exact_cp: float | None = None


class GraphBackend:
    def __init__(self, graph):
        graph.validate()
        self.graph = graph
        self.initial = graph.root

    def view(self, state, model):
        return View(state, self.graph.nodes[state])

    def advance(self, state, edge):
        return edge.child


class ChessBackend:
    def __init__(self, root_fen, fitted, specs, train, c, max_positions):
        self.initial = chess.Board(root_fen)
        self.kernels = Kernels(fitted, specs, max_positions)
        self.train, self.c, self.max_positions = train, c, max_positions
        self.fens = {}

    def view(self, board, model):
        key = str(zobrist_int64(board))
        if key not in self.fens:
            if len(self.fens) >= self.max_positions:
                raise ValueError("visited-position cap exceeded")
            self.fens[key] = board.fen()
        elif self.fens[key].split()[:4] != board.fen().split()[:4]:
            raise ValueError("position hash/FEN conflict")
        exact = rule_outcome(board)
        if exact is not None:
            cp = (-100000 if board.turn else 100000) if board.is_checkmate() else 0
            return View(key, Node("terminal", wdl=exact), cp)
        ending = self.train["endings"].get(key, (0, 0, 0))
        end_n = sum(ending)
        continued = sum(self.train["moves"].get(key, {}).values())
        prediction = wdl(self.train["outcomes"].get(key, (0, 0, 0)), self.c)
        if end_n and not continued:
            return View(key, Node("human_terminal", wdl=tuple(x/end_n for x in ending)))
        moves, probabilities = self.kernels.get(key, board.fen())
        edges = tuple(Edge(san, moves[san], probabilities[model][san]) for san in sorted(moves))
        return View(key, Node("us" if board.turn else "opp", edges, wdl=prediction,
            end_probability=end_n/(end_n+continued) if end_n+continued else 0,
            end_wdl=tuple(x/end_n for x in ending) if end_n else (0, 1, 0)))

    def advance(self, board, edge):
        child = board.copy(stack=False)
        child.push_uci(edge.child)
        return child


def sample_trace(backend, model, book, cfg, seed, index):
    state, keys, moves, active = backend.initial, [], [], True
    if cfg.fallback or cfg.opponent:
        raise ValueError("sampling uses the backend's complete frozen kernels")
    for ply in range(cfg.horizon+1):
        view = backend.view(state, model)
        key, node = view.key, view.node
        repeated = cfg.repetition_visits is not None and keys.count(key)+1 >= cfg.repetition_visits
        keys.append(key)
        reason = ("terminal" if node.side == "terminal" else "repetition" if repeated else
                  "human_end" if node.side == "human_terminal" else
                  "frontier" if node.side == "frontier" else "horizon" if ply == cfg.horizon else None)
        ending = (0, 1, 0) if reason == "repetition" else node.wdl
        if not reason and uniform(seed, index, ply, "end") < node.end_probability:
            reason, ending = "human_end", node.end_wdl
        if reason:
            return {"index": index, "keys": keys, "moves": moves, "reason": reason,
                    "wdl": list(ending), "exact_cp": view.exact_cp}
        edges = sorted(node.edges, key=lambda e: e.move)
        if key in book and node.side == "us" and (active or cfg.reentry):
            recall = cfg.recall_by_position.get(key, cfg.recall)
        else:
            recall = 0
        if uniform(seed, index, ply, "recall") < recall:
            choices = [e for e in edges if e.move == book[key]]
            if len(choices) != 1:
                raise ValueError("prescribed move absent from legal support")
            edge, active = choices[0], True
        else:
            edge = categorical(edges, uniform(seed, index, ply, "move"))
            if node.side == "us":
                active = cfg.reentry
        moves.append(edge.move)
        state = backend.advance(state, edge)
    raise AssertionError("trajectory exceeded horizon")


def score_trace(trace, evals, cfg):
    event, hit_ply, unknown, unknown_before = 0, 0, False, False
    for ply, key in enumerate(trace["keys"]):
        cp = trace["exact_cp"] if ply == len(trace["keys"])-1 and trace.get("exact_cp") is not None else evals.get(key)
        unknown |= cp is None
        unknown_before |= event == 0 and cp is None
        if not event and cp is not None and abs(cp) >= cfg.threshold_cp:
            event, hit_ply = (1 if cp > 0 else -1), ply
    reason = trace["reason"]
    early_cut = reason == "frontier" and len(trace["keys"])-1 < cfg.horizon
    w, d, l = trace["wdl"]
    m = Metrics(win=w, draw=d, loss=l, advantage=float(event == 1), disadvantage=float(event == -1),
        no_hit=float(event == 0), early_advantage=cfg.discount**hit_ply if event == 1 else 0,
        win_after_advantage=w if event == 1 else 0, terminal_mass=float(reason == "terminal"),
        frontier_mass=float(reason == "frontier"), horizon_mass=float(reason == "horizon"),
        repetition_mass=float(reason == "repetition"), unknown_eval_mass=float(unknown), human_end_mass=float(reason == "human_end"),
        frontier_before_horizon_mass=float(early_cut), unresolved_no_hit_mass=float(early_cut and not event),
        certain_advantage_mass=float(event == 1 and not unknown_before),
        certain_early_advantage=cfg.discount**hit_ply if event == 1 and not unknown_before else 0,
        unresolved_first_hit_mass=float(unknown_before or (early_cut and not event)))
    return m.report()


def mean_se(values):
    n = len(values)
    if n < 2:
        raise ValueError("at least two simulation replicates required")
    mean = sum(values)/n
    variance = sum((x-mean)**2 for x in values)/(n-1)
    return {"mean": mean, "mc_se": sqrt(variance/n)}


def observation_endpoints(row, discounted=False):
    lower = row["certain_early_advantage" if discounted else "certain_advantage_mass"]
    return lower, min(1.0, lower+row["unresolved_first_hit_mass"])


def arm_summary(rows, alpha=.05):
    n = len(rows)
    result = {name: mean_se([r[name] for r in rows]) for name in (*Metrics.__dataclass_fields__, "points")}
    eps = sqrt(log(2/alpha)/(2*n))
    for label, discounted in (("first_advantage", False), ("discounted_first_advantage", True)):
        endpoints = [observation_endpoints(r, discounted) for r in rows]
        lo, hi = [sum(e[i] for e in endpoints)/n for i in (0, 1)]
        result[label] = {"sample_observation_interval": [lo, hi],
            "mc_outer_95": [max(0, lo-eps), min(1, hi+eps)], "hoeffding_margin": eps}
    return {"trajectories": n, "metrics": result}


def paired_summary(a_rows, b_rows, a_traces, b_traces, alpha=.05):
    n = len(a_rows)
    if not n == len(b_rows) == len(a_traces) == len(b_traces):
        raise ValueError("unpaired simulation counts")
    if any(a["index"] != b["index"] for a, b in zip(a_traces, b_traces)):
        raise ValueError("unpaired simulation indices")
    out = {"identical_position_sequences": sum(a["keys"] == b["keys"] for a, b in zip(a_traces, b_traces))}
    for label, discounted in (("first_advantage", False), ("discounted_first_advantage", True)):
        lower, upper = [], []
        for a, b, ta, tb in zip(a_rows, b_rows, a_traces, b_traces):
            la, ua = observation_endpoints(a, discounted)
            lb, ub = observation_endpoints(b, discounted)
            same = ta["keys"] == tb["keys"] and ta.get("exact_cp") == tb.get("exact_cp")
            lower.append(0 if same else la-ub)
            upper.append(0 if same else ua-lb)
        eps = 2*sqrt(log(2/alpha)/(2*n))
        lo, hi = sum(lower)/n, sum(upper)/n
        out[label] = {"sample_difference_interval": [lo, hi],
            "mc_outer_95": [max(-1, lo-eps), min(1, hi+eps)], "hoeffding_margin": eps}
    for name in ("early_advantage", "points"):
        a, b = [r[name] for r in a_rows], [r[name] for r in b_rows]
        out[name] = {**mean_se([x-y for x, y in zip(a, b)]),
            "independence_assumed_se": sqrt(mean_se(a)["mc_se"]**2+mean_se(b)["mc_se"]**2)}
    return out
