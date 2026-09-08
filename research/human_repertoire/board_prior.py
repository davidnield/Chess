"""Small engine-free geometric prior correction; no existing model is modified."""
from collections import Counter, defaultdict
from math import log
import time

import chess
import numpy as np

from .cohort import rank_id
from .shared_fallback import distribution, move_token

FEATURES = ("check", "castle", "captured_value", "pawn_attacks_mover", "cheaper_attacks_mover",
            "mover_hanging", "mover_defended", "promotion", "mover_was_attacked", "max_friendly_hanging")
VALUES = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9, chess.KING: 100}


def features(board, move):
    color = board.turn
    captured = 1 if board.is_en_passant(move) else VALUES[board.piece_type_at(move.to_square)] if board.piece_at(move.to_square) else 0
    castle, before = board.is_castling(move), bool(board.attackers(not color, move.from_square))
    board.push(move)
    try:
        enemy = board.attackers(not color, move.to_square)
        defended = bool(board.attackers(color, move.to_square))
        value = VALUES[board.piece_type_at(move.to_square)]
        hanging = max([0]+[VALUES[p.piece_type] for square, p in board.piece_map().items()
            if p.color == color and p.piece_type != chess.KING and board.is_attacked_by(not color, square) and not board.is_attacked_by(color, square)])
        return np.array([board.is_check(), castle, captured/9,
            any(board.piece_type_at(s) == chess.PAWN for s in enemy),
            any(VALUES[board.piece_type_at(s)] < value for s in enemy),
            bool(enemy) and not defended, defended, bool(move.promotion), before, hanging/9], dtype=np.float64)
    finally:
        board.pop()


class BoardPrior:
    def __init__(self, fitted, setting, c):
        self.fitted, self.setting, self.c, self.cache = fitted, setting, c, {}

    def get(self, key, fen):
        if key not in self.cache:
            if len(self.cache) >= self.c["feature_position_cap"]:
                raise ValueError("feature position cap")
            board = chess.Board(fen)
            moves = sorted(board.legal_moves, key=lambda m: board.san(m))
            sans, ucis = [board.san(m) for m in moves], [m.uci() for m in moves]
            counts = self.fitted["positions"].get(key, {})
            q = distribution(ucis, [move_token(board, m) for m in moves], counts, self.fitted["tokens"], {**self.setting, "only_prior": True})
            self.cache[key] = {"sans": sans, "ucis": ucis, "x": np.stack([features(board, m) for m in moves]),
                "q": np.array(q), "counts": np.array([counts.get(m, 0) for m in ucis]), "core": fen.split()[:4]}
        row = self.cache[key]
        if row["core"] != fen.split()[:4]:
            raise ValueError("feature FEN drift")
        return row

    def probabilities(self, key, fen, weights, beta):
        row = self.get(key, fen)
        logits = np.log(row["q"])+beta*(row["x"]@weights)
        q = np.exp(logits-np.max(logits)); q /= q.sum()
        alpha = self.setting["alpha"]
        p = (row["counts"]+alpha*q)/(row["counts"].sum()+alpha)
        if not np.isfinite(p).all() or np.any(p <= 0) or abs(p.sum()-1) > 1e-10:
            raise ValueError("invalid board-aware probability")
        return row["sans"], p


def panel(games, c, split):
    out = []
    for event in ("Blitz", "Rapid", "Classical"):
        pool = sorted((g for g in games if g["event"] == event), key=lambda g: (rank_id(g["game_id"], c["development_seed"]+split), g["game_id"]))
        if len(pool) < c["development_games_per_event"]:
            raise ValueError("short development panel")
        out.extend(pool[:c["development_games_per_event"]])
    return out


def training_arrays(games, kernel, c):
    xs, qs, starts, chosen, offset = [], [], [], [], 0
    started = time.monotonic()
    for game in games:
        if time.monotonic()-started > c["phase_seconds"]:
            raise TimeoutError("feature training guard")
        for i, san in enumerate(game["moves"]):
            p = game["positions"][i]
            row = kernel.get(p["key"], p["fen"])
            starts.append(offset); chosen.append(offset+row["sans"].index(san))
            xs.append(row["x"]); qs.append(row["q"])
            offset += len(row["sans"])
            if offset > c["legal_row_cap"]:
                raise ValueError("training legal-row cap")
    return np.concatenate(xs), np.log(np.concatenate(qs)), np.array(starts), np.array(chosen)


def objective_gradient(weights, x, logq, starts, chosen, ridge):
    lengths = np.diff(np.r_[starts, len(logq)])
    logits = logq+x@weights
    maximum = np.maximum.reduceat(logits, starts)
    e = np.exp(logits-np.repeat(maximum, lengths))
    totals = np.add.reduceat(e, starts)
    probs = e/np.repeat(totals, lengths)
    loss = np.mean(maximum+np.log(totals)-logits[chosen])+ridge*float(weights@weights)/2
    residual = probs.copy(); residual[chosen] -= 1
    gradient = x.T@residual/len(chosen)+ridge*weights
    return float(loss), gradient


def fit_weights(arrays, c):
    weights = np.zeros(len(FEATURES)); history = []
    started = time.monotonic()
    for step in range(c["gradient_steps"]):
        if time.monotonic()-started > c["phase_seconds"]:
            raise TimeoutError("gradient runtime guard")
        loss, grad = objective_gradient(weights, *arrays, c["ridge"])
        history.append(loss)
        weights -= c["learning_rate"]/(1+step/20)**.5*grad
    return weights, history


def score(games, kernel, weights, strengths, c):
    sums = {name: defaultdict(Counter) for name in strengths}; rows = []
    started, legal_rows = time.monotonic(), 0
    for game in games:
        if time.monotonic()-started > c["phase_seconds"]:
            raise TimeoutError("board-prior scoring guard")
        per_game = {name: Counter() for name in strengths}
        for i, human in enumerate(game["moves"]):
            position = game["positions"][i]; key, fen = position["key"], position["fen"]
            support = "seen" if kernel.fitted["positions"].get(key) else "unseen"
            legal_rows += len(kernel.get(key, fen)["sans"])
            if legal_rows > c["legal_row_cap"]:
                raise ValueError("scoring legal-row cap")
            for name, beta in strengths.items():
                sans, p = kernel.probabilities(key, fen, weights, beta)
                q = float(p[sans.index(human)])
                values = {"decisions": 1, "log_loss": -log(q), "brier": float(1-2*q+p@p)}
                for group in ("pooled", "event:"+game["event"], "support:"+support):
                    sums[name][group].update(values)
                per_game[name].update(values)
        rows.append({"game_id": game["game_id"], "event": game["event"], "white": game["white"], "black": game["black"], "models": dict(per_game)})
    summary = {name: {group: {"decisions": values["decisions"], **{k: v/values["decisions"] for k, v in values.items() if k != "decisions"}}
                     for group, values in groups.items()} for name, groups in sums.items()}
    return summary, rows
