"""Synthetic regression for forgotten moves, unaided recovery and book re-entry."""
import chess
import numpy as np

from experiments.score_repertoire_recall import play, ref


class NoObservations:
    def get(self, hashes):
        return [{} for _ in hashes]


class ScriptedPolicy:
    def __init__(self, opening):
        self.opening = opening

    def probs(self, epds, self_elo, opp_elo, keys):
        result = []
        for h, epd in zip(keys, epds):
            board = chess.Board(epd + " 0 1")
            move = self.opening.get(h) or board.san(min(board.legal_moves, key=lambda m: m.uci()))
            result.append({move: 1.})
        return result


def test_retrieval_and_reentry():
    board = chess.Board()
    opening = {}
    for san in ("e4", "e5", "Nf3", "Nc6"):
        opening[ref.zobrist_int64(board)] = san
        board.push_san(san)
    root, later = ref.zobrist_int64(chess.Board()), ref.zobrist_int64(board)
    arms = [dict(label="a", colour="white"), dict(label="b", colour="white")]
    # Both policies miss at the root, play the same unaided moves and then
    # recall Bc4, despite an intervening uncovered decision after 1...e5.
    profiles = {"a": {"trial": {root: ("d4", 0.), later: ("Bc4", 1.)}},
                "b": {"trial": {root: ("e4", 0.), later: ("Bc4", 1.)}}}
    counts = {True: NoObservations(), False: NoObservations()}
    result = play(arms, profiles, 0, 2, counts, ScriptedPolicy(opening), {},
                  lambda keys: np.zeros(len(keys), dtype="int16"), None,
                  ((0., .01), (0., .01)))
    assert result.height == 4
    for row in result.to_dicts():
        assert row["recalled"] == 1
        assert row["unaided_book_visits"] == 1
        assert row["found"] == int(row["label"] == "b")


if __name__ == "__main__":
    test_retrieval_and_reentry()
    print("Passed recall, unaided choice and re-entry checks.")
