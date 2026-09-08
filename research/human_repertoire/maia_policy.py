"""Maia-2 legal policy adapter; user-selected 1400 fallback, no search or training.

Heavy dependencies are lazy so the ordinary research test environment stays
unchanged. Caller supplies opponent rating; it is not silently lowered to 1400.
"""
import importlib.metadata
import json
import math

import chess

from .io import ROOT, output_path, sha256

CONFIG = ROOT/'docs/human_repertoire/maia_config.json'


def model_type(event):
    if event not in ('Blitz', 'Rapid', 'Classical'):
        raise ValueError('unsupported Maia time-control category')
    return 'blitz' if event == 'Blitz' else 'rapid'


def validate_probabilities(board, probabilities):
    legal = {m.uci() for m in board.legal_moves}
    if set(probabilities) != legal:
        raise ValueError('Maia move support differs from legal moves')
    if any(not math.isfinite(p) or p <= 0 for p in probabilities.values()):
        raise ValueError('invalid or numerically zero Maia probability')
    if abs(math.fsum(probabilities.values())-1) > 1e-10:
        raise ValueError('Maia probability mass is not one')


class MaiaPolicy:
    def __init__(self, config=None):
        self.config = config or json.loads(CONFIG.read_text())
        if importlib.metadata.version('maia2') != self.config['maia2_version']:
            raise ValueError('Maia package version mismatch')
        import torch
        from maia2 import inference
        if torch.__version__ != self.config['torch_version']:
            raise ValueError('PyTorch build mismatch')
        self.torch, self.inference = torch, inference
        torch.set_num_threads(self.config['threads'])
        self.prepared = inference.prepare()
        self.models = {}

    def load(self, event):
        kind = model_type(event)
        if kind in self.models:
            return self.models[kind]
        from maia2 import model
        path = output_path('maia2/models/'+kind+'_model.pt')
        if not path.exists() or sha256(path) != self.config['checkpoint_sha256'][kind]:
            raise ValueError('Missing or mismatched Maia checkpoint; run explicit download phase')
        loaded = model.from_pretrained(kind, device='cpu', save_root=str(path.parent))
        loaded.eval()
        self.models[kind] = loaded
        return loaded

    def probabilities(self, board, event, opponent_rating, self_rating=None):
        model_type(event)
        if not board.is_valid():
            raise ValueError('invalid chess board')
        if board.outcome(claim_draw=False) is not None:
            return {}
        rating = self.config['fallback_rating'] if self_rating is None else self_rating
        if not all(isinstance(r, int) and 0 <= r <= 4000 for r in (rating, opponent_rating)):
            raise ValueError('ratings must be integer model inputs')
        torch = self.torch; moves, buckets, reverse = self.prepared
        encoded, own, opp, legal = self.inference.preprocessing(board.fen(), rating, opponent_rating, buckets, moves)
        with torch.inference_mode():
            logits, _, _ = self.load(event)(encoded.unsqueeze(0), torch.tensor([own]), torch.tensor([opp]))
            # The public display API rounds probabilities to four places.
            # Retain full legal-policy precision for likelihood and sampling.
            ps = logits[0].double().masked_fill(~legal.bool(), float('-inf')).softmax(-1)
        from maia2.utils import mirror_move
        probabilities = {}
        for index in legal.nonzero(as_tuple=True)[0].tolist():
            uci = reverse[index]
            if board.turn == chess.BLACK:
                uci = mirror_move(uci)
            probabilities[uci] = float(ps[index])
        validate_probabilities(board, probabilities)
        return dict(sorted(probabilities.items(), key=lambda item: board.san(chess.Move.from_uci(item[0]))))

    def sample(self, board, event, opponent_rating, u, self_rating=None):
        if not 0 <= u < 1:
            raise ValueError('sampling uniform outside [0,1)')
        probabilities = self.probabilities(board, event, opponent_rating, self_rating)
        cumulative = 0
        for move, p in probabilities.items():
            cumulative += p
            if u < cumulative:
                return move
        return next(reversed(probabilities)) if probabilities else None


class MaiaFallbackKernels:
    """Existing evaluator's get(key, fen) interface, with Maia on our turns only.

    Opponent behavior is explicitly delegated to the caller's existing kernel.
    This prevents a 1400 fallback preference from silently weakening opponents.
    Recall/book mixing continues to be handled by the original evaluator.
    """
    name = 'maia1400'

    def __init__(self, policy, event, our_color, opponent_rating, opponent_kernel, opponent_model, cap=20000):
        model_type(event)
        self.policy, self.event, self.our_color = policy, event, our_color
        self.opponent_rating, self.opponent_kernel, self.opponent_model = opponent_rating, opponent_kernel, opponent_model
        self.cap, self.cache = cap, {}

    def get(self, key, fen):
        from python.zobrist import zobrist_int64
        board = chess.Board(fen)
        if str(zobrist_int64(board)) != key:
            raise ValueError('Maia kernel position hash mismatch')
        if board.outcome(claim_draw=False) is not None:
            raise ValueError('terminal board has no fallback kernel')
        if key not in self.cache:
            if len(self.cache) >= self.cap:
                raise ValueError('Maia kernel position cap')
            if board.turn == self.our_color:
                ps = self.policy.probabilities(board, self.event, self.opponent_rating)
                moves = {board.san(chess.Move.from_uci(uci)): uci for uci in ps}
                probabilities = {san: ps[uci] for san, uci in moves.items()}
            else:
                moves, all_probabilities = self.opponent_kernel.get(key, fen)
                probabilities = all_probabilities[self.opponent_model]
            self.cache[key] = (moves, {self.name: probabilities}, fen.split()[:4])
        moves, probabilities, core = self.cache[key]
        if core != fen.split()[:4]:
            raise ValueError('Maia kernel FEN drift')
        return moves, probabilities
