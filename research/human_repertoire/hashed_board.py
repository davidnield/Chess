"""FEN serialization consistent with the repository's Polyglot position key."""
import chess


class HashedBoard(chess.Board):
    def fen(self, *, shredder=False, en_passant='xfen', promoted=None):
        # Polyglot includes an adjacent pawn's EP file even when the capture
        # would expose its king. Default legal-only FEN would lose that key.
        return super().fen(shredder=shredder, en_passant=en_passant, promoted=promoted)
