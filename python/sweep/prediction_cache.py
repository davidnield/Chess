"""Raw complete Maia vectors, independent of pool and downstream policy settings.

SQLite handles transactional persistence natively. Rust validates probabilities;
Python coordinates bounded model calls and serializes small request records.
"""
import json
import sqlite3
from pathlib import Path
from consumer_keys import stored_key
from .spec import canonical, digest

SCHEMA = 'maia-position-raw-v1'


class PredictionCache:
    def __init__(self, path, identity, *, batch_size=256):
        if not 1 <= batch_size <= 256:
            raise ValueError('raw prediction batch must be 1..256')
        self.identity = dict(schema=SCHEMA, inference=identity, batch_size=batch_size)
        self.namespace = digest(self.identity)
        self.batch_size = batch_size
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=60)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS raw_namespaces (id TEXT PRIMARY KEY, identity TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS raw_predictions (id TEXT PRIMARY KEY, request TEXT NOT NULL, value TEXT NOT NULL, sha256 TEXT NOT NULL)')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO raw_namespaces VALUES (?,?)', (self.namespace, canonical(self.identity)))
        if self.db.execute('SELECT identity FROM raw_namespaces WHERE id=?', (self.namespace,)).fetchone()[0] != canonical(self.identity):
            self.db.close()
            raise ValueError('raw prediction namespace differs')
        self.stats = dict(requested=0, hits=0, unique_inferred=0, committed_batches=0)

    def descriptor(self, board):
        if board.chess960 or not board.is_valid():
            raise ValueError('raw Maia requires a valid standard-chess position')
        legal = sorted(m.uci() for m in board.legal_moves)
        if not legal:
            raise ValueError('terminal board has no prediction')
        return dict(namespace=self.namespace, position=stored_key(board, True),
                    fen=' '.join(board.fen(en_passant='fen').split()[:4]), legal=legal)

    @staticmethod
    def validate(request, values):
        from book_builder_rs import prediction_temperature
        if not isinstance(values, list) or len(values) != len(request['legal']):
            raise ValueError('raw prediction legal vector differs')
        # Validate without changing the stored float32-softmax values.
        prediction_temperature(values, 1.)
        return dict(zip(request['legal'], values))

    def read(self, key, request):
        row = self.db.execute('SELECT request,value,sha256 FROM raw_predictions WHERE id=?', (key,)).fetchone()
        if row is None:
            return None
        if row[0] != canonical(request):
            raise ValueError('raw prediction request/key collision')
        values = json.loads(row[1])
        if row[2] != digest(dict(request=request, values=values)):
            raise ValueError('raw prediction content hash differs')
        return self.validate(request, values)

    def predict(self, boards, infer):
        descriptors = [self.descriptor(b) for b in boards]
        self.stats['requested'] += len(boards)
        unique = {}
        for b, d in zip(boards, descriptors):
            key = digest(d)
            if key in unique and unique[key][1] != d:
                raise ValueError('raw prediction request digest collision')
            unique.setdefault(key, (b, d))
        answers, pending = {}, []
        for key, (b, d) in unique.items():
            value = self.read(key, d)
            if value is None:
                pending.append((key, b, d))
            else:
                answers[key] = value
        self.stats['hits'] += sum(digest(d) in answers for d in descriptors)
        for offset in range(0, len(pending), self.batch_size):
            batch = pending[offset:offset+self.batch_size]
            predictions = infer([b for _, b, _ in batch])
            if len(predictions) != len(batch):
                raise ValueError('incomplete raw inference batch')
            prepared = []
            for (key, _, d), vector in zip(batch, predictions):
                if set(vector) != set(d['legal']):
                    raise ValueError('raw inference must include every legal move exactly once')
                values = [vector[u] for u in d['legal']]
                self.validate(d, values)
                prepared.append((key, canonical(d), canonical(values), digest(dict(request=d, values=values))))
            with self.db:
                self.db.executemany('INSERT OR IGNORE INTO raw_predictions VALUES (?,?,?,?)', prepared)
                # Always return the persistent winner, including concurrent-writer races.
                for key, _, d in batch:
                    answers[key] = self.read(key, d)
            self.stats['unique_inferred'] += len(batch)
            self.stats['committed_batches'] += 1
        return [dict(answers[digest(d)]) for d in descriptors]

    def close(self):
        self.db.close()
