"""Merge repertoire card games into one PGN game with recursive variations.

python -B -m research.human_repertoire.nested_pgn
Publishes nested copies of builder_books/*.pgn into builder_books/nested/.
Original card exports and frozen experiment snapshots are left intact.
"""
import io
from pathlib import Path
import chess.pgn
from .io import ROOT


def games_from(text):
    stream=io.StringIO(text); games=[]
    while (game:=chess.pgn.read_game(stream)) is not None:
        if game.errors: raise ValueError(f'Invalid PGN: {game.errors}')
        games.append(game)
    if not games: raise ValueError('No games to merge')
    return games


def append_comment(existing, additional):
    if not additional or additional in existing: return existing
    return (existing+' '+additional).strip()


def merge_games(text):
    games=games_from(text); merged=chess.pgn.Game()
    merged.headers.clear(); merged.headers.update(games[0].headers)
    merged.headers.pop('CardPosition',None)
    merged.headers['Event']=games[0].headers.get('Event','Repertoire')+' — nested variations'
    merged.headers['Result']='*'
    origin=games[0].board().fen()

    def add(source, target):
        target.comment=append_comment(target.comment,source.comment)
        target.starting_comment=append_comment(target.starting_comment,source.starting_comment)
        target.nags.update(source.nags)
        for variation in source.variations:
            if not source.board().is_legal(variation.move): raise ValueError('Illegal source move')
            child=next((v for v in target.variations if v.move==variation.move),None)
            if child is None: child=target.add_variation(variation.move)
            add(variation,child)

    for game in games:
        if game.board().fen()!=origin: raise ValueError('Cannot merge different starting positions')
        add(game,merged)
    merged.comment=append_comment(merged.comment,
        'One repertoire tree. STUDY comments identify prescribed White moves; other White moves are context routes to study positions.')
    rendered=merged.accept(chess.pgn.StringExporter(headers=True,variations=True,comments=True,columns=100))+'\n'
    output=games_from(rendered)
    if len(output)!=1: raise AssertionError('Expected exactly one output game')

    def paths(game):
        rows={}
        def visit(node,path):
            for child in node.variations:
                if not node.board().is_legal(child.move): raise AssertionError('Illegal output move')
                key=path+(child.move.uci(),); rows[key]=child; visit(child,key)
        visit(game,()); return rows

    actual=paths(output[0]); expected=set()
    for game in games:
        for path,node in paths(game).items():
            expected.add(path)
            if path not in actual: raise AssertionError('Lost source move sequence')
            saved=actual[path]
            if node.comment not in saved.comment or node.starting_comment not in saved.starting_comment or not node.nags<=saved.nags:
                raise AssertionError('Lost source annotation')
    if set(actual)!=expected: raise AssertionError('Unexpected output move sequence')
    return rendered,{'source_games':len(games),'output_games':1,'move_nodes':len(actual)}


def publish(directory=None):
    directory=Path(directory or ROOT/'docs/human_repertoire/builder_books')
    target=directory/'nested'; target.mkdir(exist_ok=True)
    results={}
    for source in sorted(directory.glob('*.pgn')):
        rendered,checks=merge_games(source.read_text(encoding='utf-8'))
        (target/source.name).write_text(rendered,encoding='utf-8')
        results[source.name]=checks
    return results


if __name__=='__main__':
    import json
    print(json.dumps(publish(),indent=2))
