"""Reviewable fixed packages as legal PGNs with studied White moves marked."""
import io
import chess.pgn
from .followthrough import packages, key
from .followthrough_data import PREFIX
from .io import output_path


def export():
    root, books, _, _ = packages(); games=[]
    for name, book in books.items():
        game=chess.pgn.Game()
        game.headers.update({'Event':name,'White':'Prepared player','Black':'Empirical opponent',
                             'Result':'*','StudyPositions':str(len(book))})
        if name.startswith('sequence'):
            continuation=['d4','exd4','Nxd4','Bc5','Be3']
            if name=='sequence4': continuation+=['Qf6','c3']
            paths=[continuation]
        else:
            paths=[]
            for reply,move in [('exd4','Nxd4'),('Nf6','Nc3'),('d6','Nc3')]:
                board=root.copy(); board.push_san('d4'); board.push_san(reply)
                if book.get(key(board))==move: paths.append(['d4',reply,move])
            if not paths: paths=[['d4']]
        for suffix in paths:
            node=game
            for san in ['e4','e5','Nf3','Nc6']+suffix:
                board=node.board(); move=board.parse_san(san)
                child=next((v for v in node.variations if v.move==move),None)
                if child is None: child=node.add_variation(move)
                if book.get(key(board))==san: child.comment='STUDY: prescribed White move.'
                node=child
        games.append(str(game))
    result='\n\n'.join(games)+'\n'
    verify(result,books)
    return result


def verify(text,books):
    stream=io.StringIO(text); found=set()
    while (game:=chess.pgn.read_game(stream)) is not None:
        if game.errors: raise ValueError('invalid PGN')
        name=game.headers['Event']; found.add(name); cards={}; pending=[game]
        while pending:
            node=pending.pop()
            for child in node.variations:
                if 'STUDY:' in child.comment:
                    board=node.board()
                    if not board.turn: raise ValueError('studied Black move')
                    cards[key(board)]=board.san(child.move)
                pending.append(child)
        if cards!=books[name] or int(game.headers['StudyPositions'])!=len(cards):
            raise ValueError('PGN study prescriptions differ')
    if found!=set(books): raise ValueError('PGN package missing')


if __name__=='__main__':
    path=output_path(PREFIX+'packages.pgn'); path.write_text(export(),encoding='utf-8'); print(path)
