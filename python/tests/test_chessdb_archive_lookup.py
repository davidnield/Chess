import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from chessdb_import import archive_lookup as m


class ArchiveLookupTests(unittest.TestCase):
    def test_binding_and_resume_fail_closed(self):
        with tempfile.TemporaryDirectory(prefix='archive-lookup-test-') as d:
            root = Path(d)
            exe, idx, marker, bind, source, out = [root/n for n in ('query.exe', 'index.json', 'ARCHIVE.COMPLETE.json', 'binding.json', 'in.tsv', 'out.tsv')]
            exe.write_bytes(b'fixture exe')
            marker.write_text('{}')
            idx.write_text(json.dumps(dict(archive=str(root), marker_sha256=m.sha(marker))))
            b = dict(schema='chessdb-archive-query-binding-v1', executable=str(exe), index=str(idx),
                     executable_sha256=m.sha(exe), index_sha256=m.sha(idx), marker_sha256=m.sha(marker))
            bind.write_text(json.dumps(b))
            source.write_text('1\t2\t8/8/8/8/8/8/K7/7k w - -\n')
            self.assertEqual(m.binding(bind), b)
            out.write_text('1\t2\t-2\t\n')
            with self.assertRaisesRegex(ValueError, 'unreceipted'):
                m.lookup(bind, source, out)
            r = dict(binding_sha256=m.sha(bind), input_sha256=m.sha(source), adapter_sha256=m.sha(Path(m.__file__)), output_sha256=m.sha(out))
            receipt = out.with_suffix('.tsv.receipt.json')
            receipt.write_text(json.dumps(r))
            with patch.object(m.subprocess, 'Popen', side_effect=AssertionError('must reuse receipt')):
                self.assertEqual(m.lookup(bind, source, out), r)
            out.write_text('changed')
            with self.assertRaisesRegex(ValueError, 'resume identity'):
                m.lookup(bind, source, out)
            exe.write_bytes(b'changed exe')
            with self.assertRaisesRegex(ValueError, 'executable identity'):
                m.binding(bind)
            exe.write_bytes(b'fixture exe')
            marker.write_text('{"changed":true}')
            with self.assertRaisesRegex(ValueError, 'archive identity'):
                m.binding(bind)

    def test_invalid_batch_refused_before_io(self):
        for size in (0, -1, 4097):
            with self.assertRaises(ValueError):
                m.lookup(Path('none'), Path('none'), Path('none'), size)


if __name__ == '__main__':
    unittest.main()
