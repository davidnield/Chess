import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import month_archive as archive
import source_inventory as inv


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.source = self.base/'games'
        self.reference = self.base/'old-provenance'
        self.file = self.source/'year=2013/month=1/event=Blitz/part.parquet'
        self.file.parent.mkdir(parents=True)
        pq.write_table(pa.table({'x': [1, 2]}), self.file)
        archive.write_json(self.reference/'month=2013_1.json', {
            'month': '2013_1', 'flags': {'events': inv.EVENTS},
            'games_in_footers': 2, 'files': [str(self.file)]})
        self.obj = inv.create(self.source, self.reference, [(2013, 1)])
        self.a = SimpleNamespace(source=self.source, stage=self.base/'stage',
                                 dest=self.base/'archive', events=['Blitz', 'Rapid'],
                                 source_manifest=None, source_provenance=None)

    def test_missing_speed_requires_independent_evidence(self):
        self.assertEqual(inv.select(self.obj, (2013, 1), self.a.events)['absent_events'], ['Rapid'])
        self.file.unlink()
        with self.assertRaises(ValueError):
            inv.create(self.source, self.reference, [(2013, 1)])
        with self.assertRaises(ValueError):
            archive.inventory(self.source, (2013, 1), self.a.events, self.obj)

    def test_extra_and_changed_files_rejected(self):
        extra = self.file.with_name('extra.parquet')
        extra.write_bytes(self.file.read_bytes())
        with self.assertRaisesRegex(ValueError, 'files changed'):
            archive.inventory(self.source, (2013, 1), self.a.events, self.obj)
        extra.unlink()
        pq.write_table(pa.table({'x': [1, 2, 3]}), self.file)
        with self.assertRaisesRegex(ValueError, 'metadata changed'):
            archive.inventory(self.source, (2013, 1), self.a.events, self.obj)

    def test_new_runs_require_package_and_dry_run_no_writes(self):
        with self.assertRaisesRegex(ValueError, 'inventory required'):
            inv.resolve(self.a, [(2013, 1)])
        inv.publish(self.source/inv.RELATIVE, self.obj)
        obj = inv.resolve(self.a, [(2013, 1)])
        inv.publish(self.a.dest/inv.RELATIVE, obj, dry=True)
        self.assertFalse(self.a.dest.exists())

    def test_archive_only_resume_and_conflicting_identity(self):
        inv.publish(self.a.dest/inv.RELATIVE, self.obj)
        (self.reference/'month=2013_1.json').unlink()
        obj = inv.resolve(self.a, [(2013, 1)])
        self.assertEqual(archive.inventory(self.source, (2013, 1), self.a.events, obj), inv.select(obj,(2013,1),self.a.events))
        other = copy.deepcopy(self.obj)
        other['verification'] = 'different generation'
        inv.publish(self.a.stage/inv.RELATIVE, other)
        with self.assertRaisesRegex(ValueError, 'changed'):
            inv.resolve(self.a, [(2013, 1)])

    def test_changed_binding_and_embedded_provenance_rejected(self):
        p = self.source/inv.RELATIVE
        inv.publish(p, self.obj)
        obj = copy.deepcopy(self.obj)
        obj['months']['2013_1']['provenance']['files'] = []
        archive.write_json(p, obj)
        with self.assertRaisesRegex(ValueError, 'binding changed'):
            inv.load(p)
        with self.assertRaisesRegex(ValueError, 'digest'):
            inv.publish(self.a.dest/inv.RELATIVE, obj)

    def test_interrupted_copy_repaired_only_from_valid_package(self):
        inv.publish(self.source/inv.RELATIVE, self.obj)
        p = self.a.dest/inv.RELATIVE
        archive.write_json(p, self.obj)
        with self.assertRaises(FileNotFoundError):
            inv.load(p)
        obj = inv.resolve(self.a, [(2013, 1)])
        inv.publish(p, obj)
        self.assertEqual(inv.load(p), obj)

    def test_legacy_attachment_preserves_receipt_and_rejects_drift(self):
        root = self.a.dest
        archive.write_json(root/'_launcher_settings.json', {'events': self.a.events})
        p = root/'_archive_receipts/2013_1.json'
        archive.write_json(p, {'source':inv.select(self.obj, (2013,1),self.a.events), 'files':{'example':'unchanged'}})
        (root/'_month=2013_1.DONE').write_text('unchanged')
        original = p.read_bytes()
        inv.attach(self.obj, root)
        self.assertEqual(p.read_bytes(), original)
        self.assertEqual((root/'_month=2013_1.DONE').read_text(), 'unchanged')
        r = archive.read_json(p)
        r['source']['files'][0]['rows'] += 1
        archive.write_json(p,r)
        with self.assertRaisesRegex(ValueError, 'source differs'):
            inv.attach(self.obj, root)

    def test_invalid_scope_path_and_duplicate_rejected(self):
        for change in ('path', 'duplicate', 'rows'):
            obj=copy.deepcopy(self.obj); files=obj['months']['2013_1']['inventory']['files']
            if change=='path': files[0]['file']='../bad.parquet'
            if change=='duplicate': files.append(copy.deepcopy(files[0]))
            if change=='rows': files[0]['rows']=3
            with self.assertRaises(ValueError): inv.validate(obj)
        with self.assertRaisesRegex(ValueError,'cover'):
            inv.select(self.obj,(2014,1),self.a.events)


if __name__ == '__main__':
    unittest.main()
