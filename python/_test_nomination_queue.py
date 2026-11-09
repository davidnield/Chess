from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import unittest

from nomination_queue import run_controlled


class WhiteHold(unittest.TestCase):
    def test_black_completes_and_hold_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hold = root/"hold-before-white.json"
            hold.write_text('{"reason":"review"}')
            state = root/"queue.json"
            def publish(p,v):
                p.write_text(json.dumps(v))
            r = SimpleNamespace(publish=publish,now=lambda:"now")
            work = []
            def workflow():
                work.append("Black")
                r.publish(state,{"state":"building-white"})
                work.append("White")
            driver = SimpleNamespace(nt=SimpleNamespace(TRIAL=root),r=r,workflow=workflow)
            for _ in range(2):
                run_controlled(driver)
                self.assertEqual(json.loads(state.read_text())["state"],"held-before-white")
                self.assertIs(r.publish,publish)
            self.assertEqual(work,["Black","Black"])
            hold.unlink()
            run_controlled(driver)
            self.assertEqual(work[-2:],["Black","White"])


if __name__=="__main__":
    unittest.main()
