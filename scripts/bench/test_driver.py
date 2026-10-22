#!/usr/bin/env python3
"""Regression: a child may exit before the stdout reader publishes its final JSON."""
import subprocess
import sys
import time
import unittest

from driver import _StdoutLines


class DelayedReader(_StdoutLines):
    def _run(self):
        time.sleep(0.1)
        super()._run()


class StdoutExitRace(unittest.TestCase):
    def test_final_json_survives_child_exit_before_reader(self):
        with subprocess.Popen(
            [sys.executable, '-c', 'print(\'{"role":"probe","value":7}\', flush=True)'],
            stdout=subprocess.PIPE,
        ) as child:
            reader = DelayedReader(child)
            self.assertEqual(child.wait(timeout=2), 0)
            self.assertEqual(reader.await_last_json(2), {'role': 'probe', 'value': 7})

    def test_eof_without_json_is_not_a_result(self):
        with subprocess.Popen([sys.executable, '-c', 'print("no metrics")'],
                              stdout=subprocess.PIPE) as child:
            reader = _StdoutLines(child)
            self.assertIsNone(reader.await_last_json(2))
            self.assertEqual(child.wait(timeout=2), 0)


if __name__ == '__main__':
    unittest.main()
