import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from thread_metric import HELPERS, INCLUSIVE, IDENTITIES, binding_digest, check_threads, within_thread_limit


class ThreadMetricTests(unittest.TestCase):
    def test_boundaries_and_real_excess(self):
        for workers in (1, 4):
            self.assertTrue(within_thread_limit(workers - 1, workers, HELPERS))
            self.assertFalse(within_thread_limit(workers, workers, HELPERS))
            self.assertTrue(within_thread_limit(workers, workers, INCLUSIVE))
            self.assertFalse(within_thread_limit(workers + 1, workers, INCLUSIVE))
            self.assertFalse(within_thread_limit(0, workers, INCLUSIVE))

    def test_invalid_metrics(self):
        for contract in (HELPERS, INCLUSIVE):
            for value in (-1, True, False, 1.0, None, '1'):
                self.assertFalse(within_thread_limit(value, 4, contract))
            for workers in (-1, 0, True, 4.0, None):
                self.assertFalse(within_thread_limit(1, workers, contract))
        with self.assertRaises(ValueError):
            within_thread_limit(1, 4, 'unknown')

    def test_binding_and_effective_workers(self):
        identity = {key: 'a' * (40 if key in ('source_commit', 'source_tree') else 64)
                    for key in IDENTITIES}
        binding = dict(identity, schema='assay-thread-metric-binding/v1', contract=INCLUSIVE)
        pin = binding_digest(binding)
        metrics = dict(workers=4, worker_threads_peak=4)
        self.assertTrue(check_threads(metrics, 4, identity, binding, pin))
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            check_threads(metrics, 4, identity, dict(binding, contract=HELPERS), pin)
        for key in IDENTITIES:
            with self.subTest(key=key), self.assertRaises(ValueError):
                check_threads(metrics, 4, dict(identity, **{key: 'b' * len(identity[key])}), binding, pin)
        for metrics in ({}, {'workers': True, 'worker_threads_peak': 1},
                        {'workers': 1, 'worker_threads_peak': 1},
                        {'workers': 4, 'worker_threads_peak': 5}):
            self.assertFalse(check_threads(metrics, 4, identity, binding, pin))
        for bad in ({}, dict(binding, contract='unknown'), dict(binding, binary_sha256='')):
            with self.assertRaises(ValueError):
                check_threads({}, 4, identity, bad, binding_digest(bad))


if __name__ == '__main__':
    unittest.main()
