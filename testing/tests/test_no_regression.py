import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).parents[1]))
from result_contract import compare_outcomes, STRICT, NO_REGRESSION


class NoRegressionTests(unittest.TestCase):
    def row(self, status='passed', **extra):
        return dict(status=status, original_status='engine-error' if status == 'error' else status,
                    answer_sha256='a'*64, failure_signature_sha256='b'*64, **extra)

    def test_stable_errors_visible_only_in_explicit_policy(self):
        for category in ('engine-error', 'native-error', 'oracle-error'):
            row=self.row('error'); row['original_status']=category
            compare_outcomes(row, row, NO_REGRESSION)
            with self.assertRaises(ValueError): compare_outcomes(row, row, STRICT)
            self.assertEqual(row['status'], 'error')

    def test_new_failure_and_changed_answer_refused(self):
        good=self.row()
        for status in ('error', 'unsupported', 'wrong', 'not-run'):
            with self.assertRaises(ValueError): compare_outcomes(self.row(status), good, NO_REGRESSION)
        with self.assertRaisesRegex(ValueError, 'answer'):
            compare_outcomes(dict(good, answer_sha256='c'*64), good, NO_REGRESSION)
        with self.assertRaises(ValueError): compare_outcomes(dict(good, answer_sha256=None), good, NO_REGRESSION)

    def test_same_counts_do_not_hide_swapped_or_changed_errors(self):
        old=self.row('error')
        for changed in (dict(old, failure_signature_sha256='c'*64),
                        dict(old, original_status='oracle-error'),
                        dict(old, original_status='session-failure'),
                        dict(old, failure_signature_sha256=None)):
            with self.assertRaises(ValueError): compare_outcomes(changed, old, NO_REGRESSION)
        with self.assertRaises(ValueError): compare_outcomes(self.row('unsupported'), old, NO_REGRESSION)

    def test_improvements_allowed_but_wrong_baseline_not_waived(self):
        compare_outcomes(self.row(), self.row('error'), NO_REGRESSION)
        compare_outcomes(self.row(), self.row('unsupported'), NO_REGRESSION)
        for status in ('wrong', 'not-run'):
            with self.assertRaises(ValueError): compare_outcomes(self.row(), self.row(status), NO_REGRESSION)
        with self.assertRaises(ValueError): compare_outcomes(self.row(), self.row(), 'unknown')

    def test_infrastructure_failure_cannot_be_reclassified(self):
        for status in ('passed','unsupported','error'):
            for category in ('session-failure','native-unavailable','engine-internal'):
                row=self.row(status);row['original_status']=category
                with self.assertRaises(ValueError):compare_outcomes(row,row,NO_REGRESSION)


if __name__ == '__main__': unittest.main()
