"""Versioned tracked-thread limits, bound to independently reviewed artifacts.

The caller must authenticate the build receipt and source inventory before
supplying observed identities. This check does not establish that provenance,
check memory or spill accounting, or measure whole-process OS threads.
"""
import hashlib
import json
import re

HELPERS = 'helper-permits-excluding-coordinator/v1'
INCLUSIVE = 'live-execution-threads-including-coordinator/v2'
IDENTITIES = ('source_commit', 'source_tree', 'source_manifest_sha256',
              'build_receipt_sha256', 'binary_sha256')


def within_thread_limit(peak, workers, contract):
    if contract not in (HELPERS, INCLUSIVE):
        raise ValueError('unknown thread metric contract')
    if type(peak) is not int or type(workers) is not int or workers < 1:
        return False
    if contract == HELPERS:
        return 0 <= peak < workers
    return 1 <= peak <= workers


def binding_digest(binding):
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def check_threads(metrics, requested_workers, observed_identity, reviewed_binding,
                  expected_binding_sha256):
    """Fail closed on unbound identities before evaluating reported metrics.

    expected_binding_sha256 is supplied by protected reviewer policy, never
    selected by the candidate. It covers the contract name as well as identities.
    Retain the binding and its digest in a new receipt, preserving old receipts.
    """
    if binding_digest(reviewed_binding) != expected_binding_sha256:
        raise ValueError('thread metric binding digest mismatch')
    if reviewed_binding.get('schema') != 'assay-thread-metric-binding/v1':
        raise ValueError('unknown thread metric binding')
    contract = reviewed_binding.get('contract')
    if contract not in (HELPERS, INCLUSIVE):
        raise ValueError('unknown thread metric contract')
    for key in IDENTITIES:
        expected = reviewed_binding.get(key)
        size = 40 if key in ('source_commit', 'source_tree') else 64
        if (not isinstance(expected, str) or
                re.fullmatch('[0-9a-f]{' + str(size) + '}', expected) is None or
                observed_identity.get(key) != expected):
            raise ValueError('thread metric identity mismatch: ' + key)
    effective = metrics.get('workers')
    if type(effective) is not int or type(requested_workers) is not int:
        return False
    if effective != requested_workers:
        return False
    return within_thread_limit(metrics.get('worker_threads_peak'),
                               requested_workers, contract)
