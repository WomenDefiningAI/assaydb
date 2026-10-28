#!/usr/bin/env python3
"""Verify private external SF1 reference inputs before returning their bytes.

No benchmark SQL, answers, or local paths are distributed in this script.
Set ASSAY_EXTERNAL_SF1_INPUTS to the separately provisioned input directory.
"""
import hashlib
import json
import os
from pathlib import Path
import sys


def read_verified(name):
    root = Path(__file__).resolve().parents[1]
    hashes = json.loads((root / 'tests/support/external_sf1_hashes.json').read_text())
    if name not in hashes:
        raise ValueError('unknown external SF1 input')
    directory = os.environ.get('ASSAY_EXTERNAL_SF1_INPUTS')
    if not directory:
        raise ValueError('qualification requires ASSAY_EXTERNAL_SF1_INPUTS')
    base = Path(directory).resolve(strict=True)
    path = (base / name).resolve(strict=True)
    if not path.is_relative_to(base):
        raise ValueError('external input escapes its configured directory')
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != hashes[name]:
        raise ValueError('external SF1 input SHA-256 mismatch: ' + name)
    return data


def main():
    if len(sys.argv) != 2:
        raise ValueError('supply one manifest-relative input name or --check-all')
    name = sys.argv[1]
    if name == '--check-all':
        root = Path(__file__).resolve().parents[1]
        hashes = json.loads((root / 'tests/support/external_sf1_hashes.json').read_text())
        for entry in hashes:
            read_verified(entry)
        print(f'Verified all {len(hashes)} mandatory external SF1 reference inputs')
    else:
        sys.stdout.buffer.write(read_verified(name))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError) as error:
        print('External SF1 qualification failure: ' + str(error), file=sys.stderr)
        sys.exit(1)
