# Parquet decoder provenance

This directory retains the upstream `parquet` 57.0.0 crate from crates.io.
Archive SHA-256: `7a0f31027ef1af7549f7cec603a9a21dce706d3f8d7c2060a68f43c1773be95a`.
Upstream LICENSE.txt, NOTICE.txt and Cargo manifests are unchanged. Apache-2.0
continues to apply to this component. Modified files carry modification notices.

The upstream delta is confined to:

- `src/arrow/array_reader/fixed_len_byte_array.rs`: direct FLBA-to-Decimal128 decode.
- `src/compression.rs`: reusable sized Zstandard decompression for eligible single
  frames with a declared window no larger than 128 MiB; streaming fallback for
  other inputs or failure.
- This provenance document.

The frozen source campaign compared 303 decoder error records, eight FLBA
controls and 82 coverage records per role. These are bounded test evidence,
not proof of identical behavior for all inputs. See the promotion qualification
report for exact source identities and exported-tree validation status.

Decoder native allocations, retained Zstandard context, output capacity and
FLBA conversion buffers are not fully covered by engine reservation accounting.
The window admission limit is not a whole-process or total-decoder memory cap.
Semantic fallback does not prove equal resource usage. This limitation remains
open; no hard-RSS or universal memory qualification is claimed.

Only this directory is selected by the root Cargo patch. No package release is
part of this source promotion. Removing the patch requires restoring the upstream
registry lock entry and repeating affected tests; it is not an evidence-preserving
substitution.
