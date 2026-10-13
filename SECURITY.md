# Security policy

Assay is an experimental alpha. No stable-release support or response-time
guarantee is promised. Only read Parquet files you trust: malformed metadata
can exhaust memory, and accounted query limits do not cap all process memory.
Local file readers are not a directory sandbox.

Report sensitive findings through the repository's
[private vulnerability reporting route](https://github.com/WomenDefiningAI/assaydb/security)
when available. Otherwise request a private contact through
[Women Defining AI](https://www.womendefiningai.org/) without including exploit
details or secrets in the initial message. Do not post sensitive reports as
public issues.

Include the affected version, platform, minimal reproduction, impact, and
process/resource limits. See [limitations](docs/WHAT-ASSAY-ISNT.md).
