"""Assay native SQL sessions with typed Arrow results."""
from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING, Any

from ._native import (
    AssayError, ConnectionClosedError, InternalError, NativeConnection,
    ResourceError, SQLError, UnsupportedError,
)

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ["connect", "query", "Connection", "Result", "AssayError", "SQLError",
           "UnsupportedError", "ResourceError", "InternalError", "ConnectionClosedError"]


class Result:
    """A completed query or statement. Results survive connection closure."""

    def __init__(self, ipc: bytes | None, rowcount: int) -> None:
        self._ipc = ipc
        self._table: pa.Table | None = None
        self._rowcount = rowcount

    @property
    def rowcount(self) -> int:
        """Query row count, or the engine's affected-row count for DML."""
        return self._rowcount

    @property
    def is_query(self) -> bool:
        return self._ipc is not None or self._table is not None

    def to_arrow(self) -> pa.Table:
        """Return a pyarrow.Table, preserving engine types and NULLs."""
        if not self.is_query:
            raise SQLError("statement has no Arrow result")
        if self._table is None:
            import pyarrow as pa
            with pa.ipc.open_stream(self._ipc) as reader:
                self._table = reader.read_all()
            # Arrow keeps the imported buffer owner alive. Do not retain an
            # additional Python reference after conversion.
            self._ipc = None
        return self._table

    def to_pandas(self, **kwargs: Any) -> Any:
        """Convert to pandas, defaulting to lossless nullable Arrow dtypes."""
        try:
            import pandas as pd
        except ImportError as error:
            raise ImportError("Install assaydb[pandas] to use to_pandas()") from error
        kwargs.setdefault("types_mapper", pd.ArrowDtype)
        return self.to_arrow().to_pandas(**kwargs)

    def to_polars(self, **kwargs: Any) -> Any:
        """Convert the Arrow result to Polars without SQL execution there."""
        try:
            import polars as pl
        except ImportError as error:
            raise ImportError("Install assaydb[polars] to use to_polars()") from error
        return pl.from_arrow(self.to_arrow(), **kwargs)

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self.to_arrow().__arrow_c_stream__(requested_schema)


class Connection:
    """Ephemeral native catalog; serialize operations, release the GIL in Rust."""

    def __init__(self, *, directory: os.PathLike[str] | str | None = None,
                 workers: int = 1, memory_bytes: int = 256 * 1024**2,
                 catalog_bytes: int = 64 * 1024**2,
                 max_result_bytes: int = 64 * 1024**2,
                 allow_local_files: bool = True) -> None:
        for name, value in locals().copy().items():
            if name in {"workers", "memory_bytes", "catalog_bytes", "max_result_bytes"}:
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise ValueError(f"{name} must be a positive integer")
        if not isinstance(allow_local_files, bool):
            raise ValueError("allow_local_files must be a boolean")
        parent = tempfile.gettempdir() if directory is None else os.fspath(directory)
        self._native = NativeConnection(parent, workers, memory_bytes, catalog_bytes, max_result_bytes, allow_local_files)

    def execute(self, sql: str) -> Result:
        """Execute one SQL statement without rewriting it or auto-retrying it."""
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        ipc, rowcount = self._native.execute(sql)
        return Result(ipc, rowcount)

    sql = execute

    def close(self) -> None:
        """Close once safely; completed results retain their independent data."""
        self._native.close()

    @property
    def closed(self) -> bool:
        return self._native.closed

    def __enter__(self) -> Connection:
        if self.closed:
            raise ConnectionClosedError("connection is closed")
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def connect(**kwargs: Any) -> Connection:
    """Create a native connection; see Connection for limits and directory."""
    return Connection(**kwargs)


def query(sql: str, **kwargs: Any) -> Result:
    """Execute in a temporary native session and return an independent result."""
    with connect(**kwargs) as connection:
        return connection.execute(sql)
