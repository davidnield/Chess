"""The one place a DuckDB query result becomes a polars DataFrame or an arrow Table.

DuckDB's Python fetch API has moved under this repo once already: 2.0 removed
.pl(), .arrow(), .fetch_arrow_table() and .fetch_record_batch() from
con.execute() results, which is how every caller here fetched frames. They all
go through these two functions now, so the next such change is a one-line fix
here. Both work on duckdb 1.5 and 2.x, and return the same column types as the
calls they replaced (docs/maintenance-report.md has the check).

They fetch through a relation, con.sql(sql), which is the path DuckDB 2.0
recommends. In 2.0 an execute() result's arrow export copies the rows an extra
time and warns that it does.

A 2.0 rule to keep in mind at the call site: a SELECT run by con.execute() and
not read to the end (.fetchone() on it, say) keeps the connection busy. Until
the next con.execute() replaces it, or con.complete() finishes it, these
functions and con.register() raise ResourceInUseException. 1.5 silently
discarded the unread result.
"""
from __future__ import annotations

import polars as pl
import pyarrow as pa


def duck_pl(con, sql: str) -> pl.DataFrame:
    """Every row of `sql` on `con` as a polars DataFrame (was con.execute(sql).pl())."""
    return con.sql(sql).pl()


def duck_arrow(con, sql: str) -> pa.Table:
    """Every row of `sql` on `con` as a pyarrow Table (was con.execute(sql).fetch_arrow_table())."""
    return con.sql(sql).to_arrow_table()
