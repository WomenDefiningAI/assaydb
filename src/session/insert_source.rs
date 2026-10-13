//! Reject statement-shaped INSERT sources before recursive session dispatch.
use super::*;

pub(super) fn validate(query: &s::Query) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate(&cte.query)?;
        }
    }
    body(&query.body)
}

fn body(source: &s::SetExpr) -> Result<()> {
    match source {
        s::SetExpr::Insert(_) | s::SetExpr::Update(_) | s::SetExpr::Delete(_) => Err(unsupported(
            "INSERT source must be read-only; nested INSERT, UPDATE, or DELETE is unsupported",
        )),
        s::SetExpr::Query(query) => validate(query),
        s::SetExpr::SetOperation { left, right, .. } => {
            body(left)?;
            body(right)
        }
        // These remain subject to their existing binding/feature checks. Their
        // expression/derived-table subqueries use read-only relational binding,
        // not recursive session statement execution.
        s::SetExpr::Select(_) | s::SetExpr::Values(_) | s::SetExpr::Table(_) => Ok(()),
    }
}
