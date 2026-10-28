//! Recursive CTE planning and scope resolution.
use super::super::*;
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

// Track sequential CTE visibility without manually enumerating expression
// subqueries. The AST visitor traverses every query form; addresses identify
// only immediate CTE bodies whose scope differs from their parent's main body.
pub(in crate::rel) fn recurring_key(name: &str) -> String {
    format!("\0recurring:{}", name.to_lowercase())
}
fn references(query: &s::Query, name: &str) -> bool {
    references_kind(query, name, false) || references_kind(query, name, true)
}
fn references_kind(query: &s::Query, name: &str, accumulated: bool) -> bool {
    struct Frame {
        body_hidden: bool,
        cte_scopes: HashMap<*const s::Query, bool>,
    }
    struct Search<'a> {
        name: &'a str,
        accumulated: bool,
        frames: Vec<Frame>,
        found: bool,
    }
    impl Visitor for Search<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &s::Query) -> ControlFlow<()> {
            let inherited = self.frames.last().is_some_and(|parent| {
                // Query addresses are stable during this immutable AST visit.
                // One keyed lookup avoids scanning all preceding siblings.
                parent
                    .cte_scopes
                    .get(&(q as *const s::Query))
                    .copied()
                    .unwrap_or(parent.body_hidden)
            });
            let mut visible_shadow = inherited;
            let mut cte_scopes = HashMap::new();
            if let Some(with) = &q.with {
                for cte in &with.cte_tables {
                    let same = cte.alias.name.value.eq_ignore_ascii_case(self.name);
                    if self.accumulated && !inherited && with.recursive && same {
                        // A nested same-name recursive anchor runs in the outer
                        // accumulated scope. Its step gets the inner binding.
                        // Visit the anchor separately before hiding the nested
                        // body's ordinary traversal (which also visits step).
                        if let s::SetExpr::SetOperation {
                            op: s::SetOperator::Union,
                            left,
                            ..
                        } = cte.query.body.as_ref()
                        {
                            let mut anchor = cte.query.as_ref().clone();
                            anchor.body = left.clone();
                            self.found |= references_kind(&anchor, self.name, true);
                        }
                    }
                    // Nonrecursive CTE sees preceding declarations, but not
                    // itself. WITH RECURSIVE adds this CTE's own declaration.
                    cte_scopes.insert(
                        cte.query.as_ref() as *const s::Query,
                        (if self.accumulated {
                            inherited
                        } else {
                            visible_shadow
                        }) || (with.recursive && same),
                    );
                    // An ordinary CTE named x does not hide recurring.x.
                    // Nested recursive x hides it only inside its own body;
                    // its containing main query retains the outer namespace.
                    if !self.accumulated {
                        visible_shadow |= same;
                    }
                }
            }
            self.frames.push(Frame {
                body_hidden: visible_shadow,
                cte_scopes,
            });
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &s::Query) -> ControlFlow<()> {
            self.frames.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, relation: &s::ObjectName) -> ControlFlow<()> {
            if !self.frames.last().is_some_and(|frame| frame.body_hidden)
                && if self.accumulated {
                    relation.0.len() == 2
                        && relation.0[0]
                            .as_ident()
                            .is_some_and(|i| i.value.eq_ignore_ascii_case("recurring"))
                        && relation.0[1]
                            .as_ident()
                            .is_some_and(|i| i.value.eq_ignore_ascii_case(self.name))
                } else {
                    relation.0.len() == 1
                        && relation.0[0]
                            .as_ident()
                            .is_some_and(|i| i.value.eq_ignore_ascii_case(self.name))
                }
            {
                self.found = true;
            }
            ControlFlow::Continue(())
        }
    }
    let mut search = Search {
        name,
        accumulated,
        frames: Vec::new(),
        found: false,
    };
    let _ = query.visit(&mut search);
    search.found
}

fn recursive_schema(anchor: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        anchor
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        anchor.metadata().clone(),
    ))
}

impl Planner<'_> {
    pub(in crate::rel) fn recursive_cte(
        &mut self,
        cte: s::Cte,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        let name = cte.alias.name.value.to_lowercase();
        if !references(&cte.query, &name) {
            let node = self.query(*cte.query, ctes)?;
            return crate::cte::column_aliases(node, &cte.alias.columns);
        }
        let mut q = *cte.query;
        if q.order_by.is_some()
            || q.limit_clause.is_some()
            || q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            return Err("unsupported: modifiers on recursive CTE union".into());
        }
        let s::SetExpr::SetOperation {
            op: s::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } = q.body.as_ref()
        else {
            return Err("recursive CTE requires an anchor UNION recursive arm".into());
        };
        let distinct = match set_quantifier {
            s::SetQuantifier::All => false,
            s::SetQuantifier::None | s::SetQuantifier::Distinct => true,
            _ => return Err("unsupported: recursive UNION quantifier".into()),
        };
        let mut anchor_query = q.clone();
        anchor_query.body = left.clone();
        q.body = right.clone();
        if references_kind(&anchor_query, &name, false)
            || (references_kind(&anchor_query, &name, true)
                && !ctes.contains_key(&recurring_key(&name)))
        {
            return Err("recursive CTE anchor cannot reference itself".into());
        }
        // An existing accumulated binding belongs to the enclosing invocation;
        // plan the anchor against it before installing this CTE's step binding.
        self.set_operand = true;
        let anchor = self.query(anchor_query, ctes)?;
        let anchor = crate::cte::column_aliases(anchor, &cte.alias.columns)?;
        // Recursive rows may introduce NULL even with a nonnullable anchor.
        // Fix this schema BEFORE binding the step or deriving optimizer facts.
        let target = recursive_schema(&anchor.schema);
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or("recursive plan identity overflow")?;
        let id = self.serial;
        let placeholder = Node {
            schema: target.clone(),
            estimated_rows: usize::MAX,
            op: RelOp::RecursiveScan {
                id,
                accumulated: false,
            },
        };
        let mut inner = ctes.clone();
        let accumulate = references_kind(&q, &name, true);
        inner.insert(
            recurring_key(&name),
            Node {
                schema: target.clone(),
                estimated_rows: usize::MAX,
                op: RelOp::RecursiveScan {
                    id,
                    accumulated: true,
                },
            },
        );
        inner.insert(name, placeholder);
        self.set_operand = true;
        let step = self.query(q, &inner)?;
        if anchor.schema.fields().len() != step.schema.fields().len() {
            return Err("recursive CTE column count mismatch".into());
        }
        // Bind recursive-only implicit conversions to the fixed anchor type.
        // Checked dialect kernels are reused without changing explicit casts.
        let step = super::coercion::step(step, &target)?;
        // Runtime uses recursive-owned funded physical normalization before
        // admission; do not synthesize ordinary UnionAll conversion wrappers.
        Ok(Node {
            schema: target,
            estimated_rows: usize::MAX,
            op: RelOp::RecursiveCte {
                id,
                anchor: Box::new(anchor),
                step: Box::new(step),
                distinct,
                accumulate,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn detects(sql: &str) -> bool {
        references(&bind::parse(sql).unwrap(), "x")
    }
    #[test]
    fn sequential_with_scope_and_quoted_relations() {
        assert!(detects("SELECT * FROM \"x\""));
        assert!(!detects("SELECT * FROM other.x"));
        assert!(detects(
            "WITH early AS (SELECT * FROM x), x AS (SELECT 8 AS n) SELECT * FROM early"
        ));
        assert!(!detects(
            "WITH x AS (SELECT 8 AS n), late AS (SELECT * FROM x) SELECT * FROM late"
        ));
        assert!(detects("WITH x AS (SELECT * FROM x) SELECT * FROM x"));
        assert!(!detects(
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<2) SELECT * FROM x"
        ));
        assert!(detects("SELECT (SELECT n FROM x)"));
        assert!(!detects(
            "WITH x AS (SELECT 8 AS n) SELECT (SELECT n FROM x)"
        ));
    }
    #[test]
    fn accumulated_namespace_ignores_ordinary_shadow_but_honors_recursive_scope() {
        let read = |sql| references_kind(&bind::parse(sql).unwrap(), "x", true);
        assert!(read("SELECT * FROM recurring.x"));
        assert!(read(
            "WITH RECURSIVE x(m) AS (SELECT MAX(n) FROM recurring.x UNION ALL SELECT m+10 FROM x WHERE FALSE) SELECT MAX(m) FROM x"
        ));
        assert!(!read(
            "WITH RECURSIVE x(m) AS (SELECT 10 UNION ALL SELECT m+1 FROM recurring.x WHERE m<11) SELECT MAX(m) FROM x"
        ));
        assert!(read("WITH x AS (SELECT 99 AS n) SELECT * FROM recurring.x"));
        assert!(read(
            "WITH recurring AS (SELECT 99) SELECT * FROM recurring.x"
        ));
        assert!(!read(
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM recurring.x WHERE n<2) SELECT * FROM x"
        ));
        assert!(read(
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM recurring.x WHERE n<2) SELECT * FROM recurring.x"
        ));
    }
    #[test]
    fn wide_cte_scope_mapping_preserves_early_late_and_shadowed_reads() {
        for width in [32, 64, 128] {
            let filler = (0..width)
                .map(|i| format!("c{i} AS (SELECT {i})"))
                .collect::<Vec<_>>()
                .join(", ");
            // Semantic controls for the indexed mapping, not a timing or
            // complexity regression test. Distinct sibling query addresses
            // must retain their individual sequential visibility states.
            for (sql, expected) in [
                (
                    format!("WITH early AS (SELECT * FROM x), {filler}, x AS (SELECT 1) SELECT 1"),
                    true,
                ),
                (
                    format!("WITH {filler}, late AS (SELECT * FROM x), x AS (SELECT 1) SELECT 1"),
                    true,
                ),
                (
                    format!("WITH x AS (SELECT 1), {filler}, late AS (SELECT * FROM x) SELECT 1"),
                    false,
                ),
                (
                    format!("WITH {filler}, x AS (SELECT 1), late AS (SELECT * FROM x) SELECT 1"),
                    false,
                ),
            ] {
                let query = bind::parse(&sql).unwrap();
                assert_eq!(references_kind(&query, "x", false), expected, "{sql}");
            }
            for sql in [
                format!(
                    "WITH early AS (SELECT * FROM recurring.x), {filler}, x AS (SELECT 1) SELECT 1"
                ),
                format!(
                    "WITH x AS (SELECT 1), {filler}, late AS (SELECT * FROM recurring.x) SELECT 1"
                ),
            ] {
                let query = bind::parse(&sql).unwrap();
                assert!(references_kind(&query, "x", true), "{sql}");
            }
        }
    }

    #[test]
    fn accumulated_scope_in_sequential_sibling_bodies() {
        let read = |sql| references_kind(&bind::parse(sql).unwrap(), "x", true);
        assert!(read(
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM x WHERE n<2), late AS (SELECT * FROM recurring.x) SELECT * FROM late"
        ));
        assert!(!read(
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM recurring.x WHERE n<2), late AS (SELECT * FROM x) SELECT * FROM late"
        ));
        assert!(read(
            "WITH RECURSIVE early AS (SELECT * FROM recurring.x), x(n) AS (SELECT 1 UNION SELECT n+1 FROM x WHERE n<2) SELECT * FROM early"
        ));
    }

    #[test]
    fn canonical_recursive_schema_is_nullable_and_preserves_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("provenance".to_owned(), "anchor".to_owned());
        let anchor = Schema::new_with_metadata(
            vec![Type::Int.field("n", false).with_nullable(false)],
            metadata,
        );
        let target = recursive_schema(&anchor);
        assert!(target.field(0).is_nullable());
        assert_eq!(target.metadata(), anchor.metadata());
        assert_eq!(target.field(0).data_type(), anchor.field(0).data_type());
        assert!(!anchor.field(0).is_nullable());
    }
    #[test]
    fn recurring_qualified_catalog_maps_fallback_and_active_binding_priority() {
        use parquet::arrow::ArrowWriter;
        use std::{
            fs::File,
            sync::atomic::{AtomicU64, Ordering},
        };
        static NEXT: AtomicU64 = AtomicU64::new(0);
        struct Remove(std::path::PathBuf);
        impl Drop for Remove {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "assay-recurring-catalog-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![Type::Int.field("n", false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![99]))])
                .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
        let _cleanup = Remove(path.clone());
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let tables = HashMap::from([("recurring.x".to_owned(), path)]);
        let view = ViewDefinition {
            query: bind::parse("SELECT 42 AS n").unwrap(),
            columns: Vec::new(),
        };
        let views = HashMap::from([("recurring.x".to_owned(), &view)]);
        let recursive = "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM recurring.x WHERE n<3) SELECT * FROM x ORDER BY n";
        for optimized in [false, true] {
            let options = Options {
                relational_optimizations: optimized,
                ..Options::default()
            };
            let check = |sql: &str,
                         tables: &HashMap<String, PathBuf>,
                         views: &HashMap<String, &ViewDefinition>| {
                let plan = prepare_with_views(sql, tables, &options, views).unwrap();
                let batch = execute::execute(&plan, &options).unwrap().batch;
                (0..batch.num_rows())
                    .map(|row| {
                        arrow::util::display::array_value_to_string(batch.column(0).as_ref(), row)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                check("SELECT n FROM recurring.x", &tables, &HashMap::new()),
                vec!["99"]
            );
            assert_eq!(
                check("SELECT n FROM recurring.x", &HashMap::new(), &views),
                vec!["42"]
            );
            assert_eq!(
                check(recursive, &tables, &HashMap::new()),
                vec!["1", "2", "3"]
            );
            assert_eq!(
                check(recursive, &HashMap::new(), &views),
                vec!["1", "2", "3"]
            );
            // An unrelated ordinary local CTE does not capture the qualified
            // catalog entry when no active accumulated binding exists.
            assert_eq!(
                check(
                    "WITH x AS (SELECT 7 AS n) SELECT n FROM recurring.x",
                    &tables,
                    &HashMap::new()
                ),
                vec!["99"]
            );
        }
    }
}
