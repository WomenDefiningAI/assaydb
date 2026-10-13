//! Bounded nested-loop markers for previously unsupported scalar COUNT(*) and
//! expression EXISTS. Inner rows are retained once; no Cartesian result or
//! query-external answer cache is built. Existing equality decorrelation stays
//! on its original path.
use super::*;

pub(super) fn separable_equality(predicate: &s::Expr, local: &Schema, outer: &Schema) -> bool {
    let s::Expr::BinaryOp {
        left,
        op: s::BinaryOperator::Eq,
        right,
    } = predicate
    else {
        return false;
    };
    (binder(local).expr(left).is_ok() && binder(outer).expr(right).is_ok())
        || (binder(local).expr(right).is_ok() && binder(outer).expr(left).is_ok())
}

// Existing scalar rewrites are unchanged. New markers cannot be lifted out of
// lazy branches until their execution accepts the expression evaluator's mask.
pub(super) fn conditional_subqueries(expr: &s::Expr) -> Result<Vec<s::Query>> {
    let mut queries = Vec::new();
    syntax::rewrite(&mut expr.clone(), &mut |e| {
        let lazy = matches!(e, s::Expr::Case { .. })
            || matches!(e, s::Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("coalesce"));
        if lazy {
            syntax::rewrite(&mut e.clone(), &mut |child| {
                if let s::Expr::Subquery(q) | s::Expr::Exists { subquery: q, .. } = child {
                    queries.push(q.as_ref().clone());
                }
                Ok(false)
            })?;
            return Ok(true);
        }
        Ok(false)
    })?;
    Ok(queries)
}

impl Planner<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn correlated_marker(
        &mut self,
        left: Node,
        mut query: s::Query,
        local: SchemaRef,
        mut correlations: Vec<s::Expr>,
        marker: CorrelatedMarker,
        name: &str,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        if query.limit_clause.is_some() || query.order_by.is_some() {
            return Err("unsupported: correlated marker ORDER BY/LIMIT".into());
        }
        let s::SetExpr::Select(select) = query.body.as_mut() else {
            return Err("unsupported: correlated marker set operation".into());
        };
        if select.having.is_some()
            || !matches!(&select.group_by, s::GroupByExpr::Expressions(es, mods) if es.is_empty() && mods.is_empty())
        {
            return Err("unsupported: correlated marker grouping/HAVING".into());
        }
        if matches!(marker, CorrelatedMarker::Exists { .. }) {
            // A scalar aggregate produces a row even on empty input. Do not
            // confuse its existence with the existence of its source rows.
            for item in &select.projection {
                if let s::SelectItem::UnnamedExpr(e)
                | s::SelectItem::ExprWithAlias { expr: e, .. } = item
                {
                    let mut aggregate = false;
                    syntax::rewrite(&mut e.clone(), &mut |e| {
                        aggregate |= syntax::is_aggregate(e);
                        Ok(false)
                    })?;
                    if aggregate {
                        return Err("unsupported: EXISTS aggregate projection".into());
                    }
                }
            }
        }
        let names = local
            .fields()
            .iter()
            .map(|_| self.fresh())
            .collect::<Vec<_>>();
        if matches!(marker, CorrelatedMarker::Exists { .. }) {
            validate_exists_projection(select, &local, &left.schema, &names)?;
        }
        select.projection = local
            .fields()
            .iter()
            .zip(&names)
            .map(|(field, name)| s::SelectItem::ExprWithAlias {
                expr: syntax::ident(field.name()),
                alias: s::Ident::new(name),
            })
            .collect();
        if select.projection.is_empty() {
            // Keep the one-row, zero-source-column case representable through
            // ordinary relational projection/materialization paths.
            let dummy = self.fresh();
            select.projection.push(s::SelectItem::ExprWithAlias {
                expr: s::Expr::Value(s::Value::Number("1".into(), false).into()),
                alias: s::Ident::new(dummy),
            });
        }
        for predicate in &mut correlations {
            syntax::rewrite(predicate, &mut |e| {
                if matches!(e, s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)) {
                    if let Some(i) = local_column(&local, e)? {
                        *e = syntax::ident(&names[i]);
                    } else {
                        // Validate the outer reference before introducing the
                        // generated local names. Local scope wins when present.
                        binder(&left.schema).expr(e)?;
                    }
                    return Ok(true);
                }
                Ok(false)
            })?;
        }
        let right = self.query(query, ctes)?;
        let expression = syntax::and(correlations)
            .unwrap_or_else(|| s::Expr::Value(s::Value::Boolean(true).into()));
        let predicate = bind_pair_expression(&left, &right, &expression)?;
        if predicate.ty != Type::Bool {
            return Err("WHERE requires boolean".into());
        }
        let mut fields = left.schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(match marker {
            CorrelatedMarker::Count => Type::Int.field(name, true).with_nullable(false),
            CorrelatedMarker::Exists { .. } => Type::Bool.field(name, true).with_nullable(false),
        }));
        Ok(Node {
            schema: Arc::new(Schema::new(fields)),
            estimated_rows: left.estimated_rows,
            op: RelOp::CorrelatedMark {
                left: Box::new(left),
                right: Box::new(right),
                predicate,
                marker,
            },
        })
    }
}

// EXISTS discards projection values, not binding errors. Validate against the
// original scopes without executing expressions or projecting their values.
fn validate_exists_projection(
    select: &s::Select,
    local: &Schema,
    outer: &Schema,
    names: &[String],
) -> Result<()> {
    let renamed = Schema::new(
        local
            .fields()
            .iter()
            .zip(names)
            .map(|(f, name)| f.as_ref().clone().with_name(name))
            .collect::<Vec<_>>(),
    );
    let scope = combined(outer, &renamed);
    for item in &select.projection {
        match item {
            s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } => {
                let mut expression = expr.clone();
                syntax::rewrite(&mut expression, &mut |e| {
                    if matches!(
                        e,
                        s::Expr::Subquery(_) | s::Expr::Exists { .. } | s::Expr::InSubquery { .. }
                    ) {
                        return Err("unsupported: EXISTS projection subquery".into());
                    }
                    if matches!(e, s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)) {
                        if let Some(i) = local_column(local, e)? {
                            *e = syntax::ident(&names[i]);
                        } else {
                            // A local relation alias hides the same outer alias
                            // even when the requested column is absent locally.
                            if let s::Expr::CompoundIdentifier(ids) = e
                                && ids.len() == 2
                                && !qualified_columns(local, &ids[0]).is_empty()
                            {
                                return Err(format!("unknown local column: {e}"));
                            }
                            binder(outer).expr(e)?;
                        }
                        return Ok(true);
                    }
                    Ok(false)
                })?;
                binder(&scope).expr(&expression)?;
            }
            s::SelectItem::Wildcard(options) => {
                validate_wildcard_options(options)?;
                if local.fields().is_empty() {
                    return Err("EXISTS wildcard requires a local relation".into());
                }
                for field in local.fields() {
                    Type::from_field(field)?;
                }
            }
            s::SelectItem::QualifiedWildcard(kind, options) => {
                validate_wildcard_options(options)?;
                let s::SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return Err("unsupported: EXISTS wildcard expression".into());
                };
                let [s::ObjectNamePart::Identifier(qualifier)] = name.0.as_slice() else {
                    return Err("unsupported: EXISTS wildcard qualification".into());
                };
                let mut fields = qualified_columns(local, qualifier);
                if fields.is_empty() {
                    fields = qualified_columns(outer, qualifier);
                }
                if fields.is_empty() {
                    return Err(format!("unknown wildcard qualifier: {name}"));
                }
                for field in fields {
                    Type::from_field(field)?;
                }
            }
        }
    }
    Ok(())
}

fn qualified_columns<'a>(schema: &'a Schema, qualifier: &s::Ident) -> Vec<&'a Field> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let prefix = f.name().rsplit_once('.')?.0;
            // Compound identifiers in the existing relational binder normalize
            // their qualifier through the ordinary case-insensitive column path.
            let matches = prefix.eq_ignore_ascii_case(&qualifier.value);
            matches.then_some(f.as_ref())
        })
        .collect()
}

fn validate_wildcard_options(options: &s::WildcardAdditionalOptions) -> Result<()> {
    let allowed = s::WildcardAdditionalOptions {
        wildcard_token: options.wildcard_token.clone(),
        ..Default::default()
    };
    if *options != allowed {
        return Err("unsupported: EXISTS wildcard modifiers".into());
    }
    Ok(())
}

// Do not reinterpret an ambiguous or unsupported local column as an outer
// reference. This guard is confined to the newly supported marker paths.
fn local_column(schema: &Schema, expression: &s::Expr) -> Result<Option<usize>> {
    match binder(schema).expr(expression) {
        Ok(Expr {
            kind: crate::expr::Kind::Column(i),
            ..
        }) => Ok(Some(i)),
        Ok(_) => Err("correlated marker expected column reference".into()),
        Err(error) => {
            let (name, quoted) = match expression {
                s::Expr::Identifier(i) => (
                    if i.quote_style.is_some() {
                        i.value.clone()
                    } else {
                        i.value.to_lowercase()
                    },
                    i.quote_style.is_some(),
                ),
                s::Expr::CompoundIdentifier(ids) if ids.len() == 2 => (
                    ids.iter()
                        .map(|i| i.value.to_lowercase())
                        .collect::<Vec<_>>()
                        .join("."),
                    false,
                ),
                _ => return Err(error),
            };
            let present = schema.fields().iter().any(|f| {
                let field = if quoted {
                    f.name().clone()
                } else {
                    f.name().to_lowercase()
                };
                field == name
                    || (!name.contains('.') && field.rsplit('.').next() == Some(name.as_str()))
            });
            if present { Err(error) } else { Ok(None) }
        }
    }
}

pub(super) fn run(
    node: &Node,
    left: &Node,
    right: &Node,
    predicate: &Expr,
    marker: CorrelatedMarker,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let budget = ctx.budget.clone();
    let cancel = ctx.options.cancellation.clone();
    let batch_size = ctx.options.batch_size;
    let store = ctx.spill.clone();
    let mut stored = Materialized {
        budget: budget.clone(),
        batches: Vec::new(),
        files: Vec::new(),
        _leases: Vec::new(),
    };
    right.run(ctx, &mut |batch| {
        cancel.check()?;
        if let Some(store) = &store {
            // Individual files preserve encounter order and bound decode/scratch
            // on replay. File ownership and I/O are charged by SpillStore.
            for offset in (0..batch.num_rows()).step_by(batch_size) {
                stored.files.push(store.write_batch(
                    &batch.slice(offset, batch_size.min(batch.num_rows() - offset)),
                )?);
            }
        } else {
            let bytes = batch
                .get_array_memory_size()
                .checked_add(256)
                .ok_or("correlated retention overflow")?;
            stored._leases.push(budget.reserve(bytes)?);
            stored.batches.push(batch);
        }
        Ok(())
    })?;
    let _program_metadata = budget.reserve(
        left.schema
            .fields()
            .len()
            .checked_add(right.schema.fields().len())
            .and_then(|n| n.checked_mul(128))
            .and_then(|n| n.checked_add(256))
            .ok_or("correlated metadata overflow")?,
    )?;
    let program =
        crate::join_filter::JoinFilter::new(predicate, &combined(&left.schema, &right.schema))?;
    let mut filter_seconds = 0.;
    let mut spilled_replays = 0;
    let result = left.run(ctx, &mut |outer| {
        for offset in (0..outer.num_rows()).step_by(batch_size) {
            cancel.check()?;
            let outer = outer.slice(offset, batch_size.min(outer.num_rows() - offset));
            // Includes retained outer buffers, count/output storage and metadata.
            let _output = budget.reserve(
                outer
                    .num_rows()
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(outer.get_array_memory_size()))
                    .and_then(|n| n.checked_add(512 + outer.num_columns() * 64))
                    .ok_or("correlated output overflow")?,
            )?;
            let mut counts = Vec::with_capacity(outer.num_rows());
            for row in 0..outer.num_rows() {
                cancel.check()?;
                let mut count = 0_u64;
                let mut visit = |inner: &RecordBatch| -> Result<()> {
                    for offset in (0..inner.num_rows()).step_by(batch_size) {
                        cancel.check()?;
                        let start = Instant::now();
                        let inner = inner.slice(offset, batch_size.min(inner.num_rows() - offset));
                        let _indices = budget.reserve(
                            inner
                                .num_rows()
                                .checked_mul(8)
                                .and_then(|n| n.checked_add(256))
                                .ok_or("correlated indices overflow")?,
                        )?;
                        let li = UInt32Array::from(vec![
                            u32::try_from(row).map_err(
                                |_| "correlated row overflow"
                            )?;
                            inner.num_rows()
                        ]);
                        let ri = UInt32Array::from(
                            (0..inner.num_rows()).map(|i| i as u32).collect::<Vec<_>>(),
                        );
                        let (mask, _) = program.eval(&outer, &inner, &li, &ri, &budget)?;
                        let matches = boolean(&mask.array)
                            .iter()
                            .filter(|v| *v == Some(true))
                            .count();
                        count = match marker {
                            CorrelatedMarker::Count => count
                                .checked_add(matches as u64)
                                .ok_or("aggregate count overflow")?,
                            CorrelatedMarker::Exists { .. } => {
                                u64::from(count != 0 || matches != 0)
                            }
                        };
                        filter_seconds += start.elapsed().as_secs_f64();
                    }
                    Ok(())
                };
                for inner in &stored.batches {
                    visit(inner)?;
                }
                for file in &stored.files {
                    let inner = file.read_batch()?;
                    visit(&inner.batch)?;
                    spilled_replays += 1;
                }
                // EXISTS intentionally evaluates the complete predicate stream:
                // no first-match shortcut suppresses a later expression error.
                counts.push(i128::from(count));
            }
            let value: ArrayRef = match marker {
                CorrelatedMarker::Count => Arc::new(
                    Decimal128Array::from(counts)
                        .with_precision_and_scale(38, 0)
                        .map_err(|e| e.to_string())?,
                ),
                CorrelatedMarker::Exists { negated } => Arc::new(BooleanArray::from_iter(
                    counts.iter().map(|n| Some((*n != 0) != negated)),
                )),
            };
            let mut arrays = outer.columns().to_vec();
            arrays.push(value);
            emit(RecordBatch::try_new(node.schema.clone(), arrays).map_err(|e| e.to_string())?)?;
        }
        Ok(())
    });
    ctx.metrics.filter_seconds += filter_seconds;
    ctx.metrics.materialization_replay_batches += spilled_replays;
    ctx.metrics.spilled_materializations += usize::from(store.is_some());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> Node {
        let one = Node {
            schema: Arc::new(Schema::empty()),
            estimated_rows: 1,
            op: RelOp::One,
        };
        let left = unary(&bind::parse("SELECT 2 AS a").unwrap(), one.clone()).unwrap();
        let right = unary(&bind::parse("SELECT 1 AS b").unwrap(), one).unwrap();
        let predicate = binder(&combined(&left.schema, &right.schema))
            .expr(
                &syntax::conjuncts({
                    let q = bind::parse("SELECT 1 WHERE b<a").unwrap();
                    let s::SetExpr::Select(s) = *q.body else {
                        unreachable!()
                    };
                    s.selection.unwrap()
                })
                .remove(0),
            )
            .unwrap();
        Node {
            schema: Arc::new(Schema::new(vec![
                Type::Int.field("a", true),
                Type::Int.field("n", true),
            ])),
            estimated_rows: 1,
            op: RelOp::CorrelatedMark {
                left: Box::new(left),
                right: Box::new(right),
                predicate,
                marker: CorrelatedMarker::Count,
            },
        }
    }

    #[test]
    fn marker_releases_every_reservation_on_success_callback_failure_and_funding_failure() {
        for (limit, fail) in [(1 << 20, false), (1 << 20, true), (800, false)] {
            let budget = Budget::new(limit);
            let options = Options::default();
            let mut ctx = Context {
                shared_attempt: None,
                options: &options,
                scan_filters: HashMap::new(),
                static_filters: HashMap::new(),
                scan_transforms: HashMap::new(),
                replay_transforms: HashMap::new(),
                materialized: HashMap::new(),
                spill: None,
                budget: budget.clone(),
                metrics: Metrics::default(),
            };
            let result = node().run(&mut ctx, &mut |b| {
                assert_eq!(b.num_rows(), 1);
                assert_eq!(
                    b.column(1)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .value(0),
                    1
                );
                if fail {
                    Err("consumer failed".into())
                } else {
                    Ok(())
                }
            });
            if fail {
                assert_eq!(result.unwrap_err(), "consumer failed");
            } else if limit == 800 {
                assert!(result.unwrap_err().starts_with("resource-limit:"));
            } else {
                result.unwrap();
            }
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.peak_workers(), 0);
        }
    }
}
