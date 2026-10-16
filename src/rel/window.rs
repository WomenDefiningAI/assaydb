//! Ranking over the completed WHERE/GROUP BY/HAVING stream. Ordinals are
//! explicit sort keys and a second sort restores the input's canonical order.
use super::*;
use arrow::compute::{SortColumn, SortOptions};

mod sort;
#[cfg(test)]
mod tests;
use sort::Sorter;

#[derive(Debug, Clone, Copy, Serialize)]
pub enum Ranking {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Debug, Clone, Serialize)]
pub struct Specification {
    pub function: Ranking,
    pub partition: Vec<Expr>,
    pub order: Vec<(Expr, bool, bool)>,
}

pub(super) fn contains(expression: &s::Expr) -> Result<bool> {
    let mut found = false;
    syntax::rewrite(&mut expression.clone(), &mut |e| {
        found |= matches!(e, s::Expr::Function(f) if f.over.is_some());
        Ok(false)
    })?;
    Ok(found)
}

fn forbid(expression: &s::Expr, location: &str) -> Result<()> {
    if contains(expression)? {
        return Err(format!("unsupported: window function in {location}"));
    }
    Ok(())
}

pub(super) fn validate_select(select: &s::Select) -> Result<()> {
    if !select.named_window.is_empty() {
        return Err("unsupported: named WINDOW clauses".into());
    }
    if select.qualify.is_some() {
        return Err("unsupported: QUALIFY".into());
    }
    if let Some(expr) = &select.selection {
        forbid(expr, "WHERE")?;
    }
    if let Some(expr) = &select.having {
        forbid(expr, "HAVING")?;
    }
    if let s::GroupByExpr::Expressions(groups, _) = &select.group_by {
        for expr in groups {
            forbid(expr, "GROUP BY")?;
        }
    }
    for item in &select.projection {
        if let s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } = item {
            syntax::rewrite(&mut expr.clone(), &mut |expression| {
                if syntax::is_aggregate(expression) && contains(expression)? {
                    return Err("unsupported: window function inside aggregate".into());
                }
                if let s::Expr::Function(function) = expression
                    && function.over.is_some()
                {
                    ranking(function)?;
                }
                Ok(false)
            })?;
        }
    }
    Ok(())
}

fn ranking(function: &s::Function) -> Result<(Ranking, &s::WindowSpec)> {
    let name = function.name.to_string().to_lowercase();
    let kind = match name.as_str() {
        "row_number" => Ranking::RowNumber,
        "rank" => Ranking::Rank,
        "dense_rank" => Ranking::DenseRank,
        "sum" | "count" | "avg" | "min" | "max" => {
            return Err("unsupported: aggregate window functions".into());
        }
        _ => return Err(format!("unsupported: window function {name}")),
    };
    let Some(s::WindowType::WindowSpec(spec)) = &function.over else {
        return Err("unsupported: named window references".into());
    };
    if spec.window_name.is_some() {
        return Err("unsupported: named window references".into());
    }
    if spec.window_frame.is_some() {
        return Err("unsupported: window frames (ROWS/RANGE/GROUPS)".into());
    }
    if function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || function.uses_odbc_syntax
        || !matches!(function.parameters, s::FunctionArguments::None)
        || !matches!(&function.args, s::FunctionArguments::List(args)
            if args.args.is_empty() && args.duplicate_treatment.is_none() && args.clauses.is_empty())
    {
        return Err("unsupported: ranking window arguments/modifiers".into());
    }
    for expression in spec
        .partition_by
        .iter()
        .chain(spec.order_by.iter().map(|key| &key.expr))
    {
        if contains(expression)? {
            return Err("unsupported: nested window functions".into());
        }
    }
    if spec.order_by.iter().any(|key| key.with_fill.is_some()) {
        return Err("unsupported: window ORDER BY fill".into());
    }
    Ok((kind, spec))
}

fn needs_tuple_order(node: &Node) -> bool {
    match &node.op {
        RelOp::Join { .. }
        | RelOp::Mark { .. }
        | RelOp::TypedMark { .. }
        | RelOp::CorrelatedMark { .. } => true,
        RelOp::Window { .. }
        | RelOp::WindowOrder { .. }
        | RelOp::Scan { .. }
        | RelOp::One
        | RelOp::Empty => false,
        RelOp::Unary { input, plan } => {
            if plan.aggregation {
                // An arbitrary partial ORDER BY does not uniquely order groups.
                // The complete grouping tuple installed by this module does.
                (0..plan.groups.len()).any(|group| {
                    !plan.order.iter().any(|key| {
                        matches!(plan.output.get(key.column), Some(bind::Output::Group(index)) if *index == group)
                    })
                })
            } else if plan.order.is_empty() {
                needs_tuple_order(input)
            } else {
                // Arrow's ordinary sort is unstable on ties, even when its
                // child is a deterministic scan or a completed window.
                (0..node.schema.fields().len())
                    .any(|column| !plan.order.iter().any(|key| key.column == column))
            }
        }
        RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => needs_tuple_order(input),
        RelOp::UnionAll { left, right } => needs_tuple_order(left) || needs_tuple_order(right),
        RelOp::ScalarDefault {
            input, fallback, ..
        } => needs_tuple_order(input) || needs_tuple_order(fallback),
    }
}

fn complete_tuple_order(node: &mut Node) {
    // Hidden ORDER keys belong to the child layout. Complete that tuple before
    // Project removes private columns, preserving the explicit primary keys.
    if let RelOp::Rename { input } | RelOp::Project { input, .. } = &mut node.op {
        complete_tuple_order(input);
        return;
    }
    let mut order = match &node.op {
        RelOp::Unary { plan, .. } => plan.order.clone(),
        _ => Vec::new(),
    };
    order.extend((0..node.schema.fields().len()).map(|column| bind::Sort {
        column,
        descending: false,
        nulls_first: false,
    }));
    // Sort the already-produced physical arrays. Adding ORDER to the child's
    // plan would disable its passthrough path and widen Int64 to Decimal128,
    // invalidating the schema and expressions already bound above that child.
    let input = Node {
        schema: node.schema.clone(),
        estimated_rows: node.estimated_rows,
        op: std::mem::replace(&mut node.op, RelOp::Empty),
    };
    node.op = RelOp::WindowOrder {
        input: Box::new(input),
        order,
    };
}

fn stabilize_limits(node: &mut Node, shared: bool) -> Result<()> {
    match &mut node.op {
        RelOp::Window { .. }
        | RelOp::WindowOrder { .. }
        | RelOp::Scan { .. }
        | RelOp::One
        | RelOp::Empty => return Ok(()),
        RelOp::Materialize { input, .. } => stabilize_limits(input, true)?,
        RelOp::Unary { input, .. }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input } => stabilize_limits(input, shared)?,
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. }
        | RelOp::UnionAll { left, right } => {
            stabilize_limits(left, shared)?;
            stabilize_limits(right, shared)?;
        }
        RelOp::ScalarDefault {
            input, fallback, ..
        } => {
            stabilize_limits(input, shared)?;
            stabilize_limits(fallback, shared)?;
        }
    }
    if let RelOp::Limit { input, .. } = &mut node.op
        && needs_tuple_order(input)
    {
        if shared {
            return Err(
                "unsupported: window input LIMIT requires canonical order inside a CTE materialization"
                    .into(),
            );
        }
        complete_tuple_order(input);
    }
    Ok(())
}

fn canonical_input(mut input: Node) -> Result<Node> {
    stabilize_limits(&mut input, false)?;
    if !needs_tuple_order(&input) || input.schema.fields().is_empty() {
        return Ok(input);
    }
    complete_tuple_order(&mut input);
    Ok(input)
}

impl Planner<'_> {
    pub(super) fn window_order_projection(
        &mut self,
        select: &mut s::Select,
        order: &mut Option<s::OrderBy>,
    ) -> Result<()> {
        if let Some(s::OrderBy {
            kind: s::OrderByKind::Expressions(keys),
            ..
        }) = order
        {
            for key in keys.iter_mut() {
                if contains(&key.expr)? {
                    if let Some(item) = select.projection.iter_mut().find(|item| match item {
                        s::SelectItem::UnnamedExpr(expr)
                        | s::SelectItem::ExprWithAlias { expr, .. } => expr == &key.expr,
                        _ => false,
                    }) {
                        let alias = match item {
                            s::SelectItem::ExprWithAlias { alias, .. } => alias.clone(),
                            s::SelectItem::UnnamedExpr(expr) => {
                                let alias = s::Ident::with_quote('"', expr.to_string());
                                *item = s::SelectItem::ExprWithAlias {
                                    expr: expr.clone(),
                                    alias: alias.clone(),
                                };
                                alias
                            }
                            _ => unreachable!(),
                        };
                        key.expr = s::Expr::Identifier(alias);
                        continue;
                    }
                    if select.distinct.is_some() {
                        return Err("unsupported: DISTINCT ORDER BY window expression must appear in SELECT".into());
                    }
                    let alias = self.fresh();
                    select.projection.push(s::SelectItem::ExprWithAlias {
                        expr: key.expr.clone(),
                        alias: s::Ident::new(&alias),
                    });
                    key.expr = syntax::ident(&alias);
                }
            }
            let mut windowed = false;
            for item in &select.projection {
                if let s::SelectItem::UnnamedExpr(expr)
                | s::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    windowed |= contains(expr)?;
                }
            }
            if windowed && !keys.is_empty() && select.distinct.is_none() {
                // Make final ORDER BY ties stable on both Arrow sorting paths.
                // This private ordinal is removed before DISTINCT/output.
                let parsed = bind::parse("SELECT ROW_NUMBER() OVER ()")?;
                let s::SetExpr::Select(parsed) = *parsed.body else {
                    unreachable!()
                };
                let s::SelectItem::UnnamedExpr(expr) =
                    parsed.projection.into_iter().next().unwrap()
                else {
                    unreachable!()
                };
                let alias = self.fresh();
                select.projection.push(s::SelectItem::ExprWithAlias {
                    expr,
                    alias: s::Ident::new(&alias),
                });
                let mut tie = keys[0].clone();
                tie.expr = syntax::ident(&alias);
                tie.options.asc = Some(true);
                tie.options.nulls_first = Some(false);
                tie.with_fill = None;
                keys.push(tie);
            }
        }
        Ok(())
    }

    pub(super) fn window_expression(
        &mut self,
        expression: &mut s::Expr,
        mut input: Node,
        ctes: &HashMap<String, Node>,
        group_source: Option<&Schema>,
    ) -> Result<Node> {
        syntax::rewrite(expression, &mut |expr| {
            // Empty IN validates its operand but cannot evaluate window keys.
            if let s::Expr::InList {
                expr: operand,
                list,
                negated,
            } = expr
                && list.is_empty()
                && contains(operand)?
            {
                let mut operand = *operand.clone();
                let validation =
                    self.window_expression(&mut operand, input.clone(), ctes, group_source)?;
                let validation = self.subqueries(&mut operand, validation, ctes)?;
                bind_node_expression(&validation, &operand)?;
                *expr = s::Expr::Value(s::Value::Boolean(*negated).into());
                return Ok(true);
            }
            if syntax::is_aggregate(expr) && contains(expr)? {
                return Err("unsupported: window function inside aggregate".into());
            }
            let s::Expr::Function(function) = expr else {
                return Ok(false);
            };
            if function.over.is_none() {
                return Ok(false);
            }
            let (kind, spec) = ranking(function)?;
            let mut spec = spec.clone();
            for expression in spec
                .partition_by
                .iter_mut()
                .chain(spec.order_by.iter_mut().map(|key| &mut key.expr))
            {
                input = self.subqueries(expression, input.clone(), ctes)?;
            }
            input = canonical_input(input.clone())?;
            let bind = |expr: &s::Expr| {
                if let Some(source) = group_source {
                    group_binding::expression(expr, &input, source)
                } else {
                    bind_node_expression(&input, expr)
                }
            };
            let specification = Specification {
                function: kind,
                partition: spec.partition_by.iter().map(&bind).collect::<Result<_>>()?,
                order: spec
                    .order_by
                    .iter()
                    .map(|key| {
                        Ok((
                            bind(&key.expr)?,
                            key.options.asc == Some(false),
                            key.options.nulls_first.unwrap_or(false),
                        ))
                    })
                    .collect::<Result<_>>()?,
            };
            let name = loop {
                let candidate = self.fresh();
                if !input.schema.fields().iter().any(|field| {
                    field
                        .name()
                        .rsplit('.')
                        .next()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&candidate))
                }) {
                    break candidate;
                }
            };
            let mut fields = input.schema.fields().to_vec();
            fields.push(Arc::new(Type::Int.field(&name, false)));
            input = Node {
                schema: Arc::new(Schema::new(fields)),
                estimated_rows: input.estimated_rows,
                op: RelOp::Window {
                    input: Box::new(input.clone()),
                    specification,
                },
            };
            *expr = syntax::ident(&name);
            Ok(true)
        })?;
        Ok(input)
    }
}

/// Canonicalize a completed physical stream without evaluating expressions or
/// changing its schema. Complete tuples make any remaining ties indistinguishable.
pub(super) fn run_order(
    child: &Node,
    order: &[bind::Sort],
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    // The owned sort-key vector must be funded before cloning, including when
    // Sorter construction subsequently refuses its larger workspace allowance.
    let _order_metadata = ctx.budget.reserve(
        order
            .len()
            .saturating_mul(std::mem::size_of::<bind::Sort>())
            .saturating_add(256),
    )?;
    let mut sorter = Sorter::new(child.schema.clone(), order.to_vec(), ctx)?;
    child.run(ctx, &mut |batch| sorter.push(batch))?;
    let metrics = sorter.finish(emit)?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}

pub(super) fn run(
    node: &Node,
    child: &Node,
    specification: &Specification,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let budget = ctx.budget.clone();
    let options = ctx.options;
    let width = child.schema.fields().len();
    let key_count = specification.partition.len() + specification.order.len();
    let _metadata = budget.reserve(
        (width + key_count + 1)
            .saturating_mul(2048)
            .saturating_add(4096),
    )?;
    let keys = specification
        .partition
        .iter()
        .chain(specification.order.iter().map(|(e, _, _)| e))
        .collect::<Vec<_>>();
    let ordinal_column = width + keys.len();
    let mut fields = child.schema.fields().to_vec();
    for (i, expression) in keys.iter().enumerate() {
        fields.push(Arc::new(
            expression.ty.field(&format!("window_key_{i}"), true),
        ));
    }
    fields.push(Arc::new(Field::new(
        "window_ordinal",
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    let keyed_schema = Arc::new(Schema::new(fields));
    let order = (0..keys.len())
        .map(|i| {
            let (descending, nulls_first) = if i < specification.partition.len() {
                (false, false)
            } else {
                let (_, descending, nulls_first) =
                    &specification.order[i - specification.partition.len()];
                (*descending, *nulls_first)
            };
            bind::Sort {
                column: width + i,
                descending,
                nulls_first,
            }
        })
        .chain([bind::Sort {
            column: ordinal_column,
            descending: false,
            nulls_first: false,
        }])
        .collect();
    let mut sorter = Sorter::new(keyed_schema.clone(), order, ctx)?;
    let mut ordinal = 0_u64;
    child.run(ctx, &mut |batch| {
        let _metadata = budget.reserve((width + keys.len() + 1).saturating_mul(1024))?;
        let input = super::input(&batch, &budget, keys.iter().copied())?;
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            options.cancellation.check()?;
            values.push(key.eval(&input, &budget)?);
        }
        let _ordinal_credit = budget.reserve(
            batch
                .num_rows()
                .saturating_mul(16_usize.saturating_mul(keys.len() + 1))
                .saturating_add((keys.len() + 1).saturating_mul(1024)),
        )?;
        let end = ordinal
            .checked_add(batch.num_rows() as u64)
            .ok_or("window ordinal overflow")?;
        let mut arrays = batch.columns().to_vec();
        // Window equality follows SQL GROUP BY: both signed zeros share one
        // partition/peer group. Normalize only the private sorting keys.
        arrays.extend(values.iter().map(|value| {
            if let Some(array) = value.array.as_any().downcast_ref::<Float64Array>()
                && array
                    .iter()
                    .flatten()
                    .any(|value| value == 0.0 && value.is_sign_negative())
            {
                Arc::new(Float64Array::from_iter(array.iter().map(|value| {
                    value.map(|value| if value == 0.0 { 0.0 } else { value })
                }))) as ArrayRef
            } else {
                value.array.clone()
            }
        }));
        arrays.push(Arc::new(UInt64Array::from_iter_values(ordinal..end)));
        ordinal = end;
        sorter.push(RecordBatch::try_new(keyed_schema.clone(), arrays).map_err(|e| e.to_string())?)
    })?;

    let mut fields = node.schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        "window_ordinal",
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    let restore_schema = Arc::new(Schema::new(fields));
    let mut restore = Sorter::new(
        restore_schema.clone(),
        vec![bind::Sort {
            column: width + 1,
            descending: false,
            nulls_first: false,
        }],
        ctx,
    )?;
    let mut previous: Option<(RecordBatch, Reservation)> = None;
    let mut row_number = 0_i64;
    let mut rank = 0_i64;
    let mut dense_rank = 0_i64;
    let metrics = sorter.finish(&mut |batch| {
        options.cancellation.check()?;
        let _credit = budget.reserve(
            batch
                .num_rows()
                .saturating_mul(16)
                .saturating_add((width + keys.len() + 1).saturating_mul(1024)),
        )?;
        let mut result = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                options.cancellation.check()?;
            }
            let old = if row == 0 {
                previous.as_ref().map(|(old, _)| (old, 0))
            } else {
                Some((&batch, row - 1))
            };
            let same_partition = old.is_some_and(|(old, old_row)| {
                equal_keys(
                    old,
                    old_row,
                    &batch,
                    row,
                    width,
                    specification.partition.len(),
                )
            });
            let peer = same_partition
                && old.is_some_and(|(old, old_row)| {
                    equal_keys(
                        old,
                        old_row,
                        &batch,
                        row,
                        width + specification.partition.len(),
                        specification.order.len(),
                    )
                });
            if !same_partition {
                row_number = 0;
                dense_rank = 0;
            }
            row_number = row_number
                .checked_add(1)
                .ok_or("window rank exceeds BIGINT")?;
            if !peer {
                rank = row_number;
                dense_rank = dense_rank
                    .checked_add(1)
                    .ok_or("window rank exceeds BIGINT")?;
            }
            result.push(match specification.function {
                Ranking::RowNumber => row_number,
                Ranking::Rank => rank,
                Ranking::DenseRank => dense_rank,
            });
        }
        if batch.num_rows() != 0 {
            // Compact the last key row: a slice would retain an entire run.
            let bytes = batch
                .get_array_memory_size()
                .saturating_mul(2)
                .saturating_add((keys.len() + 1).saturating_mul(1024));
            let credit = budget.reserve(bytes)?;
            let indices = UInt64Array::from(vec![(batch.num_rows() - 1) as u64]);
            let arrays = batch
                .columns()
                .iter()
                .map(|array| take(array, &indices, None).map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>>>()?;
            let last =
                RecordBatch::try_new(keyed_schema.clone(), arrays).map_err(|e| e.to_string())?;
            let mut credit = credit;
            credit.release_excess(
                last.get_array_memory_size()
                    .saturating_add((width + keys.len() + 1).saturating_mul(256)),
            );
            previous = Some((last, credit));
        }
        let mut arrays = batch.columns()[..width].to_vec();
        arrays.push(Arc::new(Int64Array::from(result)));
        arrays.push(batch.column(ordinal_column).clone());
        restore
            .push(RecordBatch::try_new(restore_schema.clone(), arrays).map_err(|e| e.to_string())?)
    })?;
    drop(previous);
    sort::merge_metrics(&mut ctx.metrics, metrics);
    let metrics = restore.finish(&mut |batch| {
        let _credit = budget.reserve(
            width
                .saturating_add(1)
                .saturating_mul(256)
                .saturating_add(1024),
        )?;
        emit(
            RecordBatch::try_new(node.schema.clone(), batch.columns()[..width + 1].to_vec())
                .map_err(|e| e.to_string())?,
        )
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}

// Compare exact typed Arrow values, with SQL NULL equality for peering. Float
// uses the same total ordering as Arrow ORDER BY (including signed zero/NaN).
fn equal_keys(
    a: &RecordBatch,
    ai: usize,
    b: &RecordBatch,
    bi: usize,
    start: usize,
    count: usize,
) -> bool {
    (start..start + count).all(|column| {
        let a = a.column(column);
        let b = b.column(column);
        if is_sql_null(a.as_ref(), ai) || is_sql_null(b.as_ref(), bi) {
            return is_sql_null(a.as_ref(), ai) && is_sql_null(b.as_ref(), bi);
        }
        macro_rules! equal {
            ($ty:ty) => {
                a.as_any().downcast_ref::<$ty>().unwrap().value(ai)
                    == b.as_any().downcast_ref::<$ty>().unwrap().value(bi)
            };
        }
        match a.data_type() {
            arrow::datatypes::DataType::Int64 => equal!(Int64Array),
            arrow::datatypes::DataType::Int32 => equal!(Int32Array),
            arrow::datatypes::DataType::Decimal128(_, _) => equal!(Decimal128Array),
            arrow::datatypes::DataType::Date32 => equal!(Date32Array),
            arrow::datatypes::DataType::Utf8 => equal!(StringArray),
            arrow::datatypes::DataType::Binary => equal!(BinaryArray),
            arrow::datatypes::DataType::Boolean => equal!(BooleanArray),
            arrow::datatypes::DataType::Float64 => a
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(ai)
                .total_cmp(&b.as_any().downcast_ref::<Float64Array>().unwrap().value(bi))
                .is_eq(),
            arrow::datatypes::DataType::Null => true,
            _ => unreachable!("bound window key type"),
        }
    })
}
