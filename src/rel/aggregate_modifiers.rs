//! Aggregate FILTER lowering and GROUP BY ROLLUP / CUBE / GROUPING SETS.
//!
//! FILTER: `agg(x) FILTER (WHERE c)` becomes `agg(CASE WHEN c THEN x END)`
//! (`COUNT(*)` uses the constant 1). Aggregates ignore NULL inputs, so rows
//! where `c` is FALSE or NULL contribute nothing, exactly as in DuckDB and
//! PostgreSQL. The lowered CASE evaluates `x` only where `c` holds, which is
//! PostgreSQL's behavior; DuckDB evaluates `x` on every row. The engines can
//! therefore differ only when `x` raises an error on a filtered-out row, so an
//! argument (or ORDER BY key) that may raise an error DuckDB also raises is
//! refused. Division by zero and Float overflow are admitted: DuckDB returns
//! NULL / an infinity for them instead of raising, so both references skip
//! or ignore the filtered-out rows and agree (see `filtered_rows_agree`).
//!
//! Grouping sets: every grouping set is an ordinary funded aggregation over
//! the same input, projected to one layout in which keys outside the set are
//! typed NULLs and each GROUPING(...) call is a per-set integer constant. The
//! sets are concatenated with UNION ALL; HAVING, projection and ORDER BY then
//! bind over that layout exactly as for a plain GROUP BY.
use super::*;
use crate::expr::{Kind, Op};

const MAX_SETS: usize = 128;

fn grouping_call(e: &s::Expr) -> Option<&s::Function> {
    match e {
        s::Expr::Function(f)
            if f.over.is_none() && f.name.to_string().eq_ignore_ascii_case("grouping") =>
        {
            Some(f)
        }
        _ => None,
    }
}

fn contains_filter(e: &s::Expr) -> Result<bool> {
    let mut found = false;
    syntax::rewrite(&mut e.clone(), &mut |e| {
        found |= syntax::is_aggregate(e) && matches!(e, s::Expr::Function(f) if f.filter.is_some());
        Ok(false)
    })?;
    Ok(found)
}

fn lazy(condition: s::Expr, value: s::Expr) -> s::Expr {
    use s::helpers::attached_token::AttachedToken;
    s::Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![s::CaseWhen {
            condition,
            result: value,
        }],
        else_result: None,
    }
}

/// Whether DuckDB, which evaluates a FILTER argument on every row, cannot
/// raise on a filtered-out row of `e`. Total expressions qualify. So do
/// division (DuckDB returns NULL for a zero divisor of every numeric type,
/// where Assay raises for DECIMAL operands) and Float arithmetic (DuckDB
/// returns an infinity or NaN where Assay raises "floating arithmetic
/// overflow"), over operands that qualify themselves. Casts, exact
/// INTEGER/DECIMAL overflow and every other failure DuckDB also raises keep
/// the refusal. Assay evaluates the argument only on rows the filter keeps,
/// like PostgreSQL, so on kept rows its usual errors are unchanged.
pub(super) fn filtered_rows_agree(e: &Expr, input: &Node) -> bool {
    if membership_safety::expression_is_total(e, input) {
        return true;
    }
    let numeric = |e: &Expr| matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Float);
    match &e.kind {
        Kind::Binary(Op::Div, a, b) => {
            numeric(a)
                && numeric(b)
                && filtered_rows_agree(a, input)
                && filtered_rows_agree(b, input)
        }
        Kind::Binary(Op::Add | Op::Sub | Op::Mul, a, b) if e.ty == Type::Float => {
            numeric(a)
                && numeric(b)
                && filtered_rows_agree(a, input)
                && filtered_rows_agree(b, input)
        }
        Kind::Neg(a) if e.ty == Type::Float => filtered_rows_agree(a, input),
        // Exact numeric to DOUBLE conversion of a precision-38 value is finite.
        Kind::Cast(a) if e.ty == Type::Float && numeric(a) => filtered_rows_agree(a, input),
        Kind::Function(name, a) if name == crate::mixed_numeric::PROMOTE => {
            filtered_rows_agree(a, input)
        }
        _ => false,
    }
}

fn lower(e: &mut s::Expr, input: &Node) -> Result<bool> {
    if !syntax::is_aggregate(e) {
        return Ok(false);
    }
    let s::Expr::Function(f) = e else {
        unreachable!()
    };
    if f.filter.is_none() {
        return Ok(true);
    }
    // Ordered-set and distribution aggregates keep their FILTER refusal: a
    // WITHIN GROUP fraction is their first argument, so the lowering below
    // does not apply (integration candidate 2).
    if matches!(
        f.name.to_string().to_lowercase().as_str(),
        "median"
            | "quantile"
            | "quantile_cont"
            | "quantile_disc"
            | "percentile_cont"
            | "percentile_disc"
            | "mode"
            | "approx_count_distinct"
    ) {
        return Err("unsupported: aggregate modifiers".into());
    }
    let condition = f.filter.take().expect("checked above");
    let s::FunctionArguments::List(args) = &mut f.args else {
        return Err("unsupported: aggregate arguments".into());
    };
    let total = |x: &s::Expr| -> Result<()> {
        let bound = binder(&input.schema).expr(x)?;
        if filtered_rows_agree(&bound, input) {
            Ok(())
        } else {
            Err("unsupported: aggregate FILTER over an argument that may fail (DuckDB evaluates it on filtered-out rows; PostgreSQL does not)".into())
        }
    };
    for clause in &args.clauses {
        if let s::FunctionArgumentClause::OrderBy(keys) = clause {
            for key in keys {
                total(&key.expr)?;
            }
        }
    }
    // Only the first argument is wrapped in the lazy CASE: a NULL there drops
    // the row from every aggregate admitted here (two-argument statistical
    // aggregates and ARG_MAX/ARG_MIN skip rows where any argument is NULL).
    // The other arguments are still evaluated on filtered-out rows, so each
    // must be total too.
    for argument in args.args.iter().skip(1) {
        if let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(value)) = argument {
            total(value)?;
        }
    }
    let Some(first) = args.args.first_mut() else {
        return Err("unsupported: aggregate FILTER without an argument".into());
    };
    match first {
        s::FunctionArg::Unnamed(s::FunctionArgExpr::Wildcard) => {
            *first = s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(lazy(
                *condition,
                s::Expr::Value(s::Value::Number("1".into(), false).into()),
            )));
        }
        s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(value)) => {
            total(value)?;
            let original = value.clone();
            *value = lazy(*condition, original.clone());
            // An ORDER BY key spelled as the argument itself (required for
            // STRING_AGG DISTINCT) stays identical to the lowered argument.
            let lowered = value.clone();
            for clause in &mut args.clauses {
                if let s::FunctionArgumentClause::OrderBy(keys) = clause {
                    for key in keys.iter_mut().filter(|key| key.expr == original) {
                        key.expr = lowered.clone();
                    }
                }
            }
        }
        _ => return Err("unsupported: aggregate FILTER argument".into()),
    }
    Ok(true)
}

/// Lower `FILTER (WHERE condition)` of a window aggregate call (already
/// detached from the call) into its first argument, like grouped FILTER.
/// Returns the original argument expressions; the caller checks that each is
/// total over the window input, as `lower` does for grouped aggregates.
pub(super) fn lower_call_filter(f: &mut s::Function, condition: s::Expr) -> Result<Vec<s::Expr>> {
    let s::FunctionArguments::List(args) = &mut f.args else {
        return Err("unsupported: aggregate arguments".into());
    };
    let originals = args
        .args
        .iter()
        .filter_map(|argument| match argument {
            s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(value)) => Some(value.clone()),
            _ => None,
        })
        .collect();
    let Some(first) = args.args.first_mut() else {
        return Err("unsupported: aggregate FILTER without an argument".into());
    };
    match first {
        s::FunctionArg::Unnamed(s::FunctionArgExpr::Wildcard) => {
            *first = s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(lazy(
                condition,
                s::Expr::Value(s::Value::Number("1".into(), false).into()),
            )));
        }
        s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(value)) => {
            *value = lazy(condition, value.clone());
        }
        _ => return Err("unsupported: aggregate FILTER argument".into()),
    }
    Ok(originals)
}

/// Lower FILTER in the SELECT list (including hidden ORDER BY items) and
/// HAVING. Unnamed outputs keep their original text as the column name.
pub(super) fn lower_filters(select: &mut s::Select, input: &Node) -> Result<()> {
    for item in &mut select.projection {
        match item {
            s::SelectItem::UnnamedExpr(e) if contains_filter(e)? => {
                let alias = s::Ident::with_quote('"', e.to_string());
                let mut expr = e.clone();
                syntax::rewrite(&mut expr, &mut |e| lower(e, input))?;
                *item = s::SelectItem::ExprWithAlias { expr, alias };
            }
            s::SelectItem::ExprWithAlias { expr, .. } if contains_filter(expr)? => {
                syntax::rewrite(expr, &mut |e| lower(e, input))?;
            }
            _ => {}
        }
    }
    if let Some(having) = &mut select.having
        && contains_filter(having)?
    {
        syntax::rewrite(having, &mut |e| lower(e, input))?;
    }
    Ok(())
}

pub(super) struct GroupingSets {
    pub keys: Vec<s::Expr>,
    pub sets: Vec<Vec<usize>>,
}

/// Plain GROUP BY lists return None. Elements multiply: each contributes a list
/// of alternatives and every grouping set concatenates one of each, keeping
/// duplicate sets (DuckDB and PostgreSQL both do).
pub(super) fn grouping_sets(group_by: &s::GroupByExpr) -> Result<Option<GroupingSets>> {
    let s::GroupByExpr::Expressions(elements, modifiers) = group_by else {
        return Ok(None);
    };
    if !modifiers.is_empty()
        || !elements.iter().any(|e| {
            matches!(
                e,
                s::Expr::Rollup(_) | s::Expr::Cube(_) | s::Expr::GroupingSets(_)
            )
        })
    {
        return Ok(None);
    }
    let mut keys: Vec<s::Expr> = Vec::new();
    let mut index = |e: &s::Expr| -> Result<usize> {
        if matches!(
            e,
            s::Expr::Rollup(_) | s::Expr::Cube(_) | s::Expr::GroupingSets(_)
        ) {
            return Err("unsupported: nested ROLLUP/CUBE/GROUPING SETS".into());
        }
        if matches!(e, s::Expr::Value(_)) {
            return Err(
                "unsupported: literals or ordinals inside ROLLUP/CUBE/GROUPING SETS".into(),
            );
        }
        Ok(keys.iter().position(|k| k == e).unwrap_or_else(|| {
            keys.push(e.clone());
            keys.len() - 1
        }))
    };
    let mut sets: Vec<Vec<usize>> = vec![Vec::new()];
    for element in elements {
        let alternatives: Vec<Vec<usize>> = match element {
            s::Expr::Rollup(lists) => {
                let lists = lists
                    .iter()
                    .map(|list| list.iter().map(&mut index).collect::<Result<Vec<_>>>())
                    .collect::<Result<Vec<_>>>()?;
                (0..=lists.len())
                    .rev()
                    .map(|n| lists[..n].concat())
                    .collect()
            }
            s::Expr::Cube(lists) => {
                let lists = lists
                    .iter()
                    .map(|list| list.iter().map(&mut index).collect::<Result<Vec<_>>>())
                    .collect::<Result<Vec<_>>>()?;
                if lists.len() > 7 {
                    return Err(format!("unsupported: more than {MAX_SETS} grouping sets").into());
                }
                (0..1_usize << lists.len())
                    .rev()
                    .map(|mask| {
                        lists
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| mask & (1 << (lists.len() - 1 - i)) != 0)
                            .flat_map(|(_, list)| list.iter().copied())
                            .collect()
                    })
                    .collect()
            }
            s::Expr::GroupingSets(lists) => lists
                .iter()
                .map(|list| list.iter().map(&mut index).collect::<Result<Vec<_>>>())
                .collect::<Result<Vec<_>>>()?,
            e => vec![vec![index(e)?]],
        };
        let mut next = Vec::new();
        for set in &sets {
            for alternative in &alternatives {
                let mut combined = set.clone();
                combined.extend(alternative);
                next.push(combined);
                if next.len() > MAX_SETS {
                    return Err(format!("unsupported: more than {MAX_SETS} grouping sets").into());
                }
            }
        }
        sets = next;
    }
    for set in &mut sets {
        set.sort_unstable();
        set.dedup();
    }
    Ok(Some(GroupingSets { keys, sets }))
}

/// Distinct GROUPING(...) calls in the SELECT list (including hidden ORDER BY
/// items) and HAVING.
pub(super) fn grouping_calls(select: &s::Select) -> Result<Vec<s::Expr>> {
    let mut calls = Vec::new();
    let mut visit = |e: &s::Expr| {
        syntax::rewrite(&mut e.clone(), &mut |e| {
            if grouping_call(e).is_some() {
                if !calls.contains(e) {
                    calls.push(e.clone());
                }
                return Ok(true);
            }
            Ok(false)
        })
    };
    for item in &select.projection {
        if let s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } = item {
            visit(e)?;
        }
    }
    if let Some(having) = &select.having {
        visit(having)?;
    }
    Ok(calls)
}

pub(super) struct GroupingInput {
    pub node: Node,
    pub expressions: Vec<s::Expr>,
    pub names: Vec<String>,
    pub grouped_columns: Vec<(usize, usize)>,
}

impl Planner<'_> {
    pub(super) fn grouping_input(
        &mut self,
        input: Node,
        keys: &[s::Expr],
        sets: &[Vec<usize>],
        aggregates: &[s::Expr],
        live: &[bool],
        calls: &[s::Expr],
    ) -> Result<GroupingInput> {
        let bound_keys = keys
            .iter()
            .map(|k| binder(&input.schema).expr(k))
            .collect::<Result<Vec<_>>>()?;
        // GROUPING(a, b, ...) = sum over arguments of (argument absent) << position,
        // the last argument being the least significant bit.
        let call_keys = calls
            .iter()
            .map(|call| {
                let f = grouping_call(call).expect("collected GROUPING call");
                let s::FunctionArguments::List(args) = &f.args else {
                    return Err("GROUPING requires grouping expressions as arguments".into());
                };
                if args.args.is_empty()
                    || args.duplicate_treatment.is_some()
                    || !args.clauses.is_empty()
                    || f.filter.is_some()
                {
                    return Err("GROUPING requires grouping expressions as arguments".into());
                }
                if args.args.len() > 62 {
                    return Err("unsupported: GROUPING with more than 62 arguments".into());
                }
                args.args
                    .iter()
                    .map(|argument| {
                        let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(e)) = argument else {
                            return Err("GROUPING arguments must be grouping expressions".into());
                        };
                        let bound = binder(&input.schema).expr(e).ok();
                        keys.iter()
                            .zip(&bound_keys)
                            .position(|(key, bound_key)| {
                                key == e || bound.as_ref().is_some_and(|b| b == bound_key)
                            })
                            .ok_or_else(|| {
                                crate::Error::from(
                                    "GROUPING arguments must be grouping expressions",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let names = (0..keys.len() + aggregates.len() + calls.len())
            .map(|_| self.fresh())
            .collect::<Vec<_>>();
        let mut live = live.to_vec();
        let mut hidden_count = Vec::new();
        if aggregates.is_empty() {
            // Every grouping set, including (), is an aggregation: one row per
            // group (and one row for () even over empty input).
            hidden_count.push(bind::parse("SELECT COUNT(*)").and_then(|q| match *q.body {
                s::SetExpr::Select(select) => match select.projection.into_iter().next() {
                    Some(s::SelectItem::UnnamedExpr(e)) => Ok(e),
                    _ => Err(crate::Error::from("internal: COUNT(*) template")),
                },
                _ => Err(crate::Error::from("internal: COUNT(*) template")),
            })?);
            live.push(true);
        }
        let mut union: Option<Node> = None;
        let mut types: Option<Vec<Type>> = None;
        for set in sets {
            let mut inner = bind::parse("SELECT 1")?;
            let s::SetExpr::Select(inside) = inner.body.as_mut() else {
                unreachable!()
            };
            inside.group_by = s::GroupByExpr::Expressions(
                set.iter().map(|&i| keys[i].clone()).collect(),
                Vec::new(),
            );
            inside.projection = set
                .iter()
                .map(|&i| &keys[i])
                .chain(aggregates)
                .chain(&hidden_count)
                .map(|e| s::SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: s::Ident::new(self.fresh()),
                })
                .collect();
            let mut node = unary(&inner, input.clone())?;
            empty_in::suppress_dead_inputs(&mut node, &live)?;
            let mut outputs = Vec::with_capacity(names.len());
            let mut set_types = Vec::with_capacity(names.len());
            for (key, bound) in bound_keys.iter().enumerate() {
                let (ty, kind) = match set.iter().position(|&i| i == key) {
                    Some(column) => (
                        Type::from_field(node.schema.field(column))?,
                        crate::expr::Kind::Column(column),
                    ),
                    None => (
                        bound.ty.clone(),
                        crate::expr::Kind::Literal(crate::expr::Literal::Null),
                    ),
                };
                set_types.push(ty.clone());
                outputs.push(bind::Output::Expr(Expr { ty, kind }));
            }
            for aggregate in 0..aggregates.len() {
                let column = set.len() + aggregate;
                let ty = Type::from_field(node.schema.field(column))?;
                set_types.push(ty.clone());
                outputs.push(bind::Output::Expr(Expr {
                    ty,
                    kind: crate::expr::Kind::Column(column),
                }));
            }
            for arguments in &call_keys {
                let value = arguments.iter().fold(0_i128, |bits, key| {
                    (bits << 1) | i128::from(!set.contains(key))
                });
                set_types.push(Type::Int);
                outputs.push(bind::Output::Expr(Expr {
                    ty: Type::Int,
                    kind: crate::expr::Kind::Literal(crate::expr::Literal::Num(value)),
                }));
            }
            if let Some(types) = &types {
                if *types != set_types {
                    return Err("internal: grouping set output types differ".into());
                }
            } else {
                types = Some(set_types.clone());
            }
            let plan = Plan {
                table: None,
                path: None,
                columns: (0..node.schema.fields().len()).collect(),
                filter: None,
                groups: Vec::new(),
                aggregates: Vec::new(),
                output: outputs,
                names: names.clone(),
                types: set_types,
                order: Vec::new(),
                aggregation: false,
                projection_pushdown: true,
            };
            let projected = Node {
                schema: column_projection::output_schema(&plan, &node.schema),
                estimated_rows: node.estimated_rows,
                op: RelOp::Unary {
                    plan,
                    input: Box::new(node),
                },
            };
            union = Some(match union {
                None => projected,
                Some(left) => Node {
                    schema: schema(&names, types.as_ref().expect("set types")),
                    estimated_rows: left.estimated_rows.saturating_add(projected.estimated_rows),
                    op: RelOp::UnionAll {
                        left: Box::new(left),
                        right: Box::new(projected),
                    },
                },
            });
        }
        let node = union.ok_or("internal: no grouping sets")?;
        let grouped_columns = bound_keys
            .iter()
            .enumerate()
            .filter_map(|(group, e)| match e.kind {
                crate::expr::Kind::Column(column) => Some((column, group)),
                _ => None,
            })
            .collect();
        let expressions = keys
            .iter()
            .chain(aggregates)
            .chain(calls)
            .cloned()
            .collect();
        Ok(GroupingInput {
            node,
            expressions,
            names,
            grouped_columns,
        })
    }
}
