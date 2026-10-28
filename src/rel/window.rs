//! Ranking and aggregate windows over the completed WHERE/GROUP BY/HAVING stream. Ordinals are
//! explicit sort keys and a second sort restores the input's canonical order.
use super::*;
use arrow::compute::{SortColumn, SortOptions};

mod aggregate;
mod buffered;
mod frames;
mod offset;
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
    pub aggregate: Option<bind::Agg>,
    pub offset: Option<offset::Offset>,
    pub value: Option<frames::ValueFunction>,
    pub frame: Frame,
    pub partition: Vec<Expr>,
    pub order: Vec<(Expr, bool, bool)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Frame {
    Partition,
    Range,
    Rows,
    Bounded {
        unit: frames::Unit,
        start: frames::Bound,
        end: frames::Bound,
        exclude: frames::Exclude,
    },
}

impl Specification {
    pub(crate) fn result_type(&self) -> Type {
        if let Some(value) = &self.value {
            return value.result_type();
        }
        self.offset.as_ref().map_or_else(
            || self.aggregate.as_ref().map_or(Type::Int, |a| a.ty.clone()),
            |o| o.input.ty.clone(),
        )
    }
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
        return Err(format!("unsupported: window function in {location}").into());
    }
    Ok(())
}

fn window_key(ident: &s::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// The named windows of one SELECT, in definition order.
struct NamedWindows {
    definitions: Vec<(String, s::Ident, s::WindowSpec)>,
    later: Vec<String>,
    used: Vec<bool>,
}
impl NamedWindows {
    fn lookup(&mut self, name: &s::Ident) -> Result<s::WindowSpec> {
        let key = window_key(name);
        if let Some(index) = self.definitions.iter().position(|(k, _, _)| *k == key) {
            self.used[index] = true;
            return Ok(self.definitions[index].2.clone());
        }
        if self.later.contains(&key) {
            return Err(format!("unsupported: window \"{}\" referenced before its definition (PostgreSQL rejects it; DuckDB accepts it)", name.value).into());
        }
        Err(format!(
            "unsupported: named window \"{}\" does not exist",
            name.value
        )
        .into())
    }

    /// SQL:2011 window refinement, as PostgreSQL 17 and DuckDB 1.5.6 apply it:
    /// the referenced window's PARTITION BY and ORDER BY are copied, ORDER BY
    /// may be added only when it has none, and it must not have a frame.
    fn refine(&mut self, spec: &s::WindowSpec) -> Result<s::WindowSpec> {
        let Some(name) = &spec.window_name else {
            return Ok(spec.clone());
        };
        let base = self.lookup(name)?;
        if base.window_frame.is_some() {
            return Err(format!(
                "cannot copy window \"{}\" because it has a frame clause",
                name.value
            )
            .into());
        }
        if !spec.partition_by.is_empty() {
            return Err(format!("unsupported: PARTITION BY in a window that references window \"{}\" (PostgreSQL rejects it; DuckDB accepts it)", name.value).into());
        }
        if !spec.order_by.is_empty() && !base.order_by.is_empty() {
            return Err(format!(
                "cannot override ORDER BY clause of window \"{}\"",
                name.value
            )
            .into());
        }
        Ok(s::WindowSpec {
            window_name: None,
            partition_by: base.partition_by,
            order_by: if spec.order_by.is_empty() {
                base.order_by
            } else {
                spec.order_by.clone()
            },
            window_frame: spec.window_frame.clone(),
        })
    }
}

/// Replace named window references (`OVER w`, `OVER (w ...)`) in the SELECT
/// list, QUALIFY and ORDER BY with inline specifications and drop the WINDOW
/// clause. Idempotent. An unused definition must still bind over `schema`:
/// PostgreSQL rejects an invalid unused window while DuckDB ignores it, so
/// such a query is refused rather than answered.
pub(super) fn resolve_named(
    select: &mut s::Select,
    order: &mut Option<s::OrderBy>,
    schema: &Schema,
) -> Result<()> {
    let mut windows = NamedWindows {
        definitions: Vec::new(),
        later: select
            .named_window
            .iter()
            .map(|s::NamedWindowDefinition(name, _)| window_key(name))
            .collect(),
        used: Vec::new(),
    };
    let mut referenced = false;
    for_each_window_expression(select, order, &mut |e| {
        syntax::rewrite(e, &mut |e| {
            if let s::Expr::Function(f) = e {
                referenced |= matches!(&f.over, Some(s::WindowType::NamedWindow(_)))
                    || matches!(&f.over, Some(s::WindowType::WindowSpec(spec)) if spec.window_name.is_some());
            }
            Ok(false)
        })
    })?;
    if select.named_window.is_empty() && !referenced {
        exclusion_names(select);
        return Ok(());
    }
    for s::NamedWindowDefinition(name, definition) in std::mem::take(&mut select.named_window) {
        let key = window_key(&name);
        windows
            .later
            .remove(windows.later.iter().position(|k| *k == key).unwrap());
        if windows.definitions.iter().any(|(k, _, _)| *k == key) {
            return Err(format!("window \"{}\" is already defined", name.value).into());
        }
        let spec = match &definition {
            s::NamedWindowExpr::WindowSpec(spec) => windows.refine(spec)?,
            s::NamedWindowExpr::NamedWindow(_) => {
                return Err("unsupported: WINDOW name AS other_name without parentheses".into());
            }
        };
        windows.definitions.push((key, name, spec));
        windows.used.push(false);
    }
    for_each_window_expression(select, order, &mut |e| {
        syntax::rewrite(e, &mut |e| {
            if let s::Expr::Function(f) = e {
                match &f.over {
                    Some(s::WindowType::NamedWindow(name)) => {
                        f.over = Some(s::WindowType::WindowSpec(windows.lookup(name)?));
                    }
                    Some(s::WindowType::WindowSpec(spec)) if spec.window_name.is_some() => {
                        f.over = Some(s::WindowType::WindowSpec(windows.refine(spec)?));
                    }
                    _ => {}
                }
            }
            Ok(false)
        })
    })?;
    for (index, (_, name, spec)) in windows.definitions.iter().enumerate() {
        if windows.used[index] {
            continue;
        }
        for e in spec
            .partition_by
            .iter()
            .chain(spec.order_by.iter().map(|key| &key.expr))
        {
            if contains(e)? || binder(schema).expr(e).is_err() {
                return Err(format!("unsupported: unused named window \"{}\" does not bind (PostgreSQL rejects it; DuckDB ignores it)", name.value).into());
            }
        }
        if spec.window_frame.is_some() {
            frames::parse(spec)?;
        }
    }
    exclusion_names(select);
    Ok(())
}

/// Restore EXCLUDE spelling in the names of unnamed window outputs.
fn exclusion_names(select: &mut s::Select) {
    for item in &mut select.projection {
        if let s::SelectItem::UnnamedExpr(e) = item {
            let text = e.to_string();
            if text.contains(crate::frame_exclusion::MARKER) {
                *item = s::SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: s::Ident::with_quote('"', crate::frame_exclusion::display(&text)),
                };
            }
        }
    }
}

fn for_each_window_expression(
    select: &mut s::Select,
    order: &mut Option<s::OrderBy>,
    f: &mut dyn FnMut(&mut s::Expr) -> Result<()>,
) -> Result<()> {
    if let Some(s::Distinct::On(keys)) = &mut select.distinct {
        for key in keys {
            f(key)?;
        }
    }
    for item in &mut select.projection {
        if let s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } = item {
            f(e)?;
        }
    }
    if let Some(e) = &mut select.qualify {
        f(e)?;
    }
    if let Some(s::OrderBy {
        kind: s::OrderByKind::Expressions(keys),
        ..
    }) = order
    {
        for key in keys {
            f(&mut key.expr)?;
        }
    }
    Ok(())
}

pub(super) fn validate_clauses(select: &s::Select) -> Result<()> {
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
    Ok(())
}

pub(super) fn validate_select(select: &s::Select) -> Result<()> {
    validate_clauses(select)?;
    for item in &select.projection {
        if let s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } = item {
            syntax::rewrite(&mut expr.clone(), &mut |expression| {
                if syntax::is_aggregate(expression) && contains(expression)? {
                    return Err("unsupported: window function inside aggregate".into());
                }
                if let s::Expr::Function(function) = expression
                    && function.over.is_some()
                {
                    validate_function(function)?;
                }
                Ok(false)
            })?;
        }
    }
    Ok(())
}

/// Aggregate functions accepted with OVER. STRING_AGG and the PERCENTILE_*
/// and MODE() WITHIN GROUP forms stay refused (see SQL-COVERAGE.md). Window
/// MODE breaks ties like grouped MODE, by the smallest value (decision
/// 2026-10-18; DuckDB picks by frame position).
pub(super) const AGGREGATES: &[&str] = &[
    "sum",
    "count",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "covar_samp",
    "covar_pop",
    "corr",
    "regr_slope",
    "regr_intercept",
    "regr_count",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
    "bool_and",
    "bool_or",
    "count_if",
    "arg_max",
    "arg_min",
    "max_by",
    "min_by",
    "median",
    "quantile",
    "quantile_cont",
    "quantile_disc",
    "mode",
];

fn two_argument(name: &str) -> bool {
    matches!(
        name,
        "covar_samp"
            | "covar_pop"
            | "corr"
            | "regr_slope"
            | "regr_intercept"
            | "regr_count"
            | "regr_r2"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_syy"
            | "regr_sxy"
            | "arg_max"
            | "arg_min"
            | "max_by"
            | "min_by"
            | "quantile"
            | "quantile_cont"
            | "quantile_disc"
    )
}

pub(super) fn ordered_set(name: &str) -> bool {
    matches!(
        name,
        "median" | "quantile" | "quantile_cont" | "quantile_disc" | "mode"
    )
}

fn validate_function(function: &s::Function) -> Result<(Ranking, Frame, &s::WindowSpec)> {
    let name = function.name.to_string().to_lowercase();
    let aggregate = AGGREGATES.contains(&name.as_str());
    let kind = match name.as_str() {
        "row_number" => Ranking::RowNumber,
        "rank" => Ranking::Rank,
        "dense_rank" => Ranking::DenseRank,
        // Aggregate specifications carry their typed operation separately.
        "lag" | "lead" | "first_value" | "last_value" | "nth_value" | "ntile" | "percent_rank"
        | "cume_dist" => Ranking::RowNumber,
        _ if aggregate => Ranking::RowNumber,
        _ => return Err(format!("unsupported: window function {name}").into()),
    };
    let Some(s::WindowType::WindowSpec(spec)) = &function.over else {
        return Err("unsupported: named window references".into());
    };
    if spec.window_name.is_some() {
        return Err("unsupported: named window references".into());
    }
    let offset = matches!(name.as_str(), "lag" | "lead");
    let value = matches!(
        name.as_str(),
        "first_value" | "last_value" | "nth_value" | "ntile" | "percent_rank" | "cume_dist"
    );
    let nullable = offset || matches!(name.as_str(), "first_value" | "last_value" | "nth_value");
    if offset && spec.order_by.is_empty() {
        return Err(format!("unsupported: window function {name} requires ORDER BY").into());
    }
    if !aggregate && !offset && !value && spec.window_frame.is_some() {
        return Err("unsupported: window frames (ROWS/RANGE/GROUPS)".into());
    }
    let frame = if offset {
        // LAG/LEAD ignore frames, as both references do.
        Frame::Partition
    } else {
        frames::parse(spec)?
    };
    let s::FunctionArguments::List(args) = &function.args else {
        return Err(if aggregate {
            "unsupported: aggregate window arguments/modifiers (including DISTINCT/FILTER)"
        } else if offset {
            "unsupported: offset window arguments/modifiers (including NULL treatment)"
        } else {
            "unsupported: ranking window arguments/modifiers"
        }
        .into());
    };
    let null_treatment = args
        .clauses
        .iter()
        .any(|c| matches!(c, s::FunctionArgumentClause::IgnoreOrRespectNulls(_)));
    let distinct = args.duplicate_treatment == Some(s::DuplicateTreatment::Distinct);
    let arity = args.args.len();
    let arity_ok = if aggregate {
        if two_argument(&name) {
            (2..=3).contains(&arity)
        } else {
            arity == 1
        }
    } else if offset {
        (1..=3).contains(&arity)
    } else if value {
        arity
            == match name.as_str() {
                "nth_value" => 2,
                "percent_rank" | "cume_dist" => 0,
                _ => 1,
            }
    } else {
        arity == 0
    };
    if null_treatment && !nullable {
        return Err("unsupported: IGNORE/RESPECT NULLS applies only to FIRST_VALUE, LAST_VALUE, NTH_VALUE, LAG and LEAD (DuckDB 1.5.6; PostgreSQL 17 has no null treatment)".into());
    }
    if (function.filter.is_some() && (!aggregate || ordered_set(&name)))
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || function.uses_odbc_syntax
        || !matches!(function.parameters, s::FunctionArguments::None)
        || !args
            .clauses
            .iter()
            .all(|c| matches!(c, s::FunctionArgumentClause::IgnoreOrRespectNulls(_)))
        || (distinct && !aggregate)
        || (!aggregate && args.duplicate_treatment.is_some())
        || !arity_ok
    {
        return Err(if aggregate {
            "unsupported: aggregate window arguments/modifiers (including DISTINCT/FILTER)"
        } else if offset {
            "unsupported: offset window arguments/modifiers (including NULL treatment)"
        } else {
            "unsupported: ranking window arguments/modifiers"
        }
        .into());
    }
    for argument in &args.args {
        match argument {
            s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) => {
                forbid(expr, "nested window arguments")?
            }
            s::FunctionArg::Unnamed(s::FunctionArgExpr::Wildcard)
                if name == "count" && !distinct => {}
            _ => {
                return Err(
                    "unsupported: aggregate window requires one expression (or COUNT(*))".into(),
                );
            }
        }
    }
    if let Some(filter) = &function.filter {
        forbid(filter, "FILTER")?;
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
    Ok((kind, frame, spec))
}

fn needs_tuple_order(node: &Node) -> bool {
    match &node.op {
        RelOp::Generate { .. } => false, // stable cursor encounter order
        RelOp::RecursiveCte { .. } | RelOp::RecursiveScan { .. } => true,
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
        RelOp::Generate { .. } => return Ok(()),
        RelOp::RecursiveCte { .. } | RelOp::RecursiveScan { .. } => return Ok(()),
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

pub(super) fn canonical_input(mut input: Node) -> Result<Node> {
    stabilize_limits(&mut input, false)?;
    if !needs_tuple_order(&input) || input.schema.fields().is_empty() {
        return Ok(input);
    }
    complete_tuple_order(&mut input);
    Ok(input)
}

/// Bind an aggregate called with OVER (FILTER already lowered) using the
/// grouped aggregate binders, so window and grouped results share types and
/// kernels.
fn bind_aggregate(
    name: &str,
    call: &s::Function,
    bind: &dyn Fn(&s::Expr) -> Result<Expr>,
    schema: &Schema,
) -> Result<bind::Agg> {
    let binder = bind::Binder {
        schema,
        table: None,
        facts: None,
    };
    if ordered_set(name) {
        return binder
            .ordered_set_with(call, bind)?
            .ok_or_else(|| "internal: ordered-set window aggregate".into());
    }
    let s::FunctionArguments::List(args) = &call.args else {
        unreachable!()
    };
    if two_argument(name) {
        return bind::statistics::statistical_with(name, args, bind)?
            .ok_or_else(|| "internal: two-argument window aggregate".into());
    }
    let distinct = args.duplicate_treatment == Some(s::DuplicateTreatment::Distinct);
    let argument = match &args.args[0] {
        s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) => Some(bind(expr)?),
        _ => None,
    };
    if matches!(name, "sum" | "avg")
        && argument.as_ref().is_some_and(|a| {
            !matches!(
                a.ty,
                Type::Int | Type::Decimal(_) | Type::Float | Type::Null
            )
        })
    {
        return Err(
            format!("unsupported: {name} OVER requires Int/Decimal/Float arguments").into(),
        );
    }
    let mut aggregate = bind::Agg::bind_input(name, argument, distinct)?;
    // DISTINCT cannot change an extreme (MIN/MAX, BOOL_AND/BOOL_OR).
    if matches!(
        aggregate.function,
        bind::Aggregate::Min | bind::Aggregate::Max
    ) {
        aggregate.distinct = false;
    }
    Ok(aggregate)
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
            let (kind, frame, spec) = validate_function(function)?;
            let mut spec = spec.clone();
            let aggregate_name = function.name.to_string().to_lowercase();
            let is_aggregate = AGGREGATES.contains(&aggregate_name.as_str());
            let is_offset = matches!(aggregate_name.as_str(), "lag" | "lead");
            let is_value = matches!(
                aggregate_name.as_str(),
                "first_value" | "last_value" | "nth_value" | "ntile" | "percent_rank" | "cume_dist"
            );
            // The aggregate call without OVER; FILTER becomes a conditional
            // first argument exactly as for grouped aggregates.
            let mut call = function.clone();
            call.over = None;
            let mut filtered = Vec::new();
            if let Some(condition) = call.filter.take() {
                filtered = aggregate_modifiers::lower_call_filter(&mut call, *condition)?;
            }
            let mut argument = if is_offset
                || (is_value
                    && !matches!(
                        aggregate_name.as_str(),
                        "ntile" | "percent_rank" | "cume_dist"
                    )) {
                let s::FunctionArguments::List(args) = &function.args else {
                    unreachable!()
                };
                match &args.args[0] {
                    s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
                    _ => None,
                }
            } else {
                None
            };
            if let Some(argument) = &mut argument {
                input = self.subqueries(argument, input.clone(), ctes)?;
            }
            if is_aggregate && let s::FunctionArguments::List(args) = &mut call.args {
                for argument in &mut args.args {
                    if let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) = argument {
                        input = self.subqueries(expr, input.clone(), ctes)?;
                    }
                }
            }
            for expression in &mut filtered {
                input = self.subqueries(expression, input.clone(), ctes)?;
            }
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
            // DuckDB evaluates a FILTERed argument on every row, PostgreSQL
            // only on admitted rows: every argument must be unable to raise an
            // error DuckDB also raises. The rule is grouped FILTER's
            // (aggregate_modifiers::filtered_rows_agree; integration candidate 4).
            for expression in &filtered {
                if !aggregate_modifiers::filtered_rows_agree(&bind(expression)?, &input) {
                    return Err("unsupported: aggregate FILTER over an argument that may fail (DuckDB evaluates it on filtered-out rows; PostgreSQL does not)".into());
                }
            }
            let aggregate = if is_aggregate {
                Some(bind_aggregate(
                    &aggregate_name,
                    &call,
                    &bind,
                    &input.schema,
                )?)
            } else {
                None
            };
            let specification = Specification {
                function: kind,
                aggregate,
                offset: if is_offset {
                    Some(offset::bind(
                        function,
                        bind(argument.as_ref().unwrap())?,
                        &bind,
                    )?)
                } else {
                    None
                },
                value: if is_value {
                    Some(frames::bind_value(
                        function,
                        argument.as_ref().map(&bind).transpose()?,
                    )?)
                } else {
                    None
                },
                frame,
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
            frames::validate(&specification)?;
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
            fields.push(Arc::new(
                specification.result_type().field(
                    &name,
                    specification.aggregate.is_some()
                        || specification.offset.is_some()
                        || specification
                            .value
                            .as_ref()
                            .is_some_and(|v| v.input.is_some()),
                ),
            ));
            input = Node {
                schema: Arc::new(Schema::new(fields)),
                estimated_rows: input.estimated_rows,
                op: RelOp::Window {
                    input: Box::new(input.clone()),
                    specification: Box::new(specification),
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
    let argument = specification
        .aggregate
        .as_ref()
        .and_then(|a| a.input.as_ref())
        .or_else(|| specification.offset.as_ref().map(|o| &o.input))
        .or_else(|| specification.value.as_ref().and_then(|v| v.input.as_ref()));
    let evaluated = keys.iter().copied().chain(argument).collect::<Vec<_>>();
    let ordinal_column = width + evaluated.len();
    let mut fields = child.schema.fields().to_vec();
    for (i, expression) in evaluated.iter().enumerate() {
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
    let reverse = specification.offset.as_ref().is_some_and(|o| o.lead);
    let order = (0..keys.len())
        .map(|i| {
            let (descending, nulls_first) = if i < specification.partition.len() {
                (false, false)
            } else {
                let (_, descending, nulls_first) =
                    &specification.order[i - specification.partition.len()];
                (*descending ^ reverse, *nulls_first ^ reverse)
            };
            bind::Sort {
                column: width + i,
                descending,
                nulls_first,
            }
        })
        .chain([bind::Sort {
            column: ordinal_column,
            descending: reverse,
            nulls_first: false,
        }])
        .collect();
    let mut sorter = Sorter::new(keyed_schema.clone(), order, ctx)?;
    let mut ordinal = 0_u64;
    child.run(ctx, &mut |batch| {
        let _metadata = budget.reserve((width + keys.len() + 1).saturating_mul(1024))?;
        let input = super::input(&batch, &budget, evaluated.iter().copied())?;
        let mut values = Vec::with_capacity(evaluated.len());
        for key in &evaluated {
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
        // Canonical private keys equate signed zeros and all NaN payloads.
        // The ordinal credit above funds replacement key buffers.
        arrays.extend(values.iter().enumerate().map(|(index, value)| {
            if index < keys.len() {
                crate::expr::numeric_compare::canonical_array(&value.array)
            } else {
                value.array.clone()
            }
        }));
        arrays.push(Arc::new(UInt64Array::from_iter_values(ordinal..end)));
        ordinal = end;
        sorter.push(
            RecordBatch::try_new(keyed_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )
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
    if let Some(offset) = &specification.offset {
        return offset::run(
            node,
            specification,
            offset,
            sorter,
            restore,
            keyed_schema,
            restore_schema,
            ctx,
            emit,
        );
    }
    // Ordered-set states are consumed by finalization, so running frames
    // refold them in the buffered path.
    let consuming = specification.aggregate.as_ref().is_some_and(|aggregate| {
        crate::execute::WindowAccumulator::consuming(aggregate)
            && specification.frame != Frame::Partition
    });
    if specification.value.is_some()
        || consuming
        || matches!(specification.frame, Frame::Bounded { .. })
    {
        return buffered::run(
            node,
            specification,
            sorter,
            restore,
            keyed_schema,
            restore_schema,
            ctx,
            emit,
        );
    }
    if let Some(aggregate) = &specification.aggregate {
        return aggregate::run(
            node,
            specification,
            aggregate,
            sorter,
            restore,
            keyed_schema,
            restore_schema,
            ctx,
            emit,
        );
    }
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
                .map(|array| {
                    take(array, &indices, None).map_err(|e| crate::Error::from(e.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            let last = RecordBatch::try_new(keyed_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?;
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
        restore.push(
            RecordBatch::try_new(restore_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )
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
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}

// Compare exact typed Arrow values, with SQL NULL equality for peering. Float
// uses SQL equality, including signed-zero and NaN peers.
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
            arrow::datatypes::DataType::Timestamp(_, _) => equal!(TimestampMicrosecondArray),
            arrow::datatypes::DataType::Utf8 => equal!(StringArray),
            arrow::datatypes::DataType::Binary => equal!(BinaryArray),
            arrow::datatypes::DataType::Boolean => equal!(BooleanArray),
            arrow::datatypes::DataType::Float64 => crate::expr::numeric_compare::real_equal(
                a.as_any().downcast_ref::<Float64Array>().unwrap().value(ai),
                b.as_any().downcast_ref::<Float64Array>().unwrap().value(bi),
            ),
            arrow::datatypes::DataType::Null => true,
            _ => unreachable!("bound window key type"),
        }
    })
}
