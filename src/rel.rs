//! Original relational planner and batched hash joins. No reference-engine execution.
use crate::{
    Result,
    bind::{self, Binder, Plan},
    execute::{self, Execution, Key, Metrics, Options, Prepared, key},
    expr::{Expr, Input, boolean, is_sql_null, normalize_input},
    memory::{Budget, Reservation},
    syntax,
    types::Type,
};
use arrow::{
    array::*,
    compute::{concat_batches, filter_record_batch, take},
    datatypes::{Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter},
};
use serde::Serialize;
use sqlparser::{
    ast as s,
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer},
};
mod branch_facts;
mod column_projection;
mod correlated;
mod empty_in;
mod funded_concat;
mod group_binding;
mod in_subquery;
mod ordered_batches;
mod probe;
mod replay;
mod set_ops;
mod shared_pipeline;
mod shared_retry;

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone, Serialize)]
pub struct Node {
    #[serde(serialize_with = "serialize_schema")]
    pub schema: SchemaRef,
    pub estimated_rows: usize,
    pub op: RelOp,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum JoinKind {
    Inner,
    Left,
    Semi,
    Anti,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CorrelatedMarker {
    Count,
    Exists { negated: bool },
}
#[derive(Debug, Clone, Serialize)]
pub enum RelOp {
    Scan {
        path: PathBuf,
        columns: Vec<usize>,
        distinct: Vec<usize>,
        statistics: Vec<crate::cardinality::ColumnStats>,
    },
    One,
    Empty,
    Materialize {
        id: usize,
        input: Box<Node>,
        shared_scan: bool,
    },
    UnionAll {
        left: Box<Node>,
        right: Box<Node>,
    },
    ScalarDefault {
        input: Box<Node>,
        fallback: Box<Node>,
        value: usize,
        marker: usize,
    },
    Scalar {
        input: Box<Node>,
    },
    Mark {
        left: Box<Node>,
        right: Box<Node>,
        value: Expr,
        negated: bool,
    },
    CorrelatedMark {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Expr,
        marker: CorrelatedMarker,
    },
    Unary {
        plan: Plan,
        input: Box<Node>,
    },
    Filter {
        predicate: Expr,
        input: Box<Node>,
    },
    Rename {
        input: Box<Node>,
    },
    Project {
        input: Box<Node>,
        columns: Vec<usize>,
    },
    Limit {
        count: usize,
        input: Box<Node>,
    },
    Join {
        left: Box<Node>,
        right: Box<Node>,
        kind: JoinKind,
        keys: Vec<(Expr, Expr)>,
        residual: Option<Expr>,
    },
    /// Planner-selected ordinary-equality membership for mixed/NULL/Float
    /// pairs. Keep the established same-type hash Mark operator separate.
    TypedMark {
        left: Box<Node>,
        right: Box<Node>,
        value: Expr,
        negated: bool,
    },
}
fn schema(names: &[String], types: &[Type]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .zip(types)
            .map(|(n, t)| t.field(n, true))
            .collect::<Vec<_>>(),
    ))
}
fn combined(a: &Schema, b: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        a.fields()
            .iter()
            .chain(b.fields())
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
    ))
}
fn binder(schema: &Schema) -> Binder<'_> {
    Binder {
        schema,
        table: None,
        facts: None,
    }
}
fn bind_node_expression(node: &Node, expression: &s::Expr) -> Result<Expr> {
    let facts = crate::mixed_numeric::binding_facts(node)?;
    Binder {
        schema: &node.schema,
        table: None,
        facts: Some(&facts),
    }
    .expr(expression)
}
fn bind_pair_expression(left: &Node, right: &Node, expression: &s::Expr) -> Result<Expr> {
    let mut facts = crate::mixed_numeric::binding_facts(left)?;
    facts.extend(crate::mixed_numeric::binding_facts(right)?);
    Binder {
        schema: &combined(&left.schema, &right.schema),
        table: None,
        facts: Some(&facts),
    }
    .expr(expression)
}

fn identity(input: &Schema) -> Result<Plan> {
    // This is already a bound physical schema, not a user name lookup. Bind
    // by position so duplicate output aliases remain valid result columns.
    let types = input
        .fields()
        .iter()
        .map(|field| Type::from_field(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(Plan {
        table: None,
        path: None,
        columns: (0..types.len()).collect(),
        filter: None,
        groups: Vec::new(),
        aggregates: Vec::new(),
        output: types
            .iter()
            .enumerate()
            .map(|(index, ty)| {
                bind::Output::Expr(Expr {
                    ty: ty.clone(),
                    kind: crate::expr::Kind::Column(index),
                })
            })
            .collect(),
        names: input
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        types,
        order: Vec::new(),
        aggregation: false,
        projection_pushdown: true,
    })
}
pub fn prepare(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
) -> Result<Prepared> {
    prepare_with_views(sql, tables, options, &HashMap::new())
}

/// Session views are bound afresh against the current table snapshots.
#[derive(Clone)]
pub(crate) struct ViewDefinition {
    pub query: s::Query,
    pub columns: Vec<String>,
}

pub(crate) fn prepare_with_views(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
    views: &HashMap<String, &ViewDefinition>,
) -> Result<Prepared> {
    prepare_with_local_files(sql, tables, options, views, None)
}

pub(crate) type LocalReader<'a> = dyn Fn(&str, &std::path::Path) -> Result<PathBuf> + 'a;

pub(crate) fn prepare_with_local_files(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
    views: &HashMap<String, &ViewDefinition>,
    local_reader: Option<&LocalReader<'_>>,
) -> Result<Prepared> {
    let q = bind::parse(sql)?;
    let words = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter_map(|t| match t {
            Token::Word(w) => Some(w.value.to_lowercase()),
            _ => None,
        })
        .collect();
    let mut planner = Planner {
        tables,
        options,
        words,
        serial: 0,
        views,
        local_reader,
        active_views: Vec::new(),
        expanded_views: 0,
    };
    let mut root = crate::cte::repeated(planner.query(q, &HashMap::new())?);
    if options.projection_pushdown && options.relational_optimizations {
        root = crate::prune::prune(root)?;
    }
    if options.relational_optimizations {
        root = crate::cte::share_scans(root, options.memory_bytes)?;
    }
    let plan = identity(&root.schema)?;
    crate::mixed_numeric::validate_node(&root)?;
    Ok(Prepared {
        internal_output: false,
        schema: root.schema.clone(),
        plan,
        relational: Some(Box::new(root)),
    })
}
struct Planner<'a> {
    tables: &'a HashMap<String, PathBuf>,
    local_reader: Option<&'a LocalReader<'a>>,
    views: &'a HashMap<String, &'a ViewDefinition>,
    active_views: Vec<String>,
    expanded_views: usize,
    options: &'a Options,
    words: HashSet<String>,
    serial: usize,
}
impl Planner<'_> {
    fn fresh(&mut self) -> String {
        loop {
            self.serial += 1;
            let name = format!("__m2_{}", self.serial);
            if !self.words.contains(&name) {
                return name;
            }
        }
    }
    fn scan_path(&self, path: PathBuf, preserve_columns: bool) -> Result<Node> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let full = Type::external_schema(builder.schema());
        let mut columns = full
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                !self.options.projection_pushdown
                    || preserve_columns
                    || self.words.contains(&f.name().to_lowercase())
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if columns.is_empty() && !full.fields().is_empty() {
            columns.push(0);
        }
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|i| full.field(*i).clone())
                .collect::<Vec<_>>(),
        ));
        Ok(Node {
            schema,
            estimated_rows: builder.metadata().file_metadata().num_rows() as usize,
            op: RelOp::Scan {
                path,
                statistics: columns
                    .iter()
                    .map(|i| crate::cardinality::column_stats(builder.metadata(), *i))
                    .collect(),
                distinct: columns
                    .iter()
                    .map(|i| {
                        let mut low = i128::MAX;
                        let mut high = i128::MIN;
                        for group in builder.metadata().row_groups() {
                            use parquet::file::statistics::Statistics;
                            let range = match group.column(*i).statistics() {
                                Some(Statistics::Int32(s)) => s
                                    .min_opt()
                                    .zip(s.max_opt())
                                    .map(|(a, b)| (i128::from(*a), i128::from(*b))),
                                Some(Statistics::Int64(s)) => s
                                    .min_opt()
                                    .zip(s.max_opt())
                                    .map(|(a, b)| (i128::from(*a), i128::from(*b))),
                                _ => None,
                            };
                            if let Some((a, b)) = range {
                                low = low.min(a);
                                high = high.max(b);
                            } else {
                                return builder.metadata().file_metadata().num_rows() as usize;
                            }
                        }
                        usize::try_from(high.saturating_sub(low).saturating_add(1))
                            .unwrap_or(usize::MAX)
                            .min(builder.metadata().file_metadata().num_rows() as usize)
                            .max(1)
                    })
                    .collect(),
                columns,
            },
        })
    }
    fn relation(
        &mut self,
        t: &s::TableFactor,
        ctes: &HashMap<String, Node>,
        preserve_columns: bool,
    ) -> Result<Node> {
        // Unaliased parentheses preserve child qualifiers and association.
        // SQLite can retain inner qualifiers alongside an alias on a joined
        // table. Our single-qualifier schema cannot represent that scope yet.
        if let s::TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } = t
        {
            if alias.is_some() {
                return Err("unsupported: alias on parenthesized join".into());
            }
            return self.table_with_joins(table_with_joins, ctes, preserve_columns);
        }
        let (mut node, alias, base) = match t {
            s::TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                partitions,
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints,
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
                let name = name.to_string().to_lowercase();
                let node = if let Some(n) = ctes.get(&name) {
                    n.clone()
                } else if let Some(view) = self.views.get(&name).copied() {
                    self.options.cancellation.check()?;
                    if self.active_views.contains(&name) {
                        return Err(format!("semantic: circular view: {name}"));
                    }
                    self.expanded_views += 1;
                    if self.active_views.len() >= 64 || self.expanded_views > 256 {
                        return Err("view expansion memory/depth budget exceeded".into());
                    }
                    // A view's own filter/projection columns must survive scan pruning.
                    let tokens = Tokenizer::new(&GenericDialect {}, &view.query.to_string())
                        .tokenize()
                        .map_err(|e| e.to_string())?;
                    self.words
                        .extend(tokens.into_iter().filter_map(|token| match token {
                            Token::Word(word) => Some(word.value.to_lowercase()),
                            _ => None,
                        }));
                    self.active_views.push(name.clone());
                    // Persistent definitions cannot capture CTEs in the caller's scope.
                    let planned = self.query(view.query.clone(), &HashMap::new());
                    self.active_views.pop();
                    let mut planned = planned?;
                    if !view.columns.is_empty() {
                        if view.columns.len() != planned.schema.fields().len() {
                            return Err("semantic: view column count mismatch".into());
                        }
                        let fields = planned
                            .schema
                            .fields()
                            .iter()
                            .zip(&view.columns)
                            .map(|(field, name)| field.as_ref().clone().with_name(name))
                            .collect::<Vec<_>>();
                        planned = Node {
                            schema: Arc::new(Schema::new(fields)),
                            estimated_rows: planned.estimated_rows,
                            op: RelOp::Rename {
                                input: Box::new(planned),
                            },
                        };
                    }
                    planned
                } else {
                    let path = self
                        .tables
                        .get(&name)
                        .ok_or_else(|| format!("unknown table: {name}"))?
                        .clone();
                    self.scan_path(path, preserve_columns)?
                };
                (node, alias, Some(name))
            }
            s::TableFactor::Table {
                name,
                alias,
                args: Some(args),
                with_hints,
                version: None,
                partitions,
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints,
            } if name.0.len() == 1
                && with_hints.is_empty()
                && partitions.is_empty()
                && index_hints.is_empty()
                && matches!(
                    name.to_string().to_ascii_lowercase().as_str(),
                    "read_parquet" | "read_csv"
                ) =>
            {
                if args.settings.is_some() || args.args.len() != 1 {
                    return Err(
                        "unsupported: local reader requires exactly one literal path".into(),
                    );
                }
                let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(s::Expr::Value(value))) =
                    &args.args[0]
                else {
                    return Err("unsupported: local reader requires a string literal path".into());
                };
                let s::Value::SingleQuotedString(path) = &value.value else {
                    return Err("unsupported: local reader requires a string literal path".into());
                };
                let function = name.to_string().to_ascii_lowercase();
                let reader = self.local_reader.ok_or("unsupported: local file readers are disabled; enable the Session local-files capability")?;
                let path = reader(&function, std::path::Path::new(path))?;
                (
                    self.scan_path(path, preserve_columns)?,
                    alias,
                    Some(function),
                )
            }
            s::TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
            } => (self.query(*subquery.clone(), ctes)?, alias, None),
            _ => return Err(format!("unsupported: table factor {t}")),
        };
        // Relational schemas currently encode qualifiers in field-name strings.
        // Do not silently strip a legitimate dotted output/column alias when
        // exposing a previously unsupported wildcard projection.
        if preserve_columns
            && (node.schema.fields().iter().any(|f| f.name().contains('.'))
                || alias
                    .as_ref()
                    .is_some_and(|a| a.columns.iter().any(|c| c.name.value.contains('.'))))
        {
            return Err("unsupported: relational wildcard over dotted input names".into());
        }
        let qualifier = alias.as_ref().map(|a| a.name.value.to_lowercase()).or(base);
        if let Some(alias) = alias
            && !alias.columns.is_empty()
            && alias.columns.len() != node.schema.fields().len()
        {
            return Err("table alias column count mismatch".into());
        }
        let fields = node
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let name = alias
                    .as_ref()
                    .and_then(|a| a.columns.get(i))
                    .map(|c| c.name.value.clone())
                    .unwrap_or_else(|| f.name().rsplit('.').next().unwrap().into());
                f.as_ref().clone().with_name(if let Some(q) = &qualifier {
                    format!("{q}.{name}")
                } else {
                    name
                })
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        if schema != node.schema {
            node = Node {
                schema,
                estimated_rows: node.estimated_rows,
                op: RelOp::Rename {
                    input: Box::new(node),
                },
            };
        }
        Ok(node)
    }
    fn table_with_joins(
        &mut self,
        item: &s::TableWithJoins,
        ctes: &HashMap<String, Node>,
        preserve_columns: bool,
    ) -> Result<Node> {
        let mut node = self.relation(&item.relation, ctes, preserve_columns)?;
        for join in &item.joins {
            let right = self.relation(&join.relation, ctes, preserve_columns)?;
            let (kind, constraint) = match &join.join_operator {
                s::JoinOperator::Join(c) | s::JoinOperator::Inner(c) => (JoinKind::Inner, c),
                s::JoinOperator::Left(c) | s::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
                s::JoinOperator::LeftSemi(c) | s::JoinOperator::Semi(c) => (JoinKind::Semi, c),
                s::JoinOperator::LeftAnti(c) | s::JoinOperator::Anti(c) => (JoinKind::Anti, c),
                s::JoinOperator::CrossJoin => (JoinKind::Inner, &s::JoinConstraint::None),
                _ => return Err("unsupported: join kind".into()),
            };
            let on = match constraint {
                s::JoinConstraint::On(e) => syntax::conjuncts(e.clone()),
                s::JoinConstraint::None => Vec::new(),
                _ => return Err("unsupported: join requires ON".into()),
            };
            node = self.join(node, right, kind, on)?;
        }
        Ok(node)
    }
    fn query(&mut self, mut q: s::Query, parent_ctes: &HashMap<String, Node>) -> Result<Node> {
        if q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            return Err("unsupported: query modifiers".into());
        }
        let mut ctes = parent_ctes.clone();
        if let Some(with) = q.with.take() {
            if with.recursive {
                return Err("unsupported: recursive CTE".into());
            }
            for c in with.cte_tables {
                let mut node = self.query(*c.query, &ctes)?;
                if self.options.relational_optimizations {
                    self.serial += 1;
                    node = Node {
                        schema: node.schema.clone(),
                        estimated_rows: node.estimated_rows,
                        op: RelOp::Materialize {
                            id: self.serial,
                            input: Box::new(node),
                            shared_scan: false,
                        },
                    };
                }
                if !c.alias.columns.is_empty() {
                    return Err("unsupported: CTE column aliases".into());
                }
                ctes.insert(c.alias.name.value.to_lowercase(), node);
            }
        }
        if let s::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } = q.body.as_ref()
        {
            let union_all =
                *op == s::SetOperator::Union && *set_quantifier == s::SetQuantifier::All;
            if !union_all
                && (!matches!(
                    set_quantifier,
                    s::SetQuantifier::None | s::SetQuantifier::Distinct
                ) || !matches!(
                    op,
                    s::SetOperator::Union
                        | s::SetOperator::Intersect
                        | s::SetOperator::Except
                        | s::SetOperator::Minus
                ))
            {
                return Err("unsupported: set operation quantifier".into());
            }
            if q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some() {
                return Err("unsupported: UNION modifiers".into());
            }
            let mut a = q.clone();
            a.body = left.clone();
            let mut b = q.clone();
            b.body = right.clone();
            let left = self.query(a, &ctes)?;
            let right = self.query(b, &ctes)?;
            if left.schema.fields().len() != right.schema.fields().len() {
                return Err("UNION column count mismatch".into());
            }
            let types = left
                .schema
                .fields()
                .iter()
                .zip(right.schema.fields())
                .map(|(a, b)| bind::common_type(&Type::from_field(a)?, &Type::from_field(b)?))
                .collect::<Result<Vec<_>>>()?;
            let names = left
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            if !union_all {
                return set_ops::distinct(left, right, *op, names, types);
            }
            return Ok(Node {
                schema: schema(&names, &types),
                estimated_rows: left.estimated_rows.saturating_add(right.estimated_rows),
                op: RelOp::UnionAll {
                    left: Box::new(left),
                    right: Box::new(right),
                },
            });
        }
        let s::SetExpr::Select(select) = q.body.as_mut() else {
            return Err("unsupported: set operations".into());
        };
        let mut predicates = select
            .selection
            .take()
            .map(syntax::conjuncts)
            .unwrap_or_default();
        let implied = predicates.iter().flat_map(common_terms).collect::<Vec<_>>();
        for p in implied {
            if !predicates.contains(&p) {
                predicates.push(p);
            }
        }
        if self.options.relational_optimizations {
            predicates.extend(equality_closure(&predicates));
        }
        let mut relations = Vec::new();
        let preserve_columns = bind::has_wildcards(&select.projection);
        for item in std::mem::take(&mut select.from) {
            relations.push(self.table_with_joins(&item, &ctes, preserve_columns)?);
        }
        if relations.is_empty() {
            relations.push(Node {
                schema: Arc::new(Schema::empty()),
                estimated_rows: 1,
                op: RelOp::One,
            });
        }
        if bind::has_wildcards(&select.projection) {
            let logical_schema = Schema::new(
                relations
                    .iter()
                    .flat_map(|node| node.schema.fields().iter().cloned())
                    .collect::<Vec<_>>(),
            );
            select.projection = bind::expand_wildcards(&select.projection, &logical_schema, None)?;
        }
        // Single-relation predicates are legal below inner joins. LEFT JOIN nodes
        // are treated as barriers: a WHERE predicate stays above that entire node.
        for node in &mut relations {
            if !self.options.relational_optimizations {
                continue;
            }
            let local = predicates
                .iter()
                .filter_map(|p| local_condition(p, &node.schema))
                .collect::<Vec<_>>();
            // Fully local predicates are already present in `local` above.
            predicates.retain(|p| binder(&node.schema).expr(p).is_err());
            if let Some(p) = syntax::and(local) {
                let expr = bind_node_expression(node, &p)?.simplify_boolean();
                *node = filtered(node.clone(), expr, self.options.relational_optimizations)?;
            }
        }
        // Greedy connected join order from immutable Parquet row counts. Filter
        // estimates are heuristics, not measured statistics. No cross joins are
        // hidden; a missing equality uses a bounded-memory nested-loop path.
        if self.options.relational_optimizations {
            relations.sort_by_key(|n| n.estimated_rows);
        }
        let mut root = relations.remove(0);
        while !relations.is_empty() {
            let index = if !self.options.relational_optimizations {
                0
            } else {
                relations
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, n)| join_estimate(&root, n, &predicates))
                    .unwrap()
                    .0
            };
            let right = relations.remove(index);
            let schema = combined(&root.schema, &right.schema);
            let mut ready = Vec::new();
            predicates.retain(|p| {
                if binder(&schema).expr(p).is_ok() {
                    ready.push(p.clone());
                    false
                } else {
                    true
                }
            });
            // Build the accumulated smaller side; stream the newly joined table.
            root = if root.estimated_rows <= right.estimated_rows {
                self.join(right, root, JoinKind::Inner, ready)?
            } else {
                self.join(root, right, JoinKind::Inner, ready)?
            };
        }
        let mut remaining = Vec::new();
        for mut predicate in predicates {
            match predicate {
                s::Expr::InSubquery {
                    expr,
                    subquery,
                    negated: false,
                } if self.options.relational_optimizations => {
                    // Only a top-level positive WHERE conjunct: UNKNOWN rejects
                    // the row here, unlike IN used as a projected boolean.
                    let value = bind_node_expression(&root, &expr)?;
                    let right = self.query(*subquery, &ctes)?;
                    let right_type = in_subquery::validate(&value, &right.schema)?;
                    root = if in_subquery::hash_equality(&value.ty, &right_type) {
                        membership(root, right, value)?
                    } else {
                        in_subquery::positive(root, right, value, &self.fresh())
                    };
                }
                s::Expr::Exists { subquery, negated } => {
                    root = self.exists(root, *subquery, negated, &ctes)?;
                }
                _ => {
                    root = self.subqueries(&mut predicate, root, &ctes)?;
                    remaining.push(predicate);
                }
            }
        }
        if !remaining.is_empty() {
            let e = syntax::and(remaining).unwrap();
            let bound = bind_node_expression(&root, &e)?;
            root = filtered(root, bound, self.options.relational_optimizations)?;
        }
        self.select(q, root, &ctes)
    }
    fn join(
        &self,
        mut left: Node,
        mut right: Node,
        kind: JoinKind,
        predicates: Vec<s::Expr>,
    ) -> Result<Node> {
        let all = combined(&left.schema, &right.schema);
        let mut keys = Vec::new();
        let mut residual = Vec::new();
        let mut left_filters = Vec::new();
        let mut right_filters = Vec::new();
        for p in predicates {
            // Validate in the original scope before testing one side; otherwise
            // an ambiguous unqualified name could become accidentally accepted.
            binder(&all).expr(&p)?;
            if let Some(k) = join_key_nodes(&p, &left, &right) {
                keys.push(k);
            } else if self.options.relational_optimizations
                && binder(&right.schema).expr(&p).is_ok()
            {
                right_filters.push(p);
            } else if self.options.relational_optimizations
                && matches!(kind, JoinKind::Inner | JoinKind::Semi)
                && binder(&left.schema).expr(&p).is_ok()
            {
                left_filters.push(p);
            } else {
                residual.push(p);
            }
        }
        if let Some(p) = syntax::and(left_filters) {
            let expr = bind_node_expression(&left, &p)?.simplify_boolean();
            left = filtered(left, expr, self.options.relational_optimizations)?;
        }
        if let Some(p) = syntax::and(right_filters) {
            let expr = bind_node_expression(&right, &p)?.simplify_boolean();
            right = filtered(right, expr, self.options.relational_optimizations)?;
        }
        if self.options.relational_optimizations {
            crate::equivalence::reduce_join_keys(&left, &right, &mut keys);
        }
        let residual = syntax::and(residual)
            .map(|e| bind_pair_expression(&left, &right, &e))
            .transpose()?;
        let schema = if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
            left.schema.clone()
        } else {
            all
        };
        Ok(Node {
            schema,
            estimated_rows: estimate_keys(&left, &right, &keys).max(if kind == JoinKind::Left {
                left.estimated_rows
            } else {
                0
            }),
            op: RelOp::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                keys,
                residual,
            },
        })
    }
    fn local_schema(&mut self, q: &s::Query, ctes: &HashMap<String, Node>) -> Result<SchemaRef> {
        let s::SetExpr::Select(select) = q.body.as_ref() else {
            return Err("unsupported: subquery set operation".into());
        };
        let mut fields = Vec::new();
        for from in &select.from {
            fields.extend(
                self.relation(&from.relation, ctes, false)?
                    .schema
                    .fields()
                    .iter()
                    .cloned(),
            );
            for j in &from.joins {
                fields.extend(
                    self.relation(&j.relation, ctes, false)?
                        .schema
                        .fields()
                        .iter()
                        .cloned(),
                );
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }
    fn correlations(
        &mut self,
        q: &mut s::Query,
        outer: &Schema,
        ctes: &HashMap<String, Node>,
    ) -> Result<(SchemaRef, Vec<s::Expr>)> {
        let local = self.local_schema(q, ctes)?;
        let s::SetExpr::Select(select) = q.body.as_mut() else {
            unreachable!()
        };
        let mut correlated = Vec::new();
        let mut predicates = Vec::new();
        for mut p in select
            .selection
            .take()
            .map(syntax::conjuncts)
            .unwrap_or_default()
        {
            let mut external = false;
            syntax::rewrite(&mut p, &mut |e| {
                if matches!(e, s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_))
                    && binder(&local).expr(e).is_err()
                {
                    binder(outer).expr(e)?;
                    external = true;
                }
                Ok(false)
            })?;
            if external {
                correlated.push(p);
            } else {
                predicates.push(p);
            }
        }
        select.selection = syntax::and(predicates);
        Ok((local, correlated))
    }
    fn exists(
        &mut self,
        left: Node,
        mut q: s::Query,
        negated: bool,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        if q.limit_clause.is_some() {
            return Err("unsupported: correlated EXISTS with LIMIT".into());
        }
        let (local, mut correlations) = self.correlations(&mut q, &left.schema, ctes)?;
        let s::SetExpr::Select(select) = q.body.as_mut() else {
            unreachable!()
        };
        if select.having.is_some()
            || !matches!(&select.group_by,s::GroupByExpr::Expressions(es,mods) if es.is_empty()&&mods.is_empty())
        {
            return Err("unsupported: EXISTS aggregation".into());
        }
        // Aggregate SELECT has a row even on empty input; do not rewrite it as
        // a row-existence join until that distinct semantic case is supported.
        for p in &select.projection {
            let mut aggregate = false;
            if let s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } = p
            {
                syntax::rewrite(&mut e.clone(), &mut |e| {
                    aggregate |= syntax::is_aggregate(e);
                    Ok(false)
                })?;
            }
            if aggregate {
                return Err("unsupported: EXISTS aggregate projection".into());
            }
        }
        let names = local
            .fields()
            .iter()
            .map(|_| self.fresh())
            .collect::<Vec<_>>();
        select.projection = local
            .fields()
            .iter()
            .zip(&names)
            .map(|(f, n)| s::SelectItem::ExprWithAlias {
                expr: syntax::ident(f.name()),
                alias: s::Ident::new(n),
            })
            .collect();
        for p in &mut correlations {
            syntax::rewrite(p, &mut |e| {
                if matches!(e, s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_))
                    && let Ok(bound) = binder(&local).expr(e)
                    && let crate::expr::Kind::Column(i) = bound.kind
                {
                    *e = syntax::ident(&names[i]);
                    return Ok(true);
                }
                Ok(false)
            })?;
        }
        let right = self.query(q, ctes)?;
        self.join(
            left,
            right,
            if negated {
                JoinKind::Anti
            } else {
                JoinKind::Semi
            },
            correlations,
        )
    }
    fn subqueries(
        &mut self,
        expr: &mut s::Expr,
        root: Node,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        let conditional = correlated::conditional_subqueries(expr)?;
        let mut root = Some(root);
        syntax::rewrite(expr, &mut |e| match e.clone() {
            s::Expr::InList { ref list, .. } if list.is_empty() => {
                self.bind_empty_in(e, root.as_ref().unwrap(), ctes)
            }
            s::Expr::Exists { subquery, negated } => {
                if conditional.contains(subquery.as_ref()) {
                    return Err(
                        "unsupported: conditional EXISTS requires masked subquery evaluation"
                            .into(),
                    );
                }
                let left = root.take().unwrap();
                let mut q = *subquery;
                let (local, correlations) = self.correlations(&mut q, &left.schema, ctes)?;
                let name = self.fresh();
                let next = self.correlated_marker(
                    left,
                    q,
                    local,
                    correlations,
                    CorrelatedMarker::Exists { negated },
                    &name,
                    ctes,
                )?;
                root = Some(next);
                *e = syntax::ident(&name);
                Ok(true)
            }
            s::Expr::Subquery(q) => {
                let conditional = conditional.contains(q.as_ref());
                let (next, name) = self.scalar(root.take().unwrap(), *q, ctes, conditional)?;
                root = Some(next);
                *e = syntax::ident(&name);
                Ok(true)
            }
            s::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let left = root.take().unwrap();
                let value = bind_node_expression(&left, &expr)?;
                let right = self.query(*subquery, ctes)?;
                let right_type = in_subquery::validate(&value, &right.schema)?;
                let hash_equality = in_subquery::hash_equality(&value.ty, &right_type);
                let name = self.fresh();
                let mut fields = left.schema.fields().iter().cloned().collect::<Vec<_>>();
                fields.push(Arc::new(Field::new(
                    &name,
                    arrow::datatypes::DataType::Boolean,
                    true,
                )));
                root = Some(Node {
                    schema: Arc::new(Schema::new(fields)),
                    estimated_rows: left.estimated_rows,
                    op: if hash_equality {
                        RelOp::Mark {
                            left: Box::new(left),
                            right: Box::new(right),
                            value,
                            negated,
                        }
                    } else {
                        RelOp::TypedMark {
                            left: Box::new(left),
                            right: Box::new(right),
                            value,
                            negated,
                        }
                    },
                });
                *e = syntax::ident(&name);
                Ok(true)
            }
            _ => Ok(false),
        })?;
        Ok(root.unwrap())
    }
    fn scalar(
        &mut self,
        left: Node,
        mut q: s::Query,
        ctes: &HashMap<String, Node>,
        conditional: bool,
    ) -> Result<(Node, String)> {
        let (local, correlations) = self.correlations(&mut q, &left.schema, ctes)?;
        let s::SetExpr::Select(select) = q.body.as_mut() else {
            unreachable!()
        };
        if select.projection.len() != 1 {
            return Err("scalar subquery requires one column".into());
        }
        let value = match &select.projection[0] {
            s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } => {
                e.clone()
            }
            _ => return Err("unsupported: scalar wildcard".into()),
        };
        let name = self.fresh();
        select.projection = vec![s::SelectItem::ExprWithAlias {
            expr: value.clone(),
            alias: s::Ident::new(&name),
        }];
        let mut conditions = Vec::new();
        if !correlations.is_empty() {
            if q.limit_clause.is_some()
                || select.having.is_some()
                || !matches!(&select.group_by,s::GroupByExpr::Expressions(es,mods) if es.is_empty()&&mods.is_empty())
            {
                return Err("unsupported: correlated scalar modifiers".into());
            }
            let mut aggregates = 0;
            syntax::rewrite(&mut value.clone(), &mut |e| {
                if syntax::is_aggregate(e) {
                    aggregates += 1;
                    return Ok(true);
                }
                Ok(false)
            })?;
            if aggregates == 0 {
                return Err("unsupported: correlated scalar requires aggregate".into());
            }
            if correlations
                .iter()
                .any(|p| !correlated::separable_equality(p, &local, &left.schema))
            {
                if conditional {
                    return Err("unsupported: conditional correlated COUNT requires masked subquery evaluation".into());
                }
                // Validate with the ordinary aggregate binder. Only COUNT(*)
                // enters this fallback; preserve the existing equality rewrite.
                let mut check = bind::parse("SELECT COUNT(*)")?;
                let s::SetExpr::Select(check_select) = check.body.as_mut() else {
                    unreachable!()
                };
                check_select.projection = select.projection.clone();
                let plan = bind::bind(&check, &local, None, true)?;
                if !matches!(plan.aggregates.as_slice(), [a] if matches!(a.function, bind::Aggregate::Count) && a.input.is_none() && !a.distinct)
                {
                    return Err(
                        "unsupported: non-equality correlated scalar requires COUNT(*)".into(),
                    );
                }
                let node = self.correlated_marker(
                    left,
                    q,
                    local,
                    correlations,
                    CorrelatedMarker::Count,
                    &name,
                    ctes,
                )?;
                return Ok((node, name));
            }
            let mut groups = Vec::new();
            for p in correlations {
                let s::Expr::BinaryOp {
                    left: a,
                    op: s::BinaryOperator::Eq,
                    right: b,
                } = p
                else {
                    return Err("unsupported: correlated scalar requires equality".into());
                };
                let (inner, outer) = if binder(&local).expr(&a).is_ok()
                    && binder(&left.schema).expr(&b).is_ok()
                {
                    (*a, *b)
                } else if binder(&local).expr(&b).is_ok() && binder(&left.schema).expr(&a).is_ok() {
                    (*b, *a)
                } else {
                    return Err("unsupported: correlation must separate scopes".into());
                };
                let key = self.fresh();
                groups.push(inner.clone());
                select.projection.push(s::SelectItem::ExprWithAlias {
                    expr: inner,
                    alias: s::Ident::new(&key),
                });
                conditions.push(s::Expr::BinaryOp {
                    left: Box::new(outer),
                    op: s::BinaryOperator::Eq,
                    right: Box::new(syntax::ident(&key)),
                });
            }
            select.group_by = s::GroupByExpr::Expressions(groups, Vec::new());
            let right = self.query(q, ctes)?;
            let column = left.schema.fields().len();
            let joined = self.join(left, right, JoinKind::Left, conditions)?;
            // Evaluate the same scalar expression over an empty input. This
            // preserves COUNT=0, SUM=NULL, CASE and arithmetic on missing groups.
            let mut empty_query = bind::parse("SELECT 1")?;
            let s::SetExpr::Select(empty_select) = empty_query.body.as_mut() else {
                unreachable!()
            };
            empty_select.projection = vec![s::SelectItem::ExprWithAlias {
                expr: value,
                alias: s::Ident::new(&name),
            }];
            let fallback = self.select(
                empty_query,
                Node {
                    schema: local,
                    estimated_rows: 0,
                    op: RelOp::Empty,
                },
                ctes,
            )?;
            return Ok((
                Node {
                    schema: joined.schema.clone(),
                    estimated_rows: joined.estimated_rows,
                    op: RelOp::ScalarDefault {
                        input: Box::new(joined),
                        fallback: Box::new(fallback),
                        value: column,
                        marker: column + 1,
                    },
                },
                name,
            ));
        }
        let right = self.query(q, ctes)?;
        let right = Node {
            schema: Arc::new(Schema::new(
                right
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            )),
            estimated_rows: 1,
            op: RelOp::Scalar {
                input: Box::new(right),
            },
        };
        Ok((self.join(left, right, JoinKind::Inner, conditions)?, name))
    }
    fn select(
        &mut self,
        mut q: s::Query,
        mut input: Node,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        let limit = q
            .limit_clause
            .take()
            .map(|c| -> Result<usize> {
                match c {
                    s::LimitClause::LimitOffset {
                        limit: Some(s::Expr::Value(v)),
                        offset: None,
                        limit_by,
                    } if limit_by.is_empty() => match v.value {
                        s::Value::Number(n, _) => {
                            n.parse::<usize>().map_err(|_| "invalid LIMIT".into())
                        }
                        _ => Err("unsupported: LIMIT".into()),
                    },
                    _ => Err("unsupported: LIMIT/OFFSET".into()),
                }
            })
            .transpose()?;
        let s::SetExpr::Select(select) = q.body.as_mut() else {
            unreachable!()
        };
        let distinct = match select.distinct.take() {
            None => false, // The parser also represents explicit SELECT ALL as None.
            Some(s::Distinct::Distinct) => true,
            Some(s::Distinct::On(_)) => return Err("unsupported: DISTINCT ON".into()),
        };
        // Resolve GROUP BY aliases against SELECT expressions (input names win).
        if let s::GroupByExpr::Expressions(groups, mods) = &mut select.group_by {
            if !mods.is_empty() {
                return Err("unsupported: group modifiers".into());
            }
            for g in groups {
                if binder(&input.schema).expr(g).is_err()
                    && let s::Expr::Identifier(i) = g
                    && let Some(s::SelectItem::ExprWithAlias{expr,..})=select.projection.iter().find(|p|matches!(p,s::SelectItem::ExprWithAlias{alias,..} if alias.value.eq_ignore_ascii_case(&i.value))){*g=expr.clone();}
            }
        }
        let groups = match &select.group_by {
            s::GroupByExpr::Expressions(es, mods) if mods.is_empty() => es.clone(),
            _ => return Err("unsupported: grouping".into()),
        };
        group_binding::having_aliases(select, &input.schema)?;
        let mut group_source_schema = None;
        let (aggregates, aggregate_live) = empty_in::aggregate_usage(select)?;
        if !groups.is_empty() || !aggregates.is_empty() {
            let mut inner = bind::parse("SELECT 1")?;
            let s::SetExpr::Select(inside) = inner.body.as_mut() else {
                unreachable!()
            };
            inside.group_by = select.group_by.clone();
            let expressions = groups
                .iter()
                .chain(&aggregates)
                .cloned()
                .collect::<Vec<_>>();
            let names = expressions.iter().map(|_| self.fresh()).collect::<Vec<_>>();
            inside.projection = expressions
                .iter()
                .zip(&names)
                .map(|(e, n)| s::SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: s::Ident::new(n),
                })
                .collect();
            let group_input_schema = input.schema.clone();
            group_source_schema = Some(group_input_schema.clone());
            input = unary(&inner, input)?;
            empty_in::suppress_dead_inputs(&mut input, &aggregate_live)?;
            let aggregate_constants = branch_facts::aggregate_constants(&input);
            // Bound column identity is independent of qualified/unqualified
            // spelling. Only actual column grouping keys are eligible: this
            // must not select arbitrary non-grouped input representatives.
            let RelOp::Unary { plan, .. } = &input.op else {
                unreachable!("grouping is a unary plan")
            };
            let grouped_columns = plan
                .groups
                .iter()
                .enumerate()
                .filter_map(|(group, expr)| {
                    if let crate::expr::Kind::Column(column) = expr.kind {
                        Some((column, group))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let mut replace = |e: &mut s::Expr| -> Result<bool> {
                let matched = expressions.iter().position(|a| a == e).or_else(|| {
                    if !grouped_columns.is_empty()
                        && matches!(e, s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_))
                    {
                        binder(&group_input_schema).expr(e).ok().and_then(|bound| {
                            if let crate::expr::Kind::Column(column) = bound.kind {
                                grouped_columns.iter().find_map(|&(source, group)| {
                                    (source == column).then_some(group)
                                })
                            } else {
                                None
                            }
                        })
                    } else {
                        None
                    }
                });
                if let Some(i) = matched {
                    *e = aggregate_constants
                        .get(i)
                        .cloned()
                        .flatten()
                        .unwrap_or_else(|| syntax::ident(&names[i]));
                    Ok(true)
                } else {
                    Ok(false)
                }
            };
            for p in &mut select.projection {
                let (e, alias) = match p {
                    s::SelectItem::UnnamedExpr(e) => {
                        (e.clone(), s::Ident::with_quote('"', e.to_string()))
                    }
                    s::SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), alias.clone()),
                    _ => unreachable!(),
                };
                let mut e = e;
                syntax::rewrite(&mut e, &mut replace)?;
                *p = s::SelectItem::ExprWithAlias { expr: e, alias };
            }
            if let Some(mut e) = select.having.take() {
                syntax::rewrite(&mut e, &mut replace)?;
                input = self.subqueries(&mut e, input, ctes)?;
                let bound = group_binding::expression(&e, &input, &group_input_schema)?;
                input = filtered(input, bound, self.options.relational_optimizations)?;
            }
            select.group_by = s::GroupByExpr::Expressions(Vec::new(), Vec::new());
        } else if select.having.is_some() {
            return Err("unsupported: HAVING without aggregation".into());
        }
        for p in &mut select.projection {
            empty_in::preserve_projection_name(p)?;
            if let s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } = p
            {
                input = self.subqueries(e, input, ctes)?;
            }
        }
        let mut root = if let Some(source) = group_source_schema {
            group_binding::projection(&q, input, &source)?
        } else {
            unary(&q, input)?
        };
        if distinct {
            root = set_ops::deduplicate_projection(root)?;
        }
        if let Some(count) = limit {
            root = Node {
                schema: root.schema.clone(),
                estimated_rows: root.estimated_rows.min(count),
                op: RelOp::Limit {
                    count,
                    input: Box::new(root),
                },
            };
        }
        Ok(root)
    }
}
// Push a positive membership restriction into the one inner-join input that
// supplies its value. The filtered side keeps exactly the same schema, so the
// surrounding equality/residual expressions retain their bound column indices.
fn membership(left: Node, right: Node, mut value: Expr) -> Result<Node> {
    let mut columns = std::collections::BTreeSet::new();
    value.columns(&mut columns);
    let Node {
        schema,
        estimated_rows,
        op,
    } = left;
    let op = match op {
        RelOp::Rename { input } => {
            return Ok(Node {
                schema,
                estimated_rows: (estimated_rows / 4).max(1),
                op: RelOp::Rename {
                    input: Box::new(membership(*input, right, value)?),
                },
            });
        }
        RelOp::Filter { input, predicate } => {
            return Ok(Node {
                schema,
                estimated_rows: (estimated_rows / 4).max(1),
                op: RelOp::Filter {
                    input: Box::new(membership(*input, right, value)?),
                    predicate,
                },
            });
        }
        RelOp::Join {
            left: a,
            right: b,
            kind: JoinKind::Inner,
            keys,
            residual,
        } if !columns.is_empty() => {
            let width = a.schema.fields().len();
            if columns.iter().all(|i| *i < width) {
                return Ok(Node {
                    schema,
                    estimated_rows: (estimated_rows / 4).max(1),
                    op: RelOp::Join {
                        left: Box::new(membership(*a, right, value)?),
                        right: b,
                        kind: JoinKind::Inner,
                        keys,
                        residual,
                    },
                });
            }
            if columns.iter().all(|i| *i >= width) {
                let map = (0..schema.fields().len())
                    .map(|i| i.checked_sub(width))
                    .collect::<Vec<_>>();
                value.remap_columns(&map)?;
                return Ok(Node {
                    schema,
                    estimated_rows: (estimated_rows / 4).max(1),
                    op: RelOp::Join {
                        left: a,
                        right: Box::new(membership(*b, right, value)?),
                        kind: JoinKind::Inner,
                        keys,
                        residual,
                    },
                });
            }
            RelOp::Join {
                left: a,
                right: b,
                kind: JoinKind::Inner,
                keys,
                residual,
            }
        }
        op => op,
    };
    let key = Expr {
        kind: crate::expr::Kind::Column(0),
        ty: value.ty.clone(),
    };
    let left = Node {
        schema: schema.clone(),
        estimated_rows,
        op,
    };
    Ok(Node {
        schema,
        estimated_rows: (estimated_rows / 4).max(1),
        op: RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: JoinKind::Semi,
            keys: vec![(value, key)],
            residual: None,
        },
    })
}

fn unary(q: &s::Query, input: Node) -> Result<Node> {
    let facts = crate::mixed_numeric::binding_facts(&input)?;
    let plan = bind::bind_with_facts(q, &input.schema, &facts)?;
    Ok(Node {
        schema: schema(&plan.names, &plan.types),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            plan,
            input: Box::new(input),
        },
    })
}
fn filtered(input: Node, predicate: Expr, estimates: bool) -> Result<Node> {
    if predicate.ty != Type::Bool {
        return Err("WHERE/ON requires boolean".into());
    }
    Ok(Node {
        schema: input.schema.clone(),
        estimated_rows: if estimates {
            crate::cardinality::filtered_rows(&input, &predicate)
        } else {
            (input.estimated_rows / 4).max(1)
        },
        op: RelOp::Filter {
            predicate,
            input: Box::new(input),
        },
    })
}
fn join_key_nodes(p: &s::Expr, left: &Node, right: &Node) -> Option<(Expr, Expr)> {
    if let s::Expr::Nested(p) = p {
        return join_key_nodes(p, left, right);
    }
    let s::Expr::BinaryOp {
        left: a,
        op: s::BinaryOperator::Eq,
        right: b,
    } = p
    else {
        return None;
    };
    for (a, b) in [(a, b), (b, a)] {
        if let (Ok(a), Ok(b)) = (
            bind_node_expression(left, a),
            bind_node_expression(right, b),
        ) {
            let mut columns = std::collections::BTreeSet::new();
            a.columns(&mut columns);
            if columns.is_empty() {
                continue;
            }
            columns.clear();
            b.columns(&mut columns);
            if !columns.is_empty() && a.ty == b.ty {
                return Some((a, b));
            }
        }
    }
    None
}

fn join_key(p: &s::Expr, left: &Schema, right: &Schema) -> Option<(Expr, Expr)> {
    if let s::Expr::Nested(p) = p {
        return join_key(p, left, right);
    }
    let s::Expr::BinaryOp {
        left: a,
        op: s::BinaryOperator::Eq,
        right: b,
    } = p
    else {
        return None;
    };
    for (a, b) in [(a, b), (b, a)] {
        if let (Ok(a), Ok(b)) = (binder(left).expr(a), binder(right).expr(b)) {
            let mut cols = std::collections::BTreeSet::new();
            a.columns(&mut cols);
            if cols.is_empty() {
                continue;
            }
            cols.clear();
            b.columns(&mut cols);
            if cols.is_empty() {
                continue;
            }
            if a.ty == b.ty {
                return Some((a, b));
            }
        }
    }
    None
}

#[derive(Clone)]
struct ScanKeyFilter {
    expressions: Vec<Expr>,
    members: Arc<HashSet<Box<[u8]>, ahash::RandomState>>,
    numeric: Option<Arc<crate::membership::NumericMembership>>,
    numeric_pair: Option<Arc<crate::membership::NumericPairMembership>>,
    _lease: Arc<Reservation>,
}
fn scan_key_mask(
    batch: &RecordBatch,
    filter: &ScanKeyFilter,
    budget: &Budget,
) -> Result<BooleanArray> {
    if let Some(mask) = crate::membership::direct_mask(
        batch,
        &filter.expressions,
        filter.numeric.as_deref(),
        filter.numeric_pair.as_deref(),
        budget,
    )? {
        return Ok(mask);
    }
    let values = input(batch, budget, &filter.expressions)?;
    let keys = filter
        .expressions
        .iter()
        .map(|e| e.eval(&values, budget))
        .collect::<Result<Vec<_>>>()?;
    if let Some(bitmap) = &filter.numeric {
        let numbers = crate::expr::numeric(&keys[0].array);
        Ok(BooleanArray::from_iter(
            numbers
                .iter()
                .map(|n| Some(n.is_some_and(|n| bitmap.contains(n)))),
        ))
    } else if let Some(pairs) = &filter.numeric_pair {
        let a = crate::expr::numeric(&keys[0].array);
        let b = crate::expr::numeric(&keys[1].array);
        Ok(BooleanArray::from_iter(a.iter().zip(b.iter()).map(
            |(a, b)| {
                Some(match (a, b) {
                    (Some(a), Some(b)) => pairs.contains(a, b),
                    _ => false,
                })
            },
        )))
    } else {
        let encoded = crate::keys::Keys::new(&keys, batch.num_rows(), budget)?;
        Ok(BooleanArray::from_iter((0..batch.num_rows()).map(|i| {
            Some(encoded.valid(i) && filter.members.contains(encoded.row(i)))
        })))
    }
}

fn numeric_membership(
    expressions: &[Expr],
    members: &HashSet<Box<[u8]>, ahash::RandomState>,
    budget: &Budget,
) -> Option<Arc<crate::membership::NumericMembership>> {
    if expressions.len() == 1 && matches!(expressions[0].ty, Type::Int | Type::Decimal(_)) {
        crate::membership::NumericMembership::new(members, budget).map(Arc::new)
    } else {
        None
    }
}
fn numeric_pair_membership(
    expressions: &[Expr],
    members: &HashSet<Box<[u8]>, ahash::RandomState>,
    budget: &Budget,
) -> Option<Arc<crate::membership::NumericPairMembership>> {
    crate::membership::NumericPairMembership::new(
        &expressions
            .iter()
            .map(|expr| expr.ty.clone())
            .collect::<Vec<_>>(),
        members,
        budget,
    )
    .map(Arc::new)
}
use crate::parquet_scan::FilterStats as ScanFilterStats;
pub(crate) struct Materialized {
    pub(crate) budget: Budget,
    pub(crate) batches: Vec<RecordBatch>,
    files: Vec<crate::spill::SpillFile>,
    _leases: Vec<Reservation>,
}
#[cfg(test)]
impl Materialized {
    pub(crate) fn from_test_batches(batches: Vec<RecordBatch>, budget: &Budget) -> Result<Self> {
        let leases = batches
            .iter()
            .map(|batch| budget.reserve(batch.get_array_memory_size() + 256))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            batches,
            files: Vec::new(),
            budget: budget.clone(),
            _leases: leases,
        })
    }
}
struct Context<'a> {
    shared_attempt: Option<Arc<crate::execute::pipeline::fallback::Attempt>>,
    options: &'a Options,
    scan_filters: HashMap<usize, ScanKeyFilter>,
    static_filters: HashMap<usize, Expr>,
    scan_transforms: HashMap<usize, Expr>,
    replay_transforms: HashMap<usize, Expr>,
    materialized: HashMap<usize, Arc<Materialized>>,
    spill: Option<crate::spill::SpillStore>,
    budget: Budget,
    metrics: Metrics,
}
pub(crate) struct ScanTarget {
    pub(crate) id: usize,
    expressions: Vec<Expr>,
    payload: usize,
    has_filter: bool,
    key_domain: usize,
}
// Only commute through row-preserving renames/projections and ordinary filters.
// Aggregates, limits, outer joins and scalar-cardinality nodes are barriers.
fn scan_target(node: &Node, mut expressions: Vec<Expr>) -> Result<Option<ScanTarget>> {
    match &node.op {
        RelOp::Scan { .. } => {
            let mut keys = std::collections::BTreeSet::new();
            for expr in &expressions {
                expr.columns(&mut keys);
            }
            // A separate key pass costs decoding/encoding/hashing every row.
            // Reserve it for scans with enough non-key payload to offset that.
            let payload = node
                .schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, _)| !keys.contains(i))
                .map(|(_, f)| match f.data_type() {
                    arrow::datatypes::DataType::Utf8 => 32,
                    arrow::datatypes::DataType::Decimal128(_, _) => 16,
                    arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Date32 => 4,
                    arrow::datatypes::DataType::Boolean => 1,
                    _ => 8,
                })
                .sum();
            let key_domain = filter_distinct(node, &expressions);
            Ok(Some(ScanTarget {
                key_domain,
                id: node as *const Node as usize,
                expressions,
                payload,
                has_filter: false,
            }))
        }
        RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => {
            // Store raw batches once, but apply each reference's runtime key
            // filter after lookup. Never filter the shared stored data itself.
            Ok(scan_target(input, expressions)?.map(|mut target| {
                target.id = node as *const Node as usize;
                target
            }))
        }
        RelOp::Rename { input } => scan_target(input, expressions),
        RelOp::Filter { input, .. } => Ok(scan_target(input, expressions)?.map(|mut target| {
            target.has_filter = true;
            target
        })),
        RelOp::Project { input, columns } => {
            let map = columns.iter().map(|i| Some(*i)).collect::<Vec<_>>();
            for expr in &mut expressions {
                expr.remap_columns(&map)?;
            }
            scan_target(input, expressions)
        }
        RelOp::Unary { input, plan }
            if !plan.aggregation && plan.order.is_empty() && plan.filter.is_none() =>
        {
            let map = plan
                .output
                .iter()
                .map(|o| match o {
                    bind::Output::Expr(Expr {
                        kind: crate::expr::Kind::Column(i),
                        ..
                    }) => Some(*i),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if map.iter().any(Option::is_none) {
                return Ok(None);
            }
            for expr in &mut expressions {
                expr.remap_columns(&map)?;
            }
            scan_target(input, expressions)
        }
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => {
            let mut needed = std::collections::BTreeSet::new();
            for expr in &expressions {
                expr.columns(&mut needed);
            }
            if needed.is_empty() {
                return Ok(None);
            }
            let width = left.schema.fields().len();
            if needed.iter().all(|i| *i < width) {
                scan_target(left, expressions)
            } else if needed.iter().all(|i| *i >= width) {
                let map = (0..node.schema.fields().len())
                    .map(|i| i.checked_sub(width))
                    .collect::<Vec<_>>();
                for expr in &mut expressions {
                    expr.remap_columns(&map)?;
                }
                scan_target(right, expressions)
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}
// A join can demand only the grouped keys present on its other input.
// Cross a grouped aggregate only through direct group columns. Never cross a
// scalar aggregate, LIMIT, materialized CTE, outer join or scalar-cardinality
// boundary. Keeping this separate from ordinary scan pushdown avoids treating
// every aggregate as a row-preserving operator.
pub(crate) fn aggregate_scan_target(
    node: &Node,
    mut expressions: Vec<Expr>,
) -> Result<Option<ScanTarget>> {
    match &node.op {
        RelOp::Rename { input } => aggregate_scan_target(input, expressions),
        RelOp::Filter { input, .. } => Ok(aggregate_scan_target(input, expressions)?.map(
            |mut target| {
                target.has_filter = true;
                target
            },
        )),
        RelOp::Project { input, columns } => {
            let map = columns.iter().map(|i| Some(*i)).collect::<Vec<_>>();
            for e in &mut expressions {
                e.remap_columns(&map)?;
            }
            aggregate_scan_target(input, expressions)
        }
        RelOp::Unary { input, plan } if plan.order.is_empty() && plan.filter.is_none() => {
            let map = plan
                .output
                .iter()
                .map(|output| {
                    let expr = match output {
                        bind::Output::Expr(e) if !plan.aggregation => Some(e),
                        bind::Output::Group(i) if plan.aggregation => Some(&plan.groups[*i]),
                        _ => None,
                    };
                    match expr.map(|e| &e.kind) {
                        Some(crate::expr::Kind::Column(i)) => Some(*i),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>();
            // A computed non-key output is harmless. A key requiring such an
            // expression is not movable by this conservative transformation.
            for e in &mut expressions {
                if e.remap_columns(&map).is_err() {
                    return Ok(None);
                }
            }
            if plan.aggregation {
                if plan.groups.is_empty()
                    || plan.aggregates.iter().any(|a| {
                        a.input
                            .as_ref()
                            .is_some_and(|e| !matches!(e.kind, crate::expr::Kind::Column(_)))
                    })
                {
                    return Ok(None);
                }
                scan_target(input, expressions)
            } else {
                aggregate_scan_target(input, expressions)
            }
        }
        _ => Ok(None),
    }
}

fn probe_chain(node: &Node) -> bool {
    match &node.op {
        RelOp::Scan { .. } => true,
        RelOp::Filter { input, .. } | RelOp::Project { input, .. } | RelOp::Rename { input } => {
            probe_chain(input)
        }
        _ => false,
    }
}

struct CachedProbe {
    batches: Vec<RecordBatch>,
    _leases: Vec<Reservation>,
}

// Admission-only estimate: use the joint domain for composite membership,
// rather than comparing tuple count with one component's NDV. Correlations can
// make this optimistic, so retain the strict selectivity and key-count guards.
// This estimate never prunes rows or changes join cardinality estimates.
fn filter_distinct(node: &Node, expressions: &[Expr]) -> usize {
    let mut seen = Vec::new();
    let mut estimate = 1_usize;
    for expression in expressions {
        if !seen.contains(&expression) {
            estimate = estimate.saturating_mul(distinct(node, expression));
            seen.push(expression);
        }
    }
    estimate.min(node.estimated_rows.max(1))
}

fn install_scan_filter(
    ctx: &mut Context<'_>,
    node: &Node,
    expressions: Vec<Expr>,
    keys: &crate::join_index::JoinIndex<'_>,
) -> Result<Option<usize>> {
    if !ctx.options.relational_optimizations || expressions.is_empty() {
        return Ok(None);
    }
    let cached_numeric = expressions.len() == 1
        && expressions[0].ty.scale().is_ok()
        && ctx
            .options
            .parquet_cache_bytes
            .is_some_and(|bytes| bytes > 0);
    let estimated_distinct = filter_distinct(node, &expressions);
    // Cached predicate decoding plus exact bitmap lookup admits moderately
    // selective numeric domains. Expensive encoded-key paths keep old limits.
    let (max_keys, multiplier) = if cached_numeric {
        (1 << 20, 4)
    } else {
        (65536, 16)
    };
    if keys.len() > max_keys || keys.len().saturating_mul(multiplier) >= estimated_distinct {
        return Ok(None);
    }
    let Some(ScanTarget {
        id,
        expressions,
        payload,
        has_filter,
        key_domain,
    }) = scan_target(node, expressions)?
    else {
        return Ok(None);
    };
    // A static filter already avoids downstream join work. Do not pay for a
    // moderately selective extra key pass over every physical input row.
    // Highly selective bitmaps still qualify, including larger key sets.
    if has_filter && keys.len().saturating_mul(16) >= key_domain {
        return Ok(None);
    }
    // A borrowed single-column bitmap avoids normalized/encoded key buffers.
    // With strong selectivity it can earn its cost on narrower payloads too.
    let cheap_column_bitmap = cached_numeric
        && matches!(expressions[0].kind, crate::expr::Kind::Column(_))
        && keys.len().saturating_mul(16) < key_domain;
    if payload < if cheap_column_bitmap { 16 } else { 32 } && !keys.is_empty() {
        return Ok(None);
    }
    // An outer join may already restrict this physical scan. Keep that filter;
    // skipping a redundant inner optimization leaves the full join checks intact.
    if ctx.scan_filters.contains_key(&id) {
        return Ok(None);
    }
    if cached_numeric
        && let Some(bitmap) =
            crate::membership::NumericMembership::from_keys(|| keys.keys(), &ctx.budget)
    {
        let Ok(lease) = ctx.budget.reserve(256) else {
            return Ok(None);
        };
        ctx.scan_filters.insert(
            id,
            ScanKeyFilter {
                expressions,
                members: Arc::new(HashSet::default()),
                numeric: Some(Arc::new(bitmap)),
                numeric_pair: None,
                _lease: Arc::new(lease),
            },
        );
        return Ok(Some(id));
    }
    // A wide domain or failed optional bitmap reservation must not create a
    // larger encoded-key set just because the cheaper bitmap path was eligible.
    if (payload < 32 && !keys.is_empty())
        || keys.len() > 65536
        || keys.len().saturating_mul(16) >= estimated_distinct
    {
        return Ok(None);
    }
    let bytes = keys.keys().try_fold(256_usize, |n, k| {
        n.checked_add(k.len().saturating_mul(2) + 128)
            .ok_or("runtime key filter size overflow")
    })?;
    let lease = Arc::new(ctx.budget.reserve(bytes)?);
    let members = keys.keys().map(Box::<[u8]>::from).collect();
    ctx.scan_filters.insert(
        id,
        ScanKeyFilter {
            numeric: numeric_membership(&expressions, &members, &ctx.budget),
            numeric_pair: numeric_pair_membership(&expressions, &members, &ctx.budget),
            expressions,
            members: Arc::new(members),
            _lease: lease,
        },
    );
    Ok(Some(id))
}

fn input<'a>(
    batch: &RecordBatch,
    budget: &Budget,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<Input> {
    let mut needed = std::collections::BTreeSet::new();
    for expr in expressions {
        expr.columns(&mut needed);
    }
    let mut columns = vec![None; batch.num_columns()];
    for i in needed {
        columns[i] = Some(normalize_input(
            batch.column(i),
            &Type::from_field(batch.schema().field(i))?,
            budget,
        )?);
    }
    Ok(Input {
        len: batch.num_rows(),
        columns,
    })
}

pub(crate) fn merge(a: &mut Metrics, b: &Metrics) {
    a.column_projection_batches += b.column_projection_batches;
    a.column_projection_rows += b.column_projection_rows;
    a.parallel_replays += b.parallel_replays;
    a.replay_tasks += b.replay_tasks;
    a.replay_batches += b.replay_batches;
    a.replay_input_rows += b.replay_input_rows;
    a.replay_exact_filter_batches += b.replay_exact_filter_batches;
    a.replay_pending_batches_peak = a
        .replay_pending_batches_peak
        .max(b.replay_pending_batches_peak);
    a.replay_pending_bytes_peak = a.replay_pending_bytes_peak.max(b.replay_pending_bytes_peak);
    a.parallel_joins += b.parallel_joins;
    a.parallel_join_tasks += b.parallel_join_tasks;
    a.parallel_join_batches += b.parallel_join_batches;
    a.parallel_join_pending_batches_peak = a
        .parallel_join_pending_batches_peak
        .max(b.parallel_join_pending_batches_peak);
    a.parallel_join_pending_bytes_peak = a
        .parallel_join_pending_bytes_peak
        .max(b.parallel_join_pending_bytes_peak);
    a.parallel_join_worker_wall_seconds += b.parallel_join_worker_wall_seconds;
    a.parallel_aggregate_batches += b.parallel_aggregate_batches;
    a.parallel_aggregate_worker_seconds += b.parallel_aggregate_worker_seconds;
    a.post_decode_scan_filters += b.post_decode_scan_filters;
    a.post_decode_filter_batches += b.post_decode_filter_batches;
    a.post_decode_filter_input_rows += b.post_decode_filter_input_rows;
    a.post_decode_filter_output_rows += b.post_decode_filter_output_rows;
    a.parallel_scan_tasks += b.parallel_scan_tasks;
    a.parallel_scan_batches += b.parallel_scan_batches;
    a.parallel_scan_pending_batches_peak = a
        .parallel_scan_pending_batches_peak
        .max(b.parallel_scan_pending_batches_peak);
    a.parallel_scan_worker_seconds += b.parallel_scan_worker_seconds;
    a.parallel_scan_caller_seconds += b.parallel_scan_caller_seconds;
    a.parallel_scan_caller_tasks += b.parallel_scan_caller_tasks;
    a.parallel_scan_backpressure_seconds += b.parallel_scan_backpressure_seconds;
    a.parallel_scan_pending_bytes_peak = a
        .parallel_scan_pending_bytes_peak
        .max(b.parallel_scan_pending_bytes_peak);
    a.parallel_scan_queue_slots_max = a
        .parallel_scan_queue_slots_max
        .max(b.parallel_scan_queue_slots_max);
    a.spilled_materializations += b.spilled_materializations;
    a.materialization_replay_batches += b.materialization_replay_batches;
    a.external_join_partitions += b.external_join_partitions;
    a.external_join_repartitions += b.external_join_repartitions;
    a.external_join_block_fallbacks += b.external_join_block_fallbacks;
    a.external_join_input_rows += b.external_join_input_rows;
    a.external_join_seconds += b.external_join_seconds;
    a.external_aggregate_partitions += b.external_aggregate_partitions;
    a.external_aggregate_repartitions += b.external_aggregate_repartitions;
    a.external_aggregate_distinct_passes += b.external_aggregate_distinct_passes;
    a.external_aggregate_rows += b.external_aggregate_rows;
    a.external_aggregate_seconds += b.external_aggregate_seconds;
    a.scan_decode_seconds += b.scan_decode_seconds;
    a.filter_seconds += b.filter_seconds;
    a.expression_seconds += b.expression_seconds;
    a.scan_expression_batches += b.scan_expression_batches;
    a.scan_group_key_batches += b.scan_group_key_batches;
    a.scan_expression_seconds += b.scan_expression_seconds;
    a.scan_normalize_seconds += b.scan_normalize_seconds;
    a.aggregate_seconds += b.aggregate_seconds;
    a.shared_expression_evaluations += b.shared_expression_evaluations;
    a.shared_aggregate_states += b.shared_aggregate_states;
    a.bounded_sum_batches += b.bounded_sum_batches;
    a.dense_group_rebuilds += b.dense_group_rebuilds;
    a.dense_group_batches += b.dense_group_batches;
    a.short_group_cache_batches += b.short_group_cache_batches;
    a.short_group_cache_hits += b.short_group_cache_hits;
    a.short_group_cache_misses += b.short_group_cache_misses;
    a.short_group_cache_bypasses += b.short_group_cache_bypasses;
    a.short_group_cache_peak_bytes = a
        .short_group_cache_peak_bytes
        .max(b.short_group_cache_peak_bytes);
    a.dense_group_index_peak_bytes = a
        .dense_group_index_peak_bytes
        .max(b.dense_group_index_peak_bytes);
    a.cte_materializations += b.cte_materializations;
    a.cte_reuses += b.cte_reuses;
    a.scan_reuses += b.scan_reuses;
    a.scan_materializations += b.scan_materializations;
    a.cached_scan_filters += b.cached_scan_filters;
    a.static_scan_filters += b.static_scan_filters;
    a.buffered_parquet_scans += b.buffered_parquet_scans;
    a.parquet_cached_column_rows += b.parquet_cached_column_rows;
    a.parquet_uncached_column_rows += b.parquet_uncached_column_rows;
    a.buffered_compressed_bytes_read += b.buffered_compressed_bytes_read;
    a.demand_filtered_aggregates += b.demand_filtered_aggregates;
    a.dense_joins += b.dense_joins;
    a.join_rejection_bitmaps += b.join_rejection_bitmaps;
    a.join_rejection_bitmap_bytes += b.join_rejection_bitmap_bytes;
    a.join_rejection_probes += b.join_rejection_probes;
    a.join_rejection_misses += b.join_rejection_misses;
    a.residual_candidate_rows += b.residual_candidate_rows;
    a.residual_gathered_bytes += b.residual_gathered_bytes;
    a.bitmap_scan_filters += b.bitmap_scan_filters;
    a.numeric_pair_scan_filters += b.numeric_pair_scan_filters;
    a.output_sort_seconds += b.output_sort_seconds;
    // Relational scan rows are counted at Scan; do not count each transform again.
    a.rows_after_filter += b.rows_after_filter;
    a.batches += b.batches;
    a.decoded_arrow_bytes_total += b.decoded_arrow_bytes_total;
    a.selected_column_compressed_bytes += b.selected_column_compressed_bytes;
}
impl Node {
    fn run(
        &self,
        ctx: &mut Context<'_>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let start = Instant::now();
        let cancel = ctx.options.cancellation.clone();
        cancel.check()?;
        let mut output_rows = 0;
        let result = self.run_inner(ctx, &mut |batch| {
            cancel.check()?;
            output_rows += batch.num_rows();
            emit(batch)
        });
        ctx.metrics.operator_profiles.push(serde_json::json!({"operator":match &self.op {RelOp::Scan{..}=>"Scan",RelOp::Join{..}=>"HashJoin",RelOp::Unary{plan,..}=>if plan.aggregation{"Aggregate"}else{"ProjectSort"},RelOp::Filter{..}=>"Filter",RelOp::Mark{..}=>"NullAwareMark",RelOp::TypedMark{..}=>"TypedNullAwareMark",RelOp::CorrelatedMark{..}=>"CorrelatedMark",RelOp::Scalar{..}=>"ScalarCardinality",RelOp::ScalarDefault{..}=>"ScalarEmptyDefault",RelOp::Rename{..}=>"Rename",RelOp::Project{..}=>"ColumnProject",RelOp::Limit{..}=>"Limit",RelOp::UnionAll{..}=>"UnionAll",RelOp::One=>"One",RelOp::Empty=>"Empty",RelOp::Materialize{shared_scan,..}=>if *shared_scan {"SharedScan"} else {"CteMaterialize"}},"estimated_rows":self.estimated_rows,"output_rows":output_rows,"inclusive_seconds":start.elapsed().as_secs_f64(),"timing_scope":"includes child execution and downstream consumer; do not sum"}));
        result
    }
    fn run_inner(
        &self,
        ctx: &mut Context<'_>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        match &self.op {
            RelOp::CorrelatedMark {
                left,
                right,
                predicate,
                marker,
            } => correlated::run(self, left, right, predicate, *marker, ctx, emit),
            RelOp::Materialize {
                id,
                input,
                shared_scan,
            } => {
                let stored = materialized_storage(*id, input, *shared_scan, ctx)?;
                let filter = if *shared_scan {
                    ctx.scan_filters
                        .get(&(self as *const Node as usize))
                        .cloned()
                } else {
                    None
                };
                if let Some(filter) = &filter {
                    ctx.metrics.dynamic_scan_filters += 1;
                    ctx.metrics.cached_scan_filters += 1;
                    ctx.metrics.bitmap_scan_filters += usize::from(filter.numeric.is_some());
                    ctx.metrics.numeric_pair_scan_filters +=
                        usize::from(filter.numeric_pair.is_some());
                }
                if *shared_scan
                    && ctx.options.relational_optimizations
                    && ctx.options.workers > 1
                    && ctx.spill.is_none()
                {
                    let predicate = ctx
                        .replay_transforms
                        .get(&(self as *const Node as usize))
                        .cloned();
                    if filter.is_some() || predicate.is_some() {
                        debug_assert!(stored.files.is_empty());
                        let stats = replay::run(
                            &stored.batches,
                            &self.schema,
                            filter.as_ref(),
                            predicate,
                            ctx.options,
                            &ctx.budget,
                            emit,
                        )?;
                        ctx.metrics.parallel_replays += usize::from(stats.parallel);
                        ctx.metrics.replay_tasks += stats.tasks;
                        ctx.metrics.replay_batches += stats.batches;
                        ctx.metrics.replay_input_rows += stats.input_rows;
                        ctx.metrics.replay_exact_filter_batches += stats.exact_batches;
                        ctx.metrics.replay_pending_batches_peak = ctx
                            .metrics
                            .replay_pending_batches_peak
                            .max(stats.pending_peak);
                        ctx.metrics.replay_pending_bytes_peak = ctx
                            .metrics
                            .replay_pending_bytes_peak
                            .max(stats.pending_bytes_peak);
                        ctx.metrics.filter_seconds += stats.filter_seconds;
                        return Ok(());
                    }
                }
                let mut replay = |batch: &RecordBatch| -> Result<()> {
                    if let Some(filter) = &filter {
                        let start = Instant::now();
                        let _lease = ctx.budget.reserve(
                            batch
                                .get_array_memory_size()
                                .saturating_mul(2)
                                .saturating_add(256),
                        )?;
                        let mask = scan_key_mask(batch, filter, &ctx.budget)?;
                        let filtered =
                            filter_record_batch(batch, &mask).map_err(|e| e.to_string())?;
                        ctx.metrics.filter_seconds += start.elapsed().as_secs_f64();
                        emit(filtered)
                    } else {
                        emit(batch.clone())
                    }
                };
                for batch in &stored.batches {
                    replay(batch)?;
                }
                for file in &stored.files {
                    let batch = file.read_batch()?;
                    replay(&batch.batch)?;
                }
                ctx.metrics.materialization_replay_batches += stored.files.len();
                Ok(())
            }
            RelOp::UnionAll { left, right } => {
                let mut send = |b: RecordBatch| {
                    let arrays = b
                        .columns()
                        .iter()
                        .zip(self.schema.fields())
                        .map(|(a, f)| {
                            arrow::compute::cast_with_options(
                                a,
                                f.data_type(),
                                &arrow::compute::CastOptions {
                                    safe: false,
                                    ..Default::default()
                                },
                            )
                            .map_err(|e| e.to_string())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    emit(
                        RecordBatch::try_new(self.schema.clone(), arrays)
                            .map_err(|e| e.to_string())?,
                    )
                };
                left.run(ctx, &mut send)?;
                right.run(ctx, &mut send)
            }
            RelOp::Empty => Ok(()),
            RelOp::ScalarDefault {
                input: child,
                fallback,
                value,
                marker,
            } => {
                let mut default = None;
                fallback.run(ctx, &mut |b| {
                    default = Some(b);
                    Ok(())
                })?;
                let default = default.ok_or("missing scalar empty-input result")?;
                let scalar = Scalar::new(default.column(0).clone());
                child.run(ctx, &mut |b| {
                    let mask = BooleanArray::from_iter(
                        (0..b.num_rows()).map(|i| Some(is_sql_null(b.column(*marker).as_ref(), i))),
                    );
                    let array = arrow::compute::kernels::zip::zip(
                        &mask,
                        &scalar,
                        &b.column(*value).as_ref(),
                    )
                    .map_err(|e| e.to_string())?;
                    let mut arrays = b.columns().to_vec();
                    arrays[*value] = array;
                    emit(
                        RecordBatch::try_new(self.schema.clone(), arrays)
                            .map_err(|e| e.to_string())?,
                    )
                })
            }
            RelOp::One => emit(
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    Vec::new(),
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                )
                .map_err(|e| e.to_string())?,
            ),
            RelOp::Scalar { input: child } => {
                let mut found = None;
                child.run(ctx, &mut |b| {
                    if b.num_rows() > 1 || (b.num_rows() > 0 && found.is_some()) {
                        return Err("scalar subquery returned multiple rows".into());
                    }
                    if b.num_rows() == 1 {
                        found = Some(b);
                    }
                    Ok(())
                })?;
                emit(found.unwrap_or_else(|| {
                    RecordBatch::try_new(
                        self.schema.clone(),
                        self.schema
                            .fields()
                            .iter()
                            .map(|f| new_null_array(f.data_type(), 1))
                            .collect(),
                    )
                    .unwrap()
                }))
            }
            RelOp::Mark {
                left,
                right,
                value,
                negated,
            } => {
                if ctx.spill.is_some() {
                    let right_key = Expr {
                        kind: crate::expr::Kind::Column(0),
                        ty: value.ty.clone(),
                    };
                    return self.spilled_join(
                        ctx,
                        emit,
                        left,
                        right,
                        crate::external_join::Mode::Mark { negated: *negated },
                        &[(value.clone(), right_key)],
                        None,
                    );
                }
                let budget = ctx.budget.clone();
                // Keys must drop before the reservations funding their payloads.
                let mut leases = Vec::new();
                let mut values = HashSet::new();
                let mut has_null = false;
                let mut empty = true;
                right.run(ctx, &mut |b| {
                    let v = normalize_input(b.column(0), &value.ty, &budget)?;
                    let _binary_scratch = execute::binary_key_scratch(&v, &budget)?;
                    for i in 0..b.num_rows() {
                        empty = false;
                        if is_sql_null(v.array.as_ref(), i) {
                            has_null = true;
                        } else {
                            let k = key(&v, &value.ty, i);
                            if !values.contains(&k) {
                                leases.push(budget.reserve(128 + k.payload_bytes())?);
                                values.insert(k);
                            }
                        }
                    }
                    Ok(())
                })?;
                left.run(ctx, &mut |b| {
                    let v = value.eval(&input(&b, &budget, [value])?, &budget)?;
                    let _binary_scratch = execute::binary_key_scratch(&v, &budget)?;
                    let mask = (0..b.num_rows()).map(|i| {
                        let k = key(&v, &value.ty, i);
                        let result = if empty {
                            Some(false)
                        } else if k == Key::Null {
                            None
                        } else if values.contains(&k) {
                            Some(true)
                        } else if has_null {
                            None
                        } else {
                            Some(false)
                        };
                        result.map(|v| v != *negated)
                    });
                    let mut arrays = b.columns().to_vec();
                    arrays.push(Arc::new(BooleanArray::from_iter(mask)));
                    emit(
                        RecordBatch::try_new(self.schema.clone(), arrays)
                            .map_err(|e| e.to_string())?,
                    )
                })
            }
            RelOp::Scan { path, columns, .. } => {
                let file = File::open(path).map_err(|e| e.to_string())?;
                let metadata = crate::parquet_scan::external_metadata(&file)?;
                ctx.metrics.selected_column_compressed_bytes += metadata
                    .metadata()
                    .row_groups()
                    .iter()
                    .flat_map(|g| columns.iter().map(|i| g.column(*i).compressed_size()))
                    .sum::<i64>();
                let projection = ProjectionMask::roots(metadata.parquet_schema(), columns.clone());
                let filter_stats = Arc::new(Mutex::new(ScanFilterStats::default()));
                let mut predicates: Vec<crate::parquet_scan::PredicateFactory> = Vec::new();
                if let Some(mut filter) = ctx
                    .scan_filters
                    .get(&(self as *const Node as usize))
                    .cloned()
                {
                    let mut needed = std::collections::BTreeSet::new();
                    for expr in &filter.expressions {
                        expr.columns(&mut needed);
                    }
                    let mut map = vec![None; columns.len()];
                    for (new, old) in needed.iter().enumerate() {
                        map[*old] = Some(new);
                    }
                    for expr in &mut filter.expressions {
                        expr.remap_columns(&map)?;
                    }
                    let physical = needed.iter().map(|i| columns[*i]).collect::<Vec<_>>();
                    let projection = ProjectionMask::roots(metadata.parquet_schema(), physical);
                    let budget = ctx.budget.clone();
                    ctx.metrics.bitmap_scan_filters += usize::from(filter.numeric.is_some());
                    ctx.metrics.numeric_pair_scan_filters +=
                        usize::from(filter.numeric_pair.is_some());
                    let stats = filter_stats.clone();
                    predicates.push(Arc::new(move |error| {
                        let filter = filter.clone();
                        let budget = budget.clone();
                        let stats = stats.clone();
                        Box::new(ArrowPredicateFn::new(
                            projection.clone(),
                            move |batch: RecordBatch| {
                                let start = Instant::now();
                                let result = scan_key_mask(&batch, &filter, &budget);
                                let mut stats = stats.lock().unwrap();
                                stats.rows += batch.num_rows();
                                stats.bytes += batch.get_array_memory_size();
                                stats.seconds += start.elapsed().as_secs_f64();
                                if let Err(message) = &result {
                                    *error.lock().unwrap() = Some(message.clone());
                                }
                                result.map_err(arrow::error::ArrowError::ComputeError)
                            },
                        ))
                    }));
                    ctx.metrics.dynamic_scan_filters += 1;
                }
                if let Some(mut expression) = ctx
                    .static_filters
                    .get(&(self as *const Node as usize))
                    .cloned()
                {
                    let mut needed = std::collections::BTreeSet::new();
                    expression.columns(&mut needed);
                    // Keep a column for constant predicates so batch cardinality
                    // survives the reader's physical projection.
                    if needed.is_empty() && !columns.is_empty() {
                        needed.insert(0);
                    }
                    let mut map = vec![None; columns.len()];
                    for (new, old) in needed.iter().enumerate() {
                        map[*old] = Some(new);
                    }
                    expression.remap_columns(&map)?;
                    let physical = needed.iter().map(|i| columns[*i]).collect::<Vec<_>>();
                    let projection = ProjectionMask::roots(metadata.parquet_schema(), physical);
                    let budget = ctx.budget.clone();
                    let stats = filter_stats.clone();
                    let count_rows = predicates.is_empty();
                    predicates.push(Arc::new(move |error| {
                        let expression = expression.clone();
                        let budget = budget.clone();
                        let stats = stats.clone();
                        Box::new(ArrowPredicateFn::new(
                            projection.clone(),
                            move |batch: RecordBatch| {
                                let start = Instant::now();
                                let result: Result<BooleanArray> = (|| {
                                    let values = input(&batch, &budget, [&expression])?;
                                    let mask = expression.eval(&values, &budget)?;
                                    Ok(boolean(&mask.array).clone())
                                })(
                                );
                                let mut stats = stats.lock().unwrap();
                                if count_rows {
                                    stats.rows += batch.num_rows();
                                }
                                stats.bytes += batch.get_array_memory_size();
                                stats.seconds += start.elapsed().as_secs_f64();
                                if let Err(message) = &result {
                                    *error.lock().unwrap() = Some(message.clone());
                                }
                                result.map_err(arrow::error::ArrowError::ComputeError)
                            },
                        ))
                    }));
                    ctx.metrics.static_scan_filters += 1;
                }
                let filtered = !predicates.is_empty();
                let transform_stats = Arc::new(Mutex::new(ScanFilterStats::default()));
                let transform: Option<crate::parquet_scan::BatchTransform> = ctx
                    .scan_transforms
                    .get(&(self as *const Node as usize))
                    .cloned()
                    .map(|expression| {
                        ctx.metrics.post_decode_scan_filters += 1;
                        let schema = metadata
                            .schema()
                            .project(columns)
                            .map_err(|e| e.to_string())?;
                        crate::parquet_scan::exact_filter(
                            expression,
                            &schema,
                            &ctx.budget,
                            &ctx.options.cancellation,
                            false,
                            transform_stats.clone(),
                        )
                    })
                    .transpose()?;
                let transformed = transform.is_some();
                let factory: crate::parquet_scan::FilterFactory = Arc::new(move |error| {
                    (!predicates.is_empty()).then(|| {
                        RowFilter::new(predicates.iter().map(|p| p(error.clone())).collect())
                    })
                });
                let scan_start = Instant::now();
                let mut reader = crate::parquet_scan::Reader::with_transform(
                    path,
                    file,
                    metadata,
                    projection,
                    factory,
                    transform,
                    ctx.options,
                    &ctx.budget,
                )?;
                ctx.metrics.scan_decode_seconds += scan_start.elapsed().as_secs_f64();
                loop {
                    ctx.options.cancellation.check()?;
                    let start = Instant::now();
                    let next = reader.next();
                    ctx.options.cancellation.check()?;
                    ctx.metrics.scan_decode_seconds += start.elapsed().as_secs_f64();
                    let Some(batch) = next else {
                        break;
                    };
                    let batch = batch?;
                    let bytes = batch.get_array_memory_size();
                    let _lease = ctx.budget.reserve(bytes)?;
                    if !transformed {
                        ctx.metrics.decoded_arrow_bytes_total += bytes;
                    }
                    if !filtered && !transformed {
                        ctx.metrics.rows_read += batch.num_rows();
                    }
                    emit(batch)?;
                }
                if filtered {
                    let stats = filter_stats.lock().unwrap();
                    ctx.metrics.rows_read += stats.rows;
                    ctx.metrics.decoded_arrow_bytes_total += stats.bytes;
                    if reader.parallel_statistics().is_none() {
                        ctx.metrics.scan_decode_seconds -= stats.seconds;
                    }
                    ctx.metrics.filter_seconds += stats.seconds;
                }
                if transformed {
                    let stats = transform_stats.lock().unwrap();
                    ctx.metrics.decoded_arrow_bytes_total += stats.bytes;
                    if !filtered {
                        ctx.metrics.rows_read += stats.rows;
                    }
                    if reader.parallel_statistics().is_none() {
                        ctx.metrics.scan_decode_seconds -= stats.seconds;
                    }
                    ctx.metrics.filter_seconds += stats.seconds;
                    ctx.metrics.post_decode_filter_batches += stats.batches;
                    ctx.metrics.post_decode_filter_input_rows += stats.rows;
                    ctx.metrics.post_decode_filter_output_rows += stats.output_rows;
                }
                if let Some(stats) = reader.parallel_statistics() {
                    ctx.metrics.parallel_scan_tasks += stats.tasks;
                    ctx.metrics.parallel_scan_batches += stats.batches;
                    ctx.metrics.parallel_scan_pending_batches_peak = ctx
                        .metrics
                        .parallel_scan_pending_batches_peak
                        .max(stats.pending_peak);
                    ctx.metrics.parallel_scan_worker_seconds += stats.worker_seconds;
                    ctx.metrics.parallel_scan_caller_seconds += stats.caller_seconds;
                    ctx.metrics.parallel_scan_caller_tasks += stats.caller_tasks;
                    ctx.metrics.parallel_scan_queue_slots_max = ctx
                        .metrics
                        .parallel_scan_queue_slots_max
                        .max(stats.queue_slots);
                    ctx.metrics.parallel_scan_backpressure_seconds += stats.backpressure_seconds;
                    ctx.metrics.parallel_scan_pending_bytes_peak = ctx
                        .metrics
                        .parallel_scan_pending_bytes_peak
                        .max(stats.pending_bytes_peak);
                }
                let (buffered, cached, uncached, bytes) = reader.statistics();
                ctx.metrics.buffered_parquet_scans += buffered;
                ctx.metrics.parquet_cached_column_rows += cached;
                ctx.metrics.parquet_uncached_column_rows += uncached;
                ctx.metrics.buffered_compressed_bytes_read += bytes;
                Ok(())
            }
            RelOp::Project { input, columns } => input.run(ctx, &mut |b| {
                emit(b.project(columns).map_err(|e| e.to_string())?)
            }),
            RelOp::Rename { input } => input.run(ctx, &mut |b| {
                emit(
                    RecordBatch::try_new(self.schema.clone(), b.columns().to_vec())
                        .map_err(|e| e.to_string())?,
                )
            }),
            RelOp::Limit { count, input } => {
                let mut remaining = *count;
                input.run(ctx, &mut |b| {
                    let n = remaining.min(b.num_rows());
                    remaining -= n;
                    if n > 0 {
                        emit(b.slice(0, n))?;
                    }
                    Ok(())
                })
            }
            RelOp::Filter {
                predicate,
                input: child,
            } => {
                let target = if ctx.options.relational_optimizations
                    && ctx
                        .options
                        .parquet_cache_bytes
                        .is_some_and(|bytes| bytes > 0)
                    // A row filter pays selection/reader overhead even when payload
                    // decoding falls. Reserve it for strongly selective estimates.
                    && self.estimated_rows <= child.estimated_rows / 16
                    && crate::scan_predicate::total(predicate)
                {
                    crate::scan_predicate::target(child, predicate.clone())?
                } else {
                    None
                };
                let installed = target.and_then(|(id, expression)| {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        ctx.static_filters.entry(id)
                    {
                        entry.insert(expression);
                        Some(id)
                    } else {
                        None
                    }
                });
                // Exact post-decode evaluation can cross only schema operations.
                // Unlike optional Parquet RowFilter selection, this keeps arbitrary
                // expression/error semantics and replaces the coordinator filter.
                let transformed = if ctx.options.relational_optimizations && ctx.options.workers > 1
                {
                    crate::scan_predicate::target(child, predicate.clone())?.and_then(
                        |(id, expression)| {
                            if let std::collections::hash_map::Entry::Vacant(entry) =
                                ctx.scan_transforms.entry(id)
                            {
                                entry.insert(expression);
                                Some(id)
                            } else {
                                None
                            }
                        },
                    )
                } else {
                    None
                };
                if let Some(id) = transformed {
                    let result = child.run(ctx, emit);
                    ctx.scan_transforms.remove(&id);
                    if let Some(id) = installed {
                        ctx.static_filters.remove(&id);
                    }
                    return result;
                }
                if ctx.options.relational_optimizations
                    && ctx.options.workers > 1
                    && ctx.spill.is_none()
                    && let Some((id, expression)) = replay::target(child, predicate.clone())?
                    && let std::collections::hash_map::Entry::Vacant(entry) =
                        ctx.replay_transforms.entry(id)
                {
                    entry.insert(expression);
                    let result = child.run(ctx, emit);
                    ctx.replay_transforms.remove(&id);
                    if let Some(id) = installed {
                        ctx.static_filters.remove(&id);
                    }
                    return result;
                }
                let budget = ctx.budget.clone();
                let mut seconds = 0.;
                let result = child.run(ctx, &mut |b| {
                    let start = Instant::now();
                    let mask = predicate.eval(&input(&b, &budget, [predicate])?, &budget)?;
                    let filtered =
                        filter_record_batch(&b, boolean(&mask.array)).map_err(|e| e.to_string())?;
                    let _lease = budget.reserve(filtered.get_array_memory_size())?;
                    seconds += start.elapsed().as_secs_f64();
                    if filtered.num_rows() > 0 {
                        emit(filtered)?;
                    }
                    Ok(())
                });
                if let Some(id) = installed {
                    ctx.static_filters.remove(&id);
                }
                ctx.metrics.filter_seconds += seconds;
                result
            }
            RelOp::Unary { plan, input: child } => {
                if let Some(metrics) = shared_pipeline::try_execute(plan, child, ctx, emit)? {
                    shared_pipeline::merge_metrics(&mut ctx.metrics, &metrics);
                    merge(&mut ctx.metrics, &metrics);
                    return Ok(());
                }
                if ctx.options.relational_optimizations
                    && ctx.options.projection_pushdown
                    && let Some(columns) =
                        column_projection::columns(plan, &child.schema, &self.schema)
                {
                    let budget = ctx.budget.clone();
                    let mut metrics = Metrics::default();
                    child.run(ctx, &mut |batch| {
                        let start = Instant::now();
                        let (output, _lease) = column_projection::project(
                            &batch,
                            &columns,
                            self.schema.clone(),
                            &budget,
                        )?;
                        metrics.column_projection_batches += 1;
                        metrics.column_projection_rows += batch.num_rows();
                        metrics.rows_after_filter += batch.num_rows();
                        metrics.batches += 1;
                        metrics.expression_seconds += start.elapsed().as_secs_f64();
                        emit(output)
                    })?;
                    merge(&mut ctx.metrics, &metrics);
                    return Ok(());
                }
                let prepared = Prepared {
                    internal_output: true,
                    plan: plan.clone(),
                    schema: child.schema.clone(),
                    relational: None,
                };
                if plan.aggregation
                    && (!plan.groups.is_empty() || plan.aggregates.iter().any(|a| a.distinct))
                    && let Some(store) = ctx.spill.clone()
                {
                    let options = ctx.options;
                    let budget = ctx.budget.clone();
                    let mut sorter = if plan.order.is_empty() {
                        None
                    } else {
                        let settings = options.spill.as_ref().unwrap();
                        Some(crate::external_sort::ExternalSort::new(
                            self.schema.clone(),
                            plan.order.clone(),
                            store.clone(),
                            budget.clone(),
                            options.cancellation.clone(),
                            settings.sort_run_bytes,
                            settings.merge_fan_in,
                            options.batch_size,
                        )?)
                    };
                    let metrics = crate::external_aggregate::run(
                        &prepared,
                        options,
                        budget,
                        store,
                        |send| child.run(ctx, send),
                        &mut |batch| {
                            if let Some(sorter) = &mut sorter {
                                sorter.push(batch)
                            } else {
                                emit(batch)
                            }
                        },
                    )?;
                    merge(&mut ctx.metrics, &metrics);
                    if let Some(sorter) = sorter {
                        let sorted = sorter.finish(emit)?;
                        ctx.metrics.external_sort_runs += sorted.runs;
                        ctx.metrics.external_sort_merge_passes += sorted.merge_passes;
                        ctx.metrics.external_sort_rows += sorted.rows;
                        ctx.metrics.output_sort_seconds += sorted.seconds;
                    }
                    return Ok(());
                }
                if !plan.order.is_empty()
                    && let Some(store) = ctx.spill.clone()
                {
                    let settings = ctx.options.spill.as_ref().unwrap();
                    let mut sorter = crate::external_sort::ExternalSort::new(
                        self.schema.clone(),
                        plan.order.clone(),
                        store,
                        ctx.budget.clone(),
                        ctx.options.cancellation.clone(),
                        settings.sort_run_bytes,
                        settings.merge_fan_in,
                        ctx.options.batch_size,
                    )?;
                    let mut projected = prepared;
                    projected.plan.order.clear();
                    let options = ctx.options;
                    let budget = ctx.budget.clone();
                    let mut metrics = Metrics::default();
                    if plan.aggregation {
                        // Group state is still in memory; the external aggregate
                        // path is a separate M3 requirement. Sorting is independent.
                        let result =
                            execute::execute_stream(&projected, options, budget, |send| {
                                child.run(ctx, send)
                            })?;
                        merge(&mut metrics, &result.metrics);
                        sorter.push(result.batch.clone())?;
                    } else {
                        child.run(ctx, &mut |batch| {
                            let result = execute::execute_stream(
                                &projected,
                                options,
                                budget.clone(),
                                |send| send(batch),
                            )?;
                            merge(&mut metrics, &result.metrics);
                            sorter.push(result.batch.clone())
                        })?;
                    }
                    let sorted = sorter.finish(emit)?;
                    merge(&mut ctx.metrics, &metrics);
                    ctx.metrics.external_sort_runs += sorted.runs;
                    ctx.metrics.external_sort_merge_passes += sorted.merge_passes;
                    ctx.metrics.external_sort_rows += sorted.rows;
                    ctx.metrics.output_sort_seconds += sorted.seconds;
                    return Ok(());
                }
                if plan.aggregation && plan.order.is_empty() {
                    let metrics = execute::execute_aggregate_stream(
                        &prepared,
                        ctx.options,
                        ctx.budget.clone(),
                        |send| child.run(ctx, send),
                        emit,
                    )?;
                    merge(&mut ctx.metrics, &metrics);
                    return Ok(());
                }
                if !plan.aggregation && plan.order.is_empty() {
                    let budget = ctx.budget.clone();
                    let options = ctx.options;
                    let mut metrics = Metrics::default();
                    child.run(ctx, &mut |b| {
                        let result =
                            execute::execute_stream(&prepared, options, budget.clone(), |send| {
                                send(b)
                            })?;
                        merge(&mut metrics, &result.metrics);
                        emit(result.batch.clone())
                    })?;
                    merge(&mut ctx.metrics, &metrics);
                    return Ok(());
                }
                let result =
                    execute::execute_stream(&prepared, ctx.options, ctx.budget.clone(), |send| {
                        child.run(ctx, send)
                    })?;
                merge(&mut ctx.metrics, &result.metrics);
                emit(result.batch.clone())
            }
            RelOp::Join {
                left,
                right,
                kind,
                keys,
                residual,
            } => self.hash_join(ctx, emit, left, right, *kind, keys, residual.as_ref()),
            RelOp::TypedMark {
                left,
                right,
                value,
                negated,
            } => in_subquery::run(self, left, right, value, *negated, ctx, emit),
        }
    }
    // For a small existence/anti left side, hash its rows and mark matches while
    // streaming the larger right side. Duplicate left rows retain independent
    // match bits; NULL equality keys never match; ON residuals use original
    // left-then-right column order. Only the left rows are emitted at the end.
    #[allow(clippy::too_many_arguments)]
    fn reverse_existence_join(
        &self,
        ctx: &mut Context<'_>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
        left: &Node,
        right: &Node,
        kind: JoinKind,
        keys: &[(Expr, Expr)],
        residual: Option<&Expr>,
    ) -> Result<()> {
        let budget = ctx.budget.clone();
        let compact_text = ctx.options.relational_optimizations;
        let mut batches = Vec::new();
        let mut leases = Vec::new();
        left.run(ctx, &mut |b| {
            leases.push(budget.reserve(b.get_array_memory_size())?);
            batches.push(b);
            Ok(())
        })?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if rows > u32::MAX as usize {
            return Err("resource-limit: join build indices".into());
        }
        let funded_build =
            funded_concat::for_join(&left.schema, &batches, &budget, &ctx.options.cancellation)?;
        drop(batches);
        drop(leases);
        let build = funded_build.batch();
        let values = input(build, &budget, keys.iter().map(|(e, _)| e))?;
        let keys_values = keys
            .iter()
            .map(|(e, _)| e.eval(&values, &budget))
            .collect::<Result<Vec<_>>>()?;
        let encoded =
            crate::keys::Keys::with_text_encoding(&keys_values, rows, &budget, compact_text)?;
        drop(keys_values);
        drop(values);
        let mut map = crate::join_index::JoinIndex::new(
            &encoded,
            rows,
            &budget,
            ctx.options.relational_optimizations,
        )?;
        ctx.metrics.dense_joins += usize::from(map.is_dense());
        let mut next = vec![u32::MAX; rows];
        let mut matched = vec![false; rows];
        let start = Instant::now();
        map.populate(&encoded, &mut next);
        let mut seconds = start.elapsed().as_secs_f64();
        ctx.metrics.join_build_rows += rows;
        ctx.metrics.reverse_existence_joins += 1;
        let batch_size = ctx.options.batch_size;
        let combined_schema = combined(&left.schema, &right.schema);
        let residual_program = if ctx.options.relational_optimizations {
            residual
                .map(|e| crate::join_filter::JoinFilter::new(e, &combined_schema))
                .transpose()?
        } else {
            None
        };
        let _candidate_lease = if residual_program.is_some() {
            Some(
                budget.reserve(
                    batch_size
                        .checked_mul(32)
                        .and_then(|n| n.checked_add(1024))
                        .ok_or("join candidate size overflow")?,
                )?,
            )
        } else {
            None
        };
        let mut residual_rows = 0;
        let mut residual_bytes = 0;
        let scan_filter = install_scan_filter(
            ctx,
            right,
            keys.iter().map(|(_, e)| e.clone()).collect(),
            &map,
        )?;
        let start = Instant::now();
        if scan_filter.is_none() && ctx.options.relational_optimizations {
            ctx.metrics.join_rejection_bitmaps +=
                usize::from(map.enable_rejection(right.estimated_rows, &budget));
        }
        seconds += start.elapsed().as_secs_f64();
        let probe_result = right.run(ctx, &mut |probe| {
            let start = Instant::now();
            let values = input(&probe, &budget, keys.iter().map(|(_, e)| e))?;
            let keys_values = keys
                .iter()
                .map(|(_, e)| e.eval(&values, &budget))
                .collect::<Result<Vec<_>>>()?;
            let probe_keys = crate::keys::Keys::with_text_encoding(
                &keys_values,
                probe.num_rows(),
                &budget,
                compact_text,
            )?;
            if residual.is_none() {
                for row in 0..probe.num_rows() {
                    if !probe_keys.valid(row) {
                        continue;
                    }
                    if let Some(mut r) = map.remove(probe_keys.row(row)) {
                        while r != u32::MAX {
                            matched[r as usize] = true;
                            r = next[r as usize];
                        }
                    }
                }
                seconds += start.elapsed().as_secs_f64();
                return Ok(());
            }
            let mut li = Vec::with_capacity(batch_size);
            let mut ri = Vec::with_capacity(batch_size);
            let mut flush = |li: &mut Vec<u32>, ri: &mut Vec<u32>| -> Result<()> {
                if li.is_empty() {
                    return Ok(());
                }
                let lidx = UInt32Array::from(li.clone());
                let ridx = UInt32Array::from(ri.clone());
                if let Some(program) = &residual_program {
                    let (mask, bytes) = program.eval(build, &probe, &lidx, &ridx, &budget)?;
                    residual_rows += li.len();
                    residual_bytes += bytes;
                    for (i, l) in li.iter().enumerate() {
                        if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i) {
                            matched[*l as usize] = true;
                        }
                    }
                    li.clear();
                    ri.clear();
                    return Ok(());
                }
                let arrays = build
                    .columns()
                    .iter()
                    .map(|a| take(a, &lidx, None).map_err(|e| e.to_string()))
                    .chain(
                        probe
                            .columns()
                            .iter()
                            .map(|a| take(a, &ridx, None).map_err(|e| e.to_string())),
                    )
                    .collect::<Result<Vec<_>>>()?;
                let batch = RecordBatch::try_new(combined_schema.clone(), arrays)
                    .map_err(|e| e.to_string())?;
                let _lease = budget.reserve(batch.get_array_memory_size())?;
                let expr = residual.unwrap();
                let mask = expr.eval(&input(&batch, &budget, [expr])?, &budget)?;
                for (l, yes) in li.iter().zip(boolean(&mask.array).iter()) {
                    if yes == Some(true) {
                        matched[*l as usize] = true;
                    }
                }
                li.clear();
                ri.clear();
                Ok(())
            };
            for row in 0..probe.num_rows() {
                if !probe_keys.valid(row) {
                    continue;
                }
                if let Some(first) = map.get(probe_keys.row(row)) {
                    let mut r = *first;
                    while r != u32::MAX {
                        li.push(r);
                        ri.push(row as u32);
                        if li.len() == batch_size {
                            flush(&mut li, &mut ri)?;
                        }
                        r = next[r as usize];
                    }
                }
            }
            flush(&mut li, &mut ri)?;
            seconds += start.elapsed().as_secs_f64();
            Ok(())
        });
        if let Some(id) = scan_filter {
            ctx.scan_filters.remove(&id);
        }
        probe_result?;
        let start = Instant::now();
        // Emit bounded batches rather than one allocation proportional to the
        // whole retained side. Input order and duplicate multiplicity survive.
        let mut keep = Vec::with_capacity(batch_size);
        let mut count = 0;
        let mut flush = |keep: &mut Vec<u32>| -> Result<()> {
            if keep.is_empty() {
                return Ok(());
            }
            let indices =
                UInt32Array::from(std::mem::replace(keep, Vec::with_capacity(batch_size)));
            let arrays = build
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>>>()?;
            let batch =
                RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| e.to_string())?;
            let _lease = budget.reserve(batch.get_array_memory_size())?;
            count += batch.num_rows();
            emit(batch)
        };
        for (row, yes) in matched.into_iter().enumerate() {
            if yes == (kind == JoinKind::Semi) {
                keep.push(row as u32);
                if keep.len() == batch_size {
                    flush(&mut keep)?;
                }
            }
        }
        flush(&mut keep)?;
        seconds += start.elapsed().as_secs_f64();
        let (bytes, probes, misses) = map.rejection_stats();
        ctx.metrics.join_rejection_bitmap_bytes += bytes;
        ctx.metrics.join_rejection_probes += probes;
        ctx.metrics.join_rejection_misses += misses;
        ctx.metrics.join_seconds += seconds;
        ctx.metrics.residual_candidate_rows += residual_rows;
        ctx.metrics.residual_gathered_bytes += residual_bytes;
        ctx.metrics.join_output_rows += count;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn spilled_join(
        &self,
        ctx: &mut Context<'_>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
        left: &Node,
        right: &Node,
        mode: crate::external_join::Mode,
        keys: &[(Expr, Expr)],
        residual: Option<&Expr>,
    ) -> Result<()> {
        let budget = ctx.budget.clone();
        let store = ctx.spill.as_ref().unwrap().clone();
        let options = ctx.options;
        let right_payload = if matches!(mode, crate::external_join::Mode::Mark { .. })
            || (matches!(
                mode,
                crate::external_join::Mode::Rows(JoinKind::Semi | JoinKind::Anti)
            ) && residual.is_none())
        {
            Arc::new(Schema::empty())
        } else {
            right.schema.clone()
        };
        let mut r = crate::external_join::Side::new(
            right_payload,
            keys.iter().map(|(_, e)| e.clone()).collect(),
            &store,
            &budget,
            options,
        )?;
        right
            .run(ctx, &mut |b| r.push(b))
            .map_err(|e| format!("{e}; external join build partitioning"))?;
        let r = r.finish()?;
        let mut l = crate::external_join::Side::new(
            left.schema.clone(),
            keys.iter().map(|(e, _)| e.clone()).collect(),
            &store,
            &budget,
            options,
        )?;
        left.run(ctx, &mut |b| l.push(b))
            .map_err(|e| format!("{e}; external join probe partitioning"))?;
        let l = l.finish()?;
        let metrics = crate::external_join::run(
            l,
            r,
            self.schema.clone(),
            mode,
            residual,
            options,
            budget,
            store,
            emit,
        )?;
        merge(&mut ctx.metrics, &metrics);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn hash_join(
        &self,
        ctx: &mut Context<'_>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
        left: &Node,
        right: &Node,
        kind: JoinKind,
        keys: &[(Expr, Expr)],
        residual: Option<&Expr>,
    ) -> Result<()> {
        if ctx.spill.is_some() {
            return self.spilled_join(
                ctx,
                emit,
                left,
                right,
                crate::external_join::Mode::Rows(kind),
                keys,
                residual,
            );
        }
        if ctx.options.relational_optimizations
            && matches!(kind, JoinKind::Semi | JoinKind::Anti)
            && left.estimated_rows.saturating_mul(4) < right.estimated_rows
        {
            return self.reverse_existence_join(ctx, emit, left, right, kind, keys, residual);
        }
        let budget = ctx.budget.clone();
        let compact_text = ctx.options.relational_optimizations;
        let mut cached_probe = None;
        let mut aggregate_filter = None;
        if ctx.options.relational_optimizations
            && matches!(kind, JoinKind::Inner | JoinKind::Left)
            && !keys.is_empty()
            && left.estimated_rows <= right.estimated_rows
            && let Some(ScanTarget {
                id, expressions, ..
            }) = aggregate_scan_target(right, keys.iter().map(|(_, e)| e.clone()).collect())?
            && !ctx.scan_filters.contains_key(&id)
        {
            let mut probe = CachedProbe {
                batches: Vec::new(),
                _leases: Vec::new(),
            };
            let mut members = HashSet::<Box<[u8]>, ahash::RandomState>::default();
            let mut member_leases = Vec::new();
            let mut too_many = false;
            left.run(ctx, &mut |batch| {
                probe
                    ._leases
                    .push(budget.reserve(batch.get_array_memory_size() + 256)?);
                if !too_many {
                    let values = input(&batch, &budget, keys.iter().map(|(e, _)| e))?;
                    let values = keys
                        .iter()
                        .map(|(e, _)| e.eval(&values, &budget))
                        .collect::<Result<Vec<_>>>()?;
                    let encoded = crate::keys::Keys::new(&values, batch.num_rows(), &budget)?;
                    for row in 0..batch.num_rows() {
                        if encoded.valid(row) && !members.contains(encoded.row(row)) {
                            if members.len() == 65536 {
                                too_many = true;
                                break;
                            }
                            member_leases.push(budget.reserve(encoded.row(row).len() * 2 + 128)?);
                            members.insert(encoded.row(row).into());
                        }
                    }
                }
                probe.batches.push(batch);
                Ok(())
            })?;
            if !too_many && members.len().saturating_mul(16) < distinct(right, &keys[0].1) {
                // Retain one reservation for the immutable key set. The scan
                // pass is justified here by avoided grouping, even for narrow
                // numeric payloads that do not justify ordinary join filtering.
                let bytes = members.iter().map(|k| k.len() * 2 + 128).sum::<usize>();
                drop(member_leases);
                ctx.scan_filters.insert(
                    id,
                    ScanKeyFilter {
                        numeric: numeric_membership(&expressions, &members, &budget),
                        numeric_pair: numeric_pair_membership(&expressions, &members, &budget),
                        expressions,
                        members: Arc::new(members),
                        _lease: Arc::new(budget.reserve(bytes)?),
                    },
                );
                aggregate_filter = Some(id);
                ctx.metrics.demand_filtered_aggregates += 1;
            }
            cached_probe = Some(probe);
        }
        let mut batches = Vec::new();
        let mut leases = Vec::new();
        let build_result = right.run(ctx, &mut |b| {
            leases.push(budget.reserve(b.get_array_memory_size())?);
            batches.push(b);
            Ok(())
        });
        if let Some(id) = aggregate_filter {
            ctx.scan_filters.remove(&id);
        }
        build_result?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if rows > u32::MAX as usize {
            return Err("resource-limit: join build indices".into());
        }
        let funded_build =
            funded_concat::for_join(&right.schema, &batches, &budget, &ctx.options.cancellation)?;
        drop(batches);
        drop(leases);
        let build = funded_build.batch();
        let build_input = input(build, &budget, keys.iter().map(|(_, e)| e))?;
        let values = keys
            .iter()
            .map(|(_, e)| e.eval(&build_input, &budget))
            .collect::<Result<Vec<_>>>()?;
        let encoded = crate::keys::Keys::with_text_encoding(&values, rows, &budget, compact_text)?;
        drop(values);
        drop(build_input);
        // Borrow immutable encoded keys. One contiguous duplicate chain replaces
        // a heap-allocated vector per key; reserve the complete table up front.
        let mut map = crate::join_index::JoinIndex::new(
            &encoded,
            rows,
            &budget,
            ctx.options.relational_optimizations,
        )?;
        ctx.metrics.dense_joins += usize::from(map.is_dense());
        let mut next = vec![u32::MAX; rows];
        let start = Instant::now();
        map.populate(&encoded, &mut next);
        ctx.metrics.join_build_rows += rows;
        let mut seconds = start.elapsed().as_secs_f64();
        let batch_size = ctx.options.batch_size;
        let combined_schema = combined(&left.schema, &right.schema);
        let residual_program = if ctx.options.relational_optimizations {
            residual
                .map(|e| crate::join_filter::JoinFilter::new(e, &combined_schema))
                .transpose()?
        } else {
            None
        };
        let _candidate_lease = if residual_program.is_some() {
            Some(
                budget.reserve(
                    batch_size
                        .checked_mul(32)
                        .and_then(|n| n.checked_add(1024))
                        .ok_or("join candidate size overflow")?,
                )?,
            )
        } else {
            None
        };
        let scan_filter =
            if cached_probe.is_none() && matches!(kind, JoinKind::Inner | JoinKind::Semi) {
                install_scan_filter(
                    ctx,
                    left,
                    keys.iter().map(|(e, _)| e.clone()).collect(),
                    &map,
                )?
            } else {
                None
            };
        let start = Instant::now();
        if scan_filter.is_none() && ctx.options.relational_optimizations {
            ctx.metrics.join_rejection_bitmaps +=
                usize::from(map.enable_rejection(left.estimated_rows, &budget));
        }
        seconds += start.elapsed().as_secs_f64();
        let mut kernel = probe::Kernel {
            map: map.view(),
            build,
            next: &next,
            keys,
            kind,
            residual,
            residual_program: residual_program.as_ref(),
            combined_schema,
            output_schema: self.schema.clone(),
            right_schema: right.schema.clone(),
            reserve_gathers: false,
            budget: &budget,
            compact_text,
            batch_size,
        };
        let cancellation = ctx.options.cancellation.clone();
        let parallel = if ctx.options.relational_optimizations
            && matches!(kind, JoinKind::Inner | JoinKind::Left)
            && residual.is_none()
            && cached_probe.is_none()
            && probe_chain(left)
        {
            probe::parallel::Configuration::new(ctx.options, &budget, left.estimated_rows)
        } else {
            None
        };
        kernel.reserve_gathers = parallel.is_some();
        let mut probe_stats = probe::Statistics::default();
        let probe_result = if let Some(config) = parallel {
            let result = probe::parallel::run(
                config,
                &budget,
                &cancellation,
                |batch, output, check| {
                    let workspace = batch_size
                        .checked_mul(32)
                        .and_then(|n| n.checked_add(batch.num_rows().saturating_mul(16)))
                        .and_then(|n| n.checked_add(batch.num_columns().saturating_mul(128)))
                        .and_then(|n| n.checked_add(1024))
                        .ok_or("join candidate size overflow")?;
                    let _workspace = budget.reserve(workspace)?;
                    kernel.run(batch, output, check)
                },
                |send| left.run(ctx, send),
                emit,
            );
            result.map(|stats| {
                ctx.metrics.parallel_joins += 1;
                ctx.metrics.parallel_join_tasks += stats.tasks;
                ctx.metrics.parallel_join_batches += stats.batches;
                ctx.metrics.parallel_join_pending_batches_peak = ctx
                    .metrics
                    .parallel_join_pending_batches_peak
                    .max(stats.pending_peak);
                ctx.metrics.parallel_join_pending_bytes_peak = ctx
                    .metrics
                    .parallel_join_pending_bytes_peak
                    .max(stats.pending_bytes_peak);
                ctx.metrics.parallel_join_worker_wall_seconds += stats.report.seconds;
                seconds += stats.seconds;
                probe_stats = stats.report;
                // Pipeline wall time already includes child and delivery work.
                probe_stats.seconds = 0.;
            })
        } else {
            let mut consume_probe = |batch| {
                probe_stats.merge(kernel.run(batch, emit, &|| cancellation.check())?);
                Ok(())
            };
            if let Some(probe) = cached_probe {
                let CachedProbe { batches, _leases } = probe;
                let result = batches.into_iter().try_for_each(&mut consume_probe);
                drop(_leases);
                result
            } else {
                left.run(ctx, &mut consume_probe)
            }
        };
        if let Some(id) = scan_filter {
            ctx.scan_filters.remove(&id);
        }
        probe_result?;
        let (bytes, probes, misses) = map.rejection_stats();
        ctx.metrics.join_rejection_bitmap_bytes += bytes;
        ctx.metrics.join_rejection_probes += probes + probe_stats.probes;
        ctx.metrics.join_rejection_misses += misses + probe_stats.rejected;
        ctx.metrics.join_seconds += seconds + probe_stats.seconds;
        ctx.metrics.residual_candidate_rows += probe_stats.residual_rows;
        ctx.metrics.residual_gathered_bytes += probe_stats.residual_bytes;
        ctx.metrics.join_output_rows += probe_stats.output_rows;
        Ok(())
    }
}
pub fn execute(root: &Node, options: &Options) -> Result<Execution> {
    crate::execute::execution_policy(options, |options, budget| {
        execute_in_budget(root, options, budget, false)
    })
}
pub(crate) fn execute_in_budget(
    root: &Node,
    options: &Options,
    budget: Budget,
    internal_output: bool,
) -> Result<Execution> {
    shared_retry::execute(root, options, budget, internal_output)
}
struct BufferedOutput {
    batches: Vec<RecordBatch>,
    reservations: Vec<Reservation>,
}
fn execute_once(
    root: &Node,
    options: &Options,
    budget: Budget,
    shared_attempt: Option<Arc<crate::execute::pipeline::fallback::Attempt>>,
    internal_output: bool,
) -> Result<Execution> {
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    let start = Instant::now();
    #[cfg(test)]
    let candidate = shared_attempt.clone();
    let mut ctx = Context {
        shared_attempt,
        options,
        scan_filters: HashMap::new(),
        static_filters: HashMap::new(),
        scan_transforms: HashMap::new(),
        replay_transforms: HashMap::new(),
        materialized: HashMap::new(),
        spill: options
            .spill
            .as_ref()
            .map(|settings| {
                crate::spill::SpillStore::new(
                    &settings.directory,
                    settings.disk_quota_bytes,
                    budget.clone(),
                    options.cancellation.clone(),
                )
            })
            .transpose()?,
        budget: budget.clone(),
        metrics: Metrics {
            workers: options.workers,
            ..Metrics::default()
        },
    };
    let mut output = BufferedOutput {
        batches: Vec::new(),
        reservations: Vec::new(),
    };
    let outcome = root.run(&mut ctx, &mut |b| {
        output
            .reservations
            .push(budget.reserve(b.get_array_memory_size())?);
        output.batches.push(b);
        #[cfg(test)]
        if options.test_shared_after_emit_failure
            && candidate
                .as_ref()
                .is_some_and(|attempt| attempt.admitted.load(std::sync::atomic::Ordering::Acquire))
        {
            budget.reserve(budget.limit())?;
        }
        Ok(())
    });
    if let Err(error) = outcome {
        ctx.materialized.clear();
        if let Some(store) = ctx.spill.take()
            && let Err(cleanup) = store.finish()
        {
            return Err(format!("{error}; {cleanup}"));
        }
        return Err(error);
    }
    ctx.materialized.clear();
    if let Some(store) = ctx.spill.take() {
        ctx.metrics.spill = store.finish()?;
    }
    options.cancellation.check()?;
    let lease = budget.reserve(
        output
            .batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum(),
    )?;
    let batch = concat_batches(&root.schema, &output.batches).map_err(|e| e.to_string())?;
    // This is the public query boundary. A derived relation, aggregate result,
    // grouping key or sort key is not a returned BIGINT value.
    let batch = if !internal_output
        && root
            .schema
            .fields()
            .iter()
            .any(|f| Type::is_wide_integer(f))
    {
        let bytes = root
            .schema
            .fields()
            .iter()
            .filter(|f| Type::is_wide_integer(f))
            .count()
            .checked_mul(batch.num_rows())
            .and_then(|n| n.checked_mul(8))
            .and_then(|n| n.checked_add(1024))
            .ok_or("output size overflow")?;
        output.reservations.push(budget.reserve(bytes)?);
        let fields = root
            .schema
            .fields()
            .iter()
            .map(|f| {
                if Type::is_wide_integer(f) {
                    Type::Int.field(f.name(), false)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect::<Vec<_>>();
        let arrays = batch
            .columns()
            .iter()
            .zip(root.schema.fields())
            .map(|(a, f)| {
                if Type::is_wide_integer(f) {
                    execute::materialize(a, &Type::Int)
                } else {
                    Ok(a.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| e.to_string())?
    } else {
        batch
    };
    options.cancellation.check()?;
    ctx.metrics.execution_seconds = start.elapsed().as_secs_f64();
    ctx.metrics.peak_reserved_bytes = budget.peak();
    ctx.metrics.worker_threads_peak = budget.peak_workers();
    Ok(Execution {
        batch,
        metrics: ctx.metrics,
        _reservations: vec![lease],
    })
}

fn common_terms(e: &s::Expr) -> Vec<s::Expr> {
    match e {
        s::Expr::Nested(e) => common_terms(e),
        s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::Or,
            right,
        } => {
            let a = common_terms(left);
            let b = common_terms(right);
            a.into_iter().filter(|e| b.contains(e)).collect()
        }
        s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::And,
            right,
        } => {
            let mut a = common_terms(left);
            a.extend(common_terms(right));
            a
        }
        _ => vec![e.clone()],
    }
}
fn local_condition(e: &s::Expr, schema: &Schema) -> Option<s::Expr> {
    if binder(schema).expr(e).is_ok() {
        return Some(e.clone());
    }
    match e {
        s::Expr::Nested(e) => local_condition(e, schema),
        s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::And,
            right,
        } => syntax::and(
            [
                local_condition(left, schema),
                local_condition(right, schema),
            ]
            .into_iter()
            .flatten()
            .collect(),
        ),
        s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::Or,
            right,
        } => Some(s::Expr::BinaryOp {
            left: Box::new(local_condition(left, schema)?),
            op: s::BinaryOperator::Or,
            right: Box::new(local_condition(right, schema)?),
        }),
        _ => None,
    }
}

fn distinct(node: &Node, e: &Expr) -> usize {
    let crate::expr::Kind::Column(i) = e.kind else {
        return node.estimated_rows.max(1);
    };
    let column = |i| Expr {
        kind: crate::expr::Kind::Column(i),
        ty: e.ty.clone(),
    };
    let n = match &node.op {
        RelOp::Scan { distinct, .. } => distinct[i],
        RelOp::Project { input, columns } => distinct(input, &column(columns[i])),
        RelOp::Rename { input }
        | RelOp::Filter { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => distinct(input, &column(i)),
        RelOp::Join { left, right, .. } => {
            if i < left.schema.fields().len() {
                distinct(left, &column(i))
            } else {
                distinct(right, &column(i - left.schema.fields().len()))
            }
        }
        RelOp::Mark { left, .. } | RelOp::TypedMark { left, .. } => {
            if i < left.schema.fields().len() {
                distinct(left, &column(i))
            } else {
                2
            }
        }
        RelOp::Unary { plan, input } => match &plan.output[i] {
            bind::Output::Expr(e) => distinct(input, e),
            bind::Output::Group(i) => distinct(input, &plan.groups[*i]),
            _ => node.estimated_rows,
        },
        _ => node.estimated_rows,
    };
    n.min(node.estimated_rows).max(1)
}
fn estimate_keys(left: &Node, right: &Node, keys: &[(Expr, Expr)]) -> usize {
    let mut rows = left.estimated_rows as f64 * right.estimated_rows as f64;
    // Composite columns can be dependent. A maximum NDV is conservative;
    // multiplying independent selectivities grossly underestimates many joins.
    if let Some(ndv) = keys
        .iter()
        .map(|(a, b)| distinct(left, a).max(distinct(right, b)))
        .max()
    {
        rows /= ndv as f64;
    }
    (rows.ceil() as usize).max(1)
}
fn join_estimate(left: &Node, right: &Node, predicates: &[s::Expr]) -> (bool, usize) {
    let keys = predicates
        .iter()
        .filter_map(|p| join_key(p, &left.schema, &right.schema))
        .collect::<Vec<_>>();
    (keys.is_empty(), estimate_keys(left, right, &keys))
}

fn equality_closure(predicates: &[s::Expr]) -> Vec<s::Expr> {
    let mut sets: Vec<Vec<s::Expr>> = Vec::new();
    for p in predicates {
        if let s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::Eq,
            right,
        } = p
            && matches!(
                left.as_ref(),
                s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
            )
            && matches!(
                right.as_ref(),
                s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
            )
        {
            let mut group = vec![*left.clone(), *right.clone()];
            let mut i = 0;
            while i < sets.len() {
                if sets[i].iter().any(|e| group.contains(e)) {
                    for e in sets.remove(i) {
                        if !group.contains(&e) {
                            group.push(e);
                        }
                    }
                    i = 0;
                } else {
                    i += 1;
                }
            }
            sets.push(group);
        }
    }
    let mut result = Vec::new();
    for group in sets {
        for i in 0..group.len() {
            for j in i + 1..group.len() {
                let e = s::Expr::BinaryOp {
                    left: Box::new(group[i].clone()),
                    op: s::BinaryOperator::Eq,
                    right: Box::new(group[j].clone()),
                };
                if !predicates.contains(&e) {
                    result.push(e);
                }
            }
        }
    }
    result
}

fn serialize_schema<S: serde::Serializer>(
    schema: &SchemaRef,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::Serialize;
    schema
        .fields()
        .iter()
        .map(|f| (f.name(), f.data_type().to_string(), f.is_nullable()))
        .collect::<Vec<_>>()
        .serialize(serializer)
}

fn materialized_storage(
    id: usize,
    input: &Node,
    shared_scan: bool,
    ctx: &mut Context<'_>,
) -> Result<Arc<Materialized>> {
    let stored = if let Some(stored) = ctx.materialized.get(&id).cloned() {
        if shared_scan {
            ctx.metrics.scan_reuses += 1;
        } else {
            ctx.metrics.cte_reuses += 1;
        }
        stored
    } else {
        let budget = ctx.budget.clone();
        let mut retained = BufferedOutput {
            batches: Vec::new(),
            reservations: Vec::new(),
        };
        let mut files = Vec::new();
        if let Some(store) = ctx.spill.clone() {
            let mut writer =
                crate::spill_partition::PartitionWriter::new(&store, &budget, ctx.options, 0, 0)?;
            input.run(ctx, &mut |batch| writer.push(&batch))?;
            for partition in writer.finish()? {
                files.extend(partition.files);
            }
            ctx.metrics.spilled_materializations += 1;
        } else {
            input.run(ctx, &mut |batch| {
                retained
                    .reservations
                    .push(budget.reserve(batch.get_array_memory_size() + 256)?);
                retained.batches.push(batch);
                Ok(())
            })?;
        }
        let stored = Arc::new(Materialized {
            budget: ctx.budget.clone(),
            batches: retained.batches,
            files,
            _leases: retained.reservations,
        });
        ctx.materialized.insert(id, stored.clone());
        if shared_scan {
            ctx.metrics.scan_materializations += 1;
        } else {
            ctx.metrics.cte_materializations += 1;
        }
        stored
    };
    Ok(stored)
}
