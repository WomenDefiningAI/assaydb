//! RIGHT and FULL outer joins, and USING/NATURAL column merging.
//!
//! No new physical join operator: every form is composed from the existing,
//! already-verified LEFT and ANTI hash joins, so build/probe memory, spill,
//! parallel probing and determinism are exactly those of LEFT/ANTI.
//!
//! * `A RIGHT JOIN B ON c` is `B LEFT JOIN A ON c` with its columns reordered.
//! * `A FULL JOIN B ON c` is `A LEFT JOIN B ON c` followed by the rows of `B`
//!   for which no row of `A` satisfies `c` (`B ANTI JOIN A ON c`), padded with
//!   NULLs for `A`. The unmatched-build-side state of a native FULL join is the
//!   ANTI join's own funded match tracking; nothing new is allocated here.
//!
//! USING/NATURAL output one merged column per USING column and keep the
//! original columns reachable by their qualified names only. The merged column
//! is named `.k` (an empty qualifier): an unqualified `k` finds it, no `t.k`
//! can, and the name is the same every time the same SQL is planned. For FULL the merged value is
//! COALESCE(left, right): the matched branch carries the left key (equal to the
//! right key whenever both are present) and the unmatched branch the right key.
use super::*;

/// Field metadata: a USING/NATURAL input column that only a qualified
/// reference (`t.k`) or a qualified wildcard (`t.*`) may select.
pub(crate) const HIDDEN: &str = "assay.internal.using_hidden";
/// Field metadata: `SELECT *` over this relation would order its columns
/// differently in DuckDB (merged column at the left column's position) and
/// PostgreSQL (merged columns first). Unqualified `*` is refused.
pub(crate) const STAR_CONFLICT: &str = "assay.internal.using_star_conflict";

pub(crate) fn hidden(field: &Field) -> bool {
    field.metadata().contains_key(HIDDEN)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Outer {
    Inner,
    Left,
    Right,
    Full,
}

pub(super) enum Using<'a> {
    Columns(&'a [s::ObjectName]),
    Natural,
}

fn column(node: &Node, index: usize) -> Result<Expr> {
    Ok(Expr {
        ty: Type::from_field(node.schema.field(index))?,
        kind: crate::expr::Kind::Column(index),
    })
}

fn last(field: &Field) -> &str {
    field.name().rsplit('.').next().unwrap_or(field.name())
}

fn project(input: Node, columns: Vec<usize>) -> Result<Node> {
    Ok(Node {
        schema: Arc::new(input.schema.project(&columns).map_err(|e| e.to_string())?),
        estimated_rows: input.estimated_rows,
        op: RelOp::Project {
            input: Box::new(input),
            columns,
        },
    })
}

/// Rows of `input` (the right side) extended with typed NULLs for every left
/// column, laid out as `merged ++ left ++ right` (merged: right key columns).
fn null_extended(input: Node, left: &Schema, merged: &[usize]) -> Result<Node> {
    let mut output = Vec::new();
    let mut names = Vec::new();
    let mut types = Vec::new();
    let mut push = |name: &str, expr: Expr| {
        names.push(name.to_string());
        types.push(expr.ty.clone());
        output.push(bind::Output::Expr(expr));
    };
    for &index in merged {
        push(input.schema.field(index).name(), column(&input, index)?);
    }
    for field in left.fields() {
        let ty = Type::from_field(field)?;
        push(
            field.name(),
            Expr {
                ty,
                kind: crate::expr::Kind::Literal(crate::expr::Literal::Null),
            },
        );
    }
    for index in 0..input.schema.fields().len() {
        push(input.schema.field(index).name(), column(&input, index)?);
    }
    let plan = Plan {
        table: None,
        path: None,
        columns: (0..input.schema.fields().len()).collect(),
        filter: None,
        groups: Vec::new(),
        aggregates: Vec::new(),
        output,
        names,
        types,
        order: Vec::new(),
        aggregation: false,
        projection_pushdown: true,
    };
    Ok(Node {
        schema: column_projection::output_schema(&plan, &input.schema),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            plan,
            input: Box::new(input),
        },
    })
}

impl Planner<'_> {
    /// Join `left` and `right` with layout `merged ++ left ++ right`, where
    /// `pairs` are USING (left, right) column pairs that become equality keys
    /// and `merged` columns (empty without USING).
    fn outer(
        &self,
        left: Node,
        right: Node,
        kind: Outer,
        on: Vec<s::Expr>,
        pairs: &[(usize, usize)],
    ) -> Result<Node> {
        let left_width = left.schema.fields().len();
        let right_width = right.schema.fields().len();
        let forward = pairs
            .iter()
            .map(|&(l, r)| Ok((column(&left, l)?, column(&right, r)?)))
            .collect::<Result<Vec<_>>>()?;
        let backward = forward
            .iter()
            .map(|(l, r)| (r.clone(), l.clone()))
            .collect::<Vec<_>>();
        let left_keys = pairs.iter().map(|&(l, _)| l).collect::<Vec<_>>();
        let right_keys = pairs.iter().map(|&(_, r)| r).collect::<Vec<_>>();
        let width = left_width + right_width;
        match kind {
            Outer::Inner | Outer::Left => {
                let joined = self.join_with_keys(
                    left,
                    right,
                    if kind == Outer::Inner {
                        JoinKind::Inner
                    } else {
                        JoinKind::Left
                    },
                    on,
                    forward,
                )?;
                if pairs.is_empty() {
                    return Ok(joined);
                }
                project(joined, left_keys.into_iter().chain(0..width).collect())
            }
            Outer::Right => {
                // The preserved right input streams as the LEFT join's probe.
                let joined = self.join_with_keys(right, left, JoinKind::Left, on, backward)?;
                project(
                    joined,
                    right_keys
                        .into_iter()
                        .chain(right_width..width)
                        .chain(0..right_width)
                        .collect(),
                )
            }
            Outer::Full => {
                let left_schema = left.schema.clone();
                let matched = self.join_with_keys(
                    left.clone(),
                    right.clone(),
                    JoinKind::Left,
                    on.clone(),
                    forward,
                )?;
                let matched = if pairs.is_empty() {
                    matched
                } else {
                    project(matched, left_keys.into_iter().chain(0..width).collect())?
                };
                let unmatched = self.join_with_keys(right, left, JoinKind::Anti, on, backward)?;
                let unmatched = null_extended(unmatched, &left_schema, &right_keys)?;
                Ok(Node {
                    schema: matched.schema.clone(),
                    estimated_rows: matched
                        .estimated_rows
                        .saturating_add(unmatched.estimated_rows),
                    op: RelOp::UnionAll {
                        left: Box::new(matched),
                        right: Box::new(unmatched),
                    },
                })
            }
        }
    }

    pub(super) fn outer_join(
        &mut self,
        left: Node,
        right: Node,
        kind: Outer,
        on: Vec<s::Expr>,
    ) -> Result<Node> {
        self.outer(left, right, kind, on, &[])
    }

    pub(super) fn using_join(
        &mut self,
        left: Node,
        right: Node,
        kind: Outer,
        using: Using<'_>,
    ) -> Result<Node> {
        let visible = |schema: &Schema| {
            (0..schema.fields().len())
                .filter(|i| !hidden(schema.field(*i)))
                .collect::<Vec<_>>()
        };
        let left_visible = visible(&left.schema);
        let right_visible = visible(&right.schema);
        // (name, quoted) of each USING column, in USING-list order.
        let names: Vec<(String, bool)> = match using {
            Using::Columns(columns) => columns
                .iter()
                .map(|name| match name.0.as_slice() {
                    [s::ObjectNamePart::Identifier(id)] => Ok(if id.quote_style.is_some() {
                        (id.value.clone(), true)
                    } else {
                        (id.value.to_lowercase(), false)
                    }),
                    _ => Err(format!("unsupported: qualified USING column {name}").into()),
                })
                .collect::<Result<_>>()?,
            Using::Natural => {
                let mut common = Vec::new();
                for &i in &left_visible {
                    let name = last(left.schema.field(i)).to_lowercase();
                    if !common.iter().any(|(c, _)| c == &name)
                        && right_visible
                            .iter()
                            .any(|&j| last(right.schema.field(j)).to_lowercase() == name)
                    {
                        common.push((name, false));
                    }
                }
                if common.is_empty() {
                    return Err("unsupported: NATURAL JOIN without common columns (DuckDB rejects it; PostgreSQL returns the cross join)".into());
                }
                common
            }
        };
        for (i, name) in names.iter().enumerate() {
            if names[..i].iter().any(|n| n.0.eq_ignore_ascii_case(&name.0)) {
                return Err(format!(
                    "unsupported: column {} appears more than once in USING (PostgreSQL rejects it; DuckDB accepts it)",
                    name.0
                )
                .into());
            }
        }
        let find = |schema: &Schema,
                    candidates: &[usize],
                    (name, quoted): &(String, bool),
                    side| {
            let hits = candidates
                .iter()
                .copied()
                .filter(|&i| {
                    let field = last(schema.field(i));
                    if *quoted {
                        field == name
                    } else {
                        field.to_lowercase() == *name
                    }
                })
                .collect::<Vec<_>>();
            match hits.as_slice() {
                [index] => Ok(*index),
                [] => Err(format!(
                    "semantic: USING column {name} does not exist on the {side} side of the join"
                )),
                _ => Err(format!(
                    "semantic: USING column {name} is ambiguous on the {side} side of the join"
                )),
            }
        };
        let mut pairs = Vec::new();
        for name in &names {
            let l = find(&left.schema, &left_visible, name, "left")?;
            let r = find(&right.schema, &right_visible, name, "right")?;
            let (lt, rt) = (
                Type::from_field(left.schema.field(l))?,
                Type::from_field(right.schema.field(r))?,
            );
            if lt != rt {
                return Err(format!(
                    "unsupported: USING column {} has different types on each side ({lt:?} and {rt:?}); DuckDB keeps the left type, PostgreSQL the common type",
                    name.0
                )
                .into());
            }
            if lt == Type::Null {
                return Err(format!("unsupported: USING column {} of NULL type", name.0).into());
            }
            pairs.push((l, r));
        }
        // DuckDB lists the merged column at its left position, PostgreSQL
        // lists the merged columns first: they agree only when the USING
        // columns are the leading visible left columns, in USING order.
        let conflict = left
            .schema
            .fields()
            .iter()
            .any(|f| f.metadata().contains_key(STAR_CONFLICT))
            || !left_visible
                .iter()
                .take(pairs.len())
                .copied()
                .eq(pairs.iter().map(|(l, _)| *l));
        let left_width = left.schema.fields().len();
        let merged_names = pairs
            .iter()
            .map(|&(l, _)| format!(".{}", last(left.schema.field(l))))
            .collect::<Vec<_>>();
        let node = self.outer(left, right, kind, Vec::new(), &pairs)?;
        let mark = |field: &Field, key: &str| {
            let mut metadata = field.metadata().clone();
            metadata.insert(key.to_string(), "true".to_string());
            field.clone().with_metadata(metadata)
        };
        let m = pairs.len();
        let fields = node
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let field = field.as_ref().clone().with_nullable(true);
                Arc::new(if i < m {
                    let mut metadata = field.metadata().clone();
                    metadata.remove(HIDDEN);
                    let field = field.with_name(&merged_names[i]).with_metadata(metadata);
                    if conflict {
                        mark(&field, STAR_CONFLICT)
                    } else {
                        field
                    }
                } else if (i - m < left_width && pairs.iter().any(|&(l, _)| l == i - m))
                    || (i - m >= left_width && pairs.iter().any(|&(_, r)| r == i - m - left_width))
                {
                    mark(&field, HIDDEN)
                } else {
                    field
                })
            })
            .collect::<Vec<_>>();
        Ok(Node {
            schema: Arc::new(Schema::new(fields)),
            estimated_rows: node.estimated_rows,
            op: RelOp::Rename {
                input: Box::new(node),
            },
        })
    }
}
