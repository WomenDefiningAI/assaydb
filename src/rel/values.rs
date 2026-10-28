//! Typed VALUES relations use existing projection and union execution paths.
//! A balanced union keeps wide VALUES lists from creating a linear plan stack.
use super::*;
use crate::expr::{Kind, Literal, cast_dialect, casts};

fn common(left: &Type, right: &Type, text_literal: bool) -> Result<Type> {
    if text_literal && *right == Type::Text && *left != Type::Null && *left != Type::Text {
        // VALUES inference is left-to-right: a later text value is converted
        // to the established type, while a leading VARCHAR fixes that type.
        return Ok(left.clone());
    }
    if (*left == Type::Bool && *right == Type::Int) || (*right == Type::Bool && *left == Type::Int)
    {
        return bind::common_type(
            if *left == Type::Bool {
                &Type::Int
            } else {
                left
            },
            if *right == Type::Bool {
                &Type::Int
            } else {
                right
            },
        );
    }
    if matches!(left, Type::Date | Type::Timestamp | Type::TimestampTz)
        && matches!(right, Type::Date | Type::Timestamp | Type::TimestampTz)
    {
        return Ok(
            if *left == Type::TimestampTz || *right == Type::TimestampTz {
                Type::TimestampTz
            } else if *left == Type::Timestamp || *right == Type::Timestamp {
                Type::Timestamp
            } else {
                Type::Date
            },
        );
    }
    bind::common_type(left, right)
}

fn convert(mut value: Expr, target: &Type) -> Result<Expr> {
    if &value.ty == target {
        return Ok(value);
    }
    if matches!(value.kind, Kind::Literal(Literal::Null)) {
        return Ok(Expr {
            ty: target.clone(),
            kind: Kind::Literal(Literal::Null),
        });
    }
    if value.ty == Type::Text && *target == Type::Int {
        // VALUES has its own DuckDB implicit-literal coercion: decimal text
        // rounds half away from zero. Reuse the exact decimal parser at scale
        // zero, then the checked integer range conversion; never go through
        // binary64 or change the explicit public INTEGER/BIGINT contracts.
        value = cast_dialect::bind(
            value,
            &s::DataType::Decimal(s::ExactNumberInfo::PrecisionAndScale(38, 0)),
        )?;
    }
    if value.ty == Type::Bool && matches!(target, Type::Int | Type::Decimal(_) | Type::Float) {
        value = casts::bind(value, Type::Int)?;
    }
    if let Type::Decimal(scale) = target
        && (value.ty.scale().is_ok() || value.ty == Type::Null)
    {
        return Ok(Expr {
            ty: target.clone(),
            kind: Kind::CastDecimal(Box::new(value), 38, *scale),
        });
    }
    // The checked dialect casts are deliberate here. Public INTEGER/REAL
    // explicit casts retain their permissive compatibility behavior; implicit
    // VALUES conversion must not turn malformed text into a numeric zero.
    let sql_type = match target {
        Type::Int => s::DataType::BigInt(None),
        Type::Float => s::DataType::Double(s::ExactNumberInfo::None),
        Type::Decimal(scale) => {
            s::DataType::Decimal(s::ExactNumberInfo::PrecisionAndScale(38, *scale as u64))
        }
        Type::Bool => s::DataType::Boolean,
        Type::Text => s::DataType::Text,
        Type::Date => s::DataType::Date,
        Type::Timestamp => s::DataType::Timestamp(None, s::TimezoneInfo::WithoutTimeZone),
        Type::TimestampTz => s::DataType::Timestamp(None, s::TimezoneInfo::WithTimeZone),
        _ => {
            return Err(format!(
                "unsupported: VALUES conversion {:?} to {target:?}",
                value.ty
            )
            .into());
        }
    };
    cast_dialect::bind(value, &sql_type)
}

fn balanced(mut nodes: Vec<Node>, output: &SchemaRef) -> Node {
    while nodes.len() > 1 {
        let mut next = Vec::with_capacity(nodes.len().div_ceil(2));
        let mut iter = nodes.into_iter();
        while let Some(left) = iter.next() {
            next.push(if let Some(right) = iter.next() {
                Node {
                    schema: output.clone(),
                    estimated_rows: left.estimated_rows.saturating_add(right.estimated_rows),
                    op: RelOp::UnionAll {
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                }
            } else {
                left
            });
        }
        nodes = next;
    }
    nodes.pop().expect("VALUES has at least one row")
}

impl Planner<'_> {
    pub(super) fn values(
        &mut self,
        values: s::Values,
        ctes: &HashMap<String, Node>,
    ) -> Result<Node> {
        let width = values.rows.first().map_or(0, Vec::len);
        if width == 0 || values.rows.iter().any(|row| row.len() != width) {
            return Err("VALUES rows must have the same nonzero column count".into());
        }
        let mut types = vec![Type::Null; width];
        let mut bound = Vec::with_capacity(values.rows.len());
        for row in values.rows {
            self.options.cancellation.check()?;
            let mut input = Node {
                schema: Arc::new(Schema::empty()),
                estimated_rows: 1,
                op: RelOp::One,
            };
            let mut expressions = Vec::with_capacity(width);
            for (column, mut expression) in row.into_iter().enumerate() {
                let mut literal = &expression;
                while let s::Expr::Nested(inner) = literal {
                    literal = inner;
                }
                let text_literal = matches!(literal, s::Expr::Value(value) if matches!(value.value, s::Value::SingleQuotedString(_)));
                input = self.subqueries(&mut expression, input, ctes)?;
                let value = bind_node_expression(&input, &expression)?;
                types[column] = common(&types[column], &value.ty, text_literal)?;
                expressions.push(value);
            }
            bound.push((input, expressions));
        }
        let names = (0..width).map(|i| format!("col{i}")).collect::<Vec<_>>();
        let output = schema(&names, &types);
        let mut nodes = Vec::with_capacity(bound.len());
        for (input, expressions) in bound {
            let expressions = expressions
                .into_iter()
                .zip(&types)
                .map(|(value, ty)| convert(value, ty).map(bind::Output::Expr))
                .collect::<Result<Vec<_>>>()?;
            let plan = Plan {
                table: None,
                path: None,
                columns: (0..input.schema.fields().len()).collect(),
                filter: None,
                groups: Vec::new(),
                aggregates: Vec::new(),
                output: expressions,
                names: names.clone(),
                types: types.clone(),
                order: Vec::new(),
                aggregation: false,
                projection_pushdown: true,
            };
            nodes.push(Node {
                schema: output.clone(),
                estimated_rows: 1,
                op: RelOp::Unary {
                    plan,
                    input: Box::new(input),
                },
            });
        }
        Ok(balanced(nodes, &output))
    }
}
