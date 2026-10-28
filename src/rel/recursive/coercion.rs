//! Recursive implicit anchor conversions, isolated from public explicit casts.
//! Observed DuckDB 2.0: anchor type remains fixed; decimal/text -> integer is
//! checked and rounds. Additional anchor domains require oracle qualification.
use super::super::*;
use crate::expr::{Kind, Literal, cast_dialect};

pub(super) fn step(input: Node, target: &SchemaRef) -> Result<Node> {
    let mut plan = identity(&input.schema)?;
    let names = target
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let types = target
        .fields()
        .iter()
        .map(|f| Type::from_field(f))
        .collect::<Result<Vec<_>>>()?;
    let mut changed = false;
    for (i, ty) in types.iter().enumerate() {
        let source = Type::from_field(input.schema.field(i))?;
        if &source == ty {
            continue;
        }
        let mut value = Expr {
            ty: source.clone(),
            kind: Kind::Column(i),
        };
        if source == Type::Null {
            value = Expr {
                ty: ty.clone(),
                kind: Kind::Literal(Literal::Null),
            };
        } else if *ty == Type::Int && matches!(source, Type::Decimal(_) | Type::Text) {
            if source == Type::Text {
                // Checked decimal parser handles complete input and performs
                // exact scale-zero rounding; never public INTEGER text casting.
                value = cast_dialect::bind(
                    value,
                    &s::DataType::Decimal(s::ExactNumberInfo::PrecisionAndScale(38, 0)),
                )?;
            }
            value = cast_dialect::bind(value, &s::DataType::BigInt(None))?;
        } else {
            return Err(format!("unsupported: recursive implicit {source:?} to {ty:?}").into());
        }
        plan.output[i] = bind::Output::Expr(value);
        changed = true;
    }
    if !changed {
        return Ok(input);
    }
    plan.names = names.clone();
    plan.types = types.clone();
    Ok(Node {
        schema: schema(&names, &types),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            plan,
            input: Box::new(input),
        },
    })
}
// Assay INTEGER is its existing 64-bit/wide-integer public domain, unlike
// DuckDB's 32-bit INTEGER. TINYINT anchor storage/domain is not supplied here.
// Target NULL from non-NULL source stays explicitly unsupported (DuckDB errors).
