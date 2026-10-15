//! Column projections share identical arrays; INTEGER widening is lossless
//! and computed once per unique source column, including duplicate aliases.
use crate::{
    Result,
    bind::{Output, Plan},
    expr::{Kind, normalize_input},
    memory::{Budget, Reservation},
    types::Type,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use std::sync::Arc;

pub(super) fn output_schema(plan: &Plan, source: &Schema) -> SchemaRef {
    let wide = super::schema(&plan.names, &plan.types);
    let Some(selected) = columns(plan, source, &wide) else {
        return wide;
    };
    // Only direct references to Int64 inputs qualify. Int32 conversions,
    // computed INTEGERs and wide INTEGER references keep precision-38 fields.
    Arc::new(Schema::new(
        selected
            .iter()
            .enumerate()
            .map(|(out, column)| {
                if plan.types[out] == Type::Int
                    && source.field(*column).data_type() == &DataType::Int64
                {
                    Type::Int.field(&plan.names[out], false)
                } else {
                    wide.field(out).clone()
                }
            })
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn columns(plan: &Plan, source: &Schema, output: &Schema) -> Option<Vec<usize>> {
    if !plan.projection_pushdown
        || plan.aggregation
        || plan.filter.is_some()
        || !plan.order.is_empty()
        || !plan.groups.is_empty()
        || !plan.aggregates.is_empty()
        || plan.output.is_empty()
        || plan.output.len() != plan.types.len()
        || plan.output.len() != plan.names.len()
        || plan.output.len() != output.fields().len()
    {
        return None;
    }
    let columns: Vec<usize> = plan
        .output
        .iter()
        .enumerate()
        .map(|(i, expression)| {
            let Output::Expr(expression) = expression else {
                return None;
            };
            let Kind::Column(column) = expression.kind else {
                return None;
            };
            let ty = output.field(i).data_type();
            let source_type = source.fields().get(column)?.data_type();
            let lossless_integer_widening = expression.ty == Type::Int
                && matches!(source_type, DataType::Int32 | DataType::Int64)
                && Type::is_wide_integer(output.field(i));
            (expression.ty == plan.types[i]
                && (source_type == ty || lossless_integer_widening)
                && output.field(i).name() == &plan.names[i])
                .then_some(column)
        })
        .collect::<Option<_>>()?;
    // Preparation and execution options can differ. Do not skip validation of
    // extra dependencies retained by a plan prepared without pushdown.
    let mut dependencies = columns.clone();
    dependencies.sort_unstable();
    dependencies.dedup();
    (plan.columns == dependencies).then_some(columns)
}

/// The caller holds the returned lease through downstream consumption. Retained
/// output buffers, including full backing buffers of slices, stay accounted.
pub(super) fn project(
    batch: &RecordBatch,
    columns: &[usize],
    schema: SchemaRef,
    budget: &Budget,
) -> Result<(RecordBatch, Reservation)> {
    let bytes = columns
        .iter()
        .enumerate()
        .try_fold(256_usize, |n, (out, i)| {
            let extra = if batch.column(*i).data_type() != schema.field(out).data_type() {
                batch
                    .num_rows()
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(512))
                    .ok_or("column projection size overflow")?
            } else {
                0
            };
            n.checked_add(batch.column(*i).get_array_memory_size())
                .and_then(|n| n.checked_add(extra))
                .ok_or("column projection size overflow")
        })?;
    let lease = budget.reserve(bytes)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut normalized = Vec::new();
    for (out, column) in columns.iter().enumerate() {
        let source = batch.column(*column);
        if source.data_type() == schema.field(out).data_type() {
            arrays.push(source.clone());
        } else if let Some(previous) =
            columns[..out].iter().enumerate().find_map(|(position, i)| {
                (*i == *column && arrays[position].data_type() == schema.field(out).data_type())
                    .then_some(position)
            })
        {
            arrays.push(arrays[previous].clone());
        } else {
            if !matches!(source.data_type(), DataType::Int32 | DataType::Int64)
                || !Type::is_wide_integer(schema.field(out))
            {
                return Err("internal: invalid column projection conversion".into());
            }
            let value = normalize_input(source, &Type::Int, budget)?;
            arrays.push(value.array.clone());
            normalized.push(value);
        }
    }
    let output = RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| e.to_string())?;
    Ok((output, lease))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bind,
        execute::{self, Options, Prepared},
        expr::Expr,
        types::Type,
    };
    use arrow::{
        array::*,
        datatypes::{DataType, Field},
    };
    use std::sync::Arc;

    fn plan(source: &Schema, selected: &[usize]) -> (Plan, SchemaRef) {
        let types: Vec<_> = selected
            .iter()
            .map(|i| Type::from_field(source.field(*i)).unwrap())
            .collect();
        let names: Vec<_> = (0..selected.len()).map(|i| format!("alias_{i}")).collect();
        let output = selected
            .iter()
            .zip(&types)
            .map(|(i, ty)| {
                Output::Expr(Expr {
                    ty: ty.clone(),
                    kind: Kind::Column(*i),
                })
            })
            .collect();
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .zip(&types)
                .map(|(n, t)| Field::new(n, t.arrow(), true))
                .collect::<Vec<_>>(),
        ));
        (
            Plan {
                table: None,
                path: None,
                columns: selected
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                filter: None,
                groups: vec![],
                aggregates: vec![],
                output,
                names,
                types,
                order: vec![],
                aggregation: false,
                projection_pushdown: true,
            },
            schema,
        )
    }

    #[test]
    fn wide_integer_aliases_convert_once_and_retain_exact_values_and_credit() {
        let input =
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])) as ArrayRef;
        let source = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let batch = RecordBatch::try_new(Arc::new(source.clone()), vec![input]).unwrap();
        let (plan, _) = plan(&source, &[0, 0]);
        let output = Arc::new(Schema::new(vec![
            Type::Int.field("alias_0", true),
            Type::Int.field("alias_1", true),
        ]));
        let selected = columns(&plan, &source, &output).unwrap();
        let budget = Budget::new(1 << 20);
        let (result, lease) = project(&batch, &selected, output.clone(), &budget).unwrap();
        assert!(Arc::ptr_eq(result.column(0), result.column(1)));
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(i64::MIN as i128), None, Some(i64::MAX as i128)]
        );
        assert!(budget.used() >= result.get_array_memory_size());
        drop(result);
        drop(lease);
        assert_eq!(budget.used(), 0);
        let tiny = Budget::new(16);
        assert!(project(&batch, &selected, output, &tiny).is_err());
        assert_eq!(tiny.used(), 0);
    }

    #[test]
    fn aliases_slices_nulls_and_empty_batches_match_materialized_projection() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(7),
                Some(i64::MIN),
                None,
                Some(i64::MAX),
                Some(8),
            ])),
            Arc::new(StringArray::from(vec![
                Some("padding"),
                Some("é\0"),
                None,
                Some("🦆"),
                Some("tail"),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
            ])),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(-2),
                None,
                Some(20000),
                Some(1),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(-0.0),
                None,
                Some(f64::INFINITY),
                Some(2.0),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some(-123), None, Some(987), Some(0)])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(NullArray::new(5)),
        ];
        let source = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let raw = RecordBatch::try_new(source.clone(), arrays).unwrap();
        let selected = [1, 0, 0, 6, 5, 4, 3, 2];
        let (plan, schema) = plan(&source, &selected);
        assert_eq!(columns(&plan, &source, &schema), Some(selected.to_vec()));
        for batch in [raw.slice(1, 3), raw.slice(2, 0)] {
            let budget = Budget::new(1 << 20);
            let (actual, lease) = project(&batch, &selected, schema.clone(), &budget).unwrap();
            let prepared = Prepared {
                auto_simple: None,
                internal_output: false,
                plan: plan.clone(),
                schema: source.clone(),
                relational: None,
            };
            let expected = execute::execute_stream(
                &prepared,
                &Options::default(),
                Budget::new(1 << 20),
                |send| send(batch.clone()),
            )
            .unwrap();
            assert_eq!(actual, expected.batch);
            assert_eq!(actual.num_rows(), batch.num_rows());
            assert!(Arc::ptr_eq(actual.column(0), batch.column(1)));
            if actual.num_rows() > 0 {
                assert_eq!(
                    actual
                        .column(5)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0)
                        .to_bits(),
                    (-0.0_f64).to_bits()
                );
            }
            drop(batch);
            assert!(budget.used() > 0);
            drop(actual);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn conversion_and_expression_barriers_keep_original_path() {
        for datatype in [DataType::Int32, DataType::Decimal128(16, 2)] {
            let source = Schema::new(vec![Field::new("x", datatype, true)]);
            let (p, out) = plan(&source, &[0]);
            assert!(columns(&p, &source, &out).is_none());
        }
        let source = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let (base, out) = plan(&source, &[0]);
        for change in 0..6 {
            let mut p = base.clone();
            match change {
                0 => p.aggregation = true,
                1 => {
                    p.filter = Some(Expr {
                        ty: Type::Bool,
                        kind: Kind::Literal(crate::expr::Literal::Bool(true)),
                    })
                }
                2 => p.order.push(bind::Sort {
                    column: 0,
                    descending: false,
                    nulls_first: false,
                }),
                3 => {
                    p.output[0] = Output::Expr(Expr {
                        ty: Type::Int,
                        kind: Kind::Literal(crate::expr::Literal::Num(1)),
                    })
                }
                4 => p.projection_pushdown = false,
                _ => p.columns.push(1),
            }
            assert!(columns(&p, &source, &out).is_none());
        }
    }

    #[test]
    fn reservation_failure_and_output_validation_release_every_lease() {
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let (_, schema) = plan(&batch.schema(), &[0]);
        let budget = Budget::new(1);
        assert!(project(&batch, &[0], schema, &budget).is_err());
        assert_eq!(budget.used(), 0);
        let budget = Budget::new(4096);
        let invalid = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        assert!(project(&batch, &[0], invalid, &budget).is_err());
        assert_eq!(budget.used(), 0);
        let empty = Arc::new(Schema::empty());
        let (output, lease) = project(&batch, &[], empty, &budget).unwrap();
        assert_eq!((output.num_columns(), output.num_rows()), (0, 3));
        drop(output);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn native_integer_projection_preserves_boundaries_nulls_and_wide_sources() {
        let wide_value = i128::from(i64::MAX) + 1;
        let source = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Type::Int.field("z", true),
        ]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
                Arc::new(
                    Decimal128Array::from(vec![Some(wide_value), None, Some(-wide_value)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let (plan, _) = plan(&source, &[0, 0, 1]);
        let output = output_schema(&plan, &source);
        for i in 0..2 {
            assert_eq!(output.field(i).data_type(), &DataType::Int64);
        }
        assert!(Type::is_wide_integer(output.field(2)));
        let selected = columns(&plan, &source, &output).unwrap();
        let budget = Budget::new(1 << 20);
        for input in [batch.clone(), batch.slice(1, 0)] {
            let (actual, lease) = project(&input, &selected, output.clone(), &budget).unwrap();
            assert!(Arc::ptr_eq(actual.column(0), input.column(0)));
            assert!(Arc::ptr_eq(actual.column(0), actual.column(1)));
            assert!(Arc::ptr_eq(actual.column(2), input.column(1)));
            assert_eq!(actual.num_rows(), input.num_rows());
            assert!(budget.used() >= actual.get_array_memory_size());
            drop(actual);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn computed_and_aggregate_integer_projections_keep_wide_fields() {
        let source = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        for sql in [
            "SELECT x + 1 AS y, x AS z",
            "SELECT SUM(x) AS y",
            "SELECT x AS y FROM t ORDER BY y",
        ] {
            let query = bind::parse(sql).unwrap();
            let plan = bind::bind(&query, &source, None, true).unwrap();
            let output = output_schema(&plan, &source);
            assert!(output.fields().iter().all(|f| Type::is_wide_integer(f)));
        }
    }
}
