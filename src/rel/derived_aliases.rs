//! Derived relation naming differs from CTE alias lists: excess aliases error,
//! partial lists retain remaining names, and even implicit duplicates deduplicate.
use super::*;

/// The bool says whether positional aliases were already applied. False keeps
/// the established qualifier/alias path, including unused dotted expression labels.
pub(super) fn apply(node: Node, aliases: &[s::TableAliasColumnDef]) -> Result<(Node, bool)> {
    let width = node.schema.fields().len();
    if aliases.len() > width {
        return Err("derived table alias column count exceeds available columns".into());
    }
    let names = node
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            aliases
                .get(index)
                .map_or(field.name().as_str(), |a| a.name.value.as_str())
        })
        .collect::<Vec<_>>();
    let mut seen = std::collections::HashSet::new();
    let unique = names.iter().all(|name| seen.insert(name.to_lowercase()));
    let dotted = names.iter().any(|name| name.contains('.'));
    let typed = aliases.iter().any(|alias| alias.data_type.is_some());
    let legacy_arity = aliases.is_empty() || aliases.len() == width;
    // Unchanged shapes must retain the old path. In particular COUNT(*) over
    // SELECT 1.5 needs no access to its dotted expression label. Dotted names
    // and typed full lists keep their prior handling, not a new blanket refusal.
    if legacy_arity && (unique || dotted || typed) {
        return Ok((node, false));
    }
    if typed {
        return Err("unsupported: typed derived table column aliases".into());
    }
    if dotted {
        return Err("unsupported: derived output names containing dots".into());
    }
    // Fully resolving the list enables implicit duplicate handling without
    // changing unaliased CTE or excess-CTE-list behavior.
    let names = names
        .into_iter()
        .map(|name| s::TableAliasColumnDef {
            name: s::Ident::with_quote('"', name),
            data_type: None,
        })
        .collect::<Vec<_>>();
    let node = crate::cte::column_aliases(node, &names)?;
    // Only newly admitted alias forms get DuckDB's unqualified projection
    // labels. Legacy forms above retain their established label behavior.
    let fields = node
        .schema
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.insert(Type::UNQUALIFIED_OUTPUT.into(), "true".into());
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        node.schema.metadata().clone(),
    ));
    Ok((
        Node {
            schema,
            estimated_rows: node.estimated_rows,
            op: RelOp::Rename {
                input: Box::new(node),
            },
        },
        true,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn derived_names_preserve_field_and_schema_metadata_and_row_estimate() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Type::Int.field("x", true).with_nullable(false),
                Type::Int
                    .field("X", false)
                    .with_metadata([("origin".into(), "second".into())].into()),
            ],
            [("source".into(), "derived".into())].into(),
        ));
        let original = schema.clone();
        let (result, renamed) = apply(
            Node {
                schema,
                estimated_rows: 73,
                op: RelOp::Empty,
            },
            &[],
        )
        .unwrap();
        assert!(renamed);
        assert_eq!(result.estimated_rows, 73);
        assert_eq!(result.schema.metadata(), original.metadata());
        for (a, b) in result.schema.fields().iter().zip(original.fields()) {
            let mut metadata = a.metadata().clone();
            metadata.remove(Type::UNQUALIFIED_OUTPUT);
            assert_eq!(&metadata, b.metadata());
            assert_eq!(a.data_type(), b.data_type());
            assert_eq!(a.is_nullable(), b.is_nullable());
        }
        assert_eq!(result.schema.field(0).name(), "x");
        assert_eq!(result.schema.field(1).name(), "X_1");
    }
}
