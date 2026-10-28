//! Preserve pinned GenericDialect with approved empty IN lists and QUALIFY.
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::{keywords::Keyword, parser::Parser, tokenizer::Token};

#[derive(Debug)]
pub struct QueryDialect;
macro_rules! generic_flags {
    ($($name:ident),* $(,)?) => { $(
        fn $name(&self) -> bool { GenericDialect.$name() }
    )* };
}
impl Dialect for QueryDialect {
    // Retain GenericDialect's type-based parser branches as well as its flags.
    fn dialect(&self) -> std::any::TypeId {
        GenericDialect.dialect()
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_delimited_identifier_start(ch)
    }
    fn supports_in_empty_list(&self) -> bool {
        true
    }
    fn is_select_item_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        if !explicit && *kw == Keyword::QUALIFY {
            // GenericDialect otherwise consumes QUALIFY as an implicit alias
            // in SELECTs without FROM. Preserve previously legal aliases when
            // no predicate follows (including SELECT 1 qualify FROM ...).
            return match parser.peek_token().token {
                Token::EOF | Token::SemiColon | Token::Comma | Token::RParen => true,
                Token::Word(word) => {
                    word.keyword == Keyword::QUALIFY
                        || !GenericDialect.is_column_alias(&word.keyword, parser)
                }
                _ => false,
            };
        }
        if !explicit && *kw == Keyword::WINDOW {
            // `WINDOW name AS (` starts a WINDOW clause even without FROM
            // (DuckDB and PostgreSQL); GenericDialect would read WINDOW as an
            // implicit alias. Other uses of the word keep their meaning.
            if let (Token::Word(_), Token::Word(as_word), Token::LParen) = (
                parser.peek_nth_token(0).token,
                parser.peek_nth_token(1).token,
                parser.peek_nth_token(2).token,
            ) && as_word.keyword == Keyword::AS
            {
                return false;
            }
        }
        GenericDialect.is_select_item_alias(explicit, kw, parser)
    }
    // Forward every non-default GenericDialect flag in pinned sqlparser0.58.
    generic_flags!(
        supports_unicode_string_literal,
        supports_group_by_expr,
        supports_group_by_with_modifier,
        supports_left_associative_joins_without_parens,
        supports_connect_by,
        supports_match_recognize,
        supports_start_transaction_modifier,
        supports_window_function_null_treatment_arg,
        supports_dictionary_syntax,
        supports_window_clause_named_window_reference,
        supports_parenthesized_set_variables,
        supports_select_wildcard_except,
        support_map_literal_syntax,
        allow_extract_custom,
        allow_extract_single_quotes,
        supports_create_index_with_clause,
        supports_explain_with_utility_options,
        supports_limit_comma,
        supports_from_first_select,
        supports_projection_trailing_commas,
        supports_asc_desc_in_column_definition,
        supports_try_convert,
        supports_comment_on,
        supports_load_extension,
        supports_named_fn_args_with_assignment_operator,
        supports_struct_literal,
        supports_empty_projections,
        supports_nested_comments,
        supports_user_host_grantee,
        supports_string_escape_constant,
        supports_array_typedef_with_brackets,
        supports_match_against,
        supports_set_names,
        supports_comma_separated_set_assignments,
        supports_filter_during_aggregation,
        supports_select_wildcard_exclude,
    );
}
