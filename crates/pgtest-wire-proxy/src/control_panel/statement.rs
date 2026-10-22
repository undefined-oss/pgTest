use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "./control_panel/grammar.pest"]
struct PgTestQueryControlPanelParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgTestQueryTypeControlStatement {
    Release(LeaseArgument),
    Ping,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("unsupported control-panel command")]
pub(super) struct InvalidControlQuery;

impl PgTestQueryTypeControlStatement {
    pub(super) fn parse(
        query: &str,
    ) -> Result<PgTestQueryTypeControlStatement, InvalidControlQuery> {
        let mut value = match PgTestQueryControlPanelParser::parse(Rule::query, query) {
            Ok(value) => value,
            Err(_) => {
                return Err(InvalidControlQuery);
            }
        };

        let first_pair = value.next().and_then(|pair| match pair.as_rule() {
            Rule::ping => Some(PgTestQueryTypeControlStatement::Ping),
            Rule::release => {
                let text_literal_pair_child = pair.into_inner().next()?;

                match text_literal_pair_child.as_rule() {
                    Rule::text_literal => {
                        let literal = text_literal_pair_child
                            .as_str()
                            .strip_prefix('\'')
                            .and_then(|partial_normalized| partial_normalized.strip_suffix('\''))
                            .map(|unquote| unquote.replace("''", "'"))?;

                        Some(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                            literal,
                        )))
                    }
                    Rule::parameter => {
                        Some(PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter {
                            index: 0,
                        }))
                    }
                    _ => None,
                }
            }
            _ => None,
        });

        first_pair.ok_or(InvalidControlQuery)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseArgument {
    Literal(String),
    Parameter { index: usize },
}

#[cfg(test)]
mod tests {
    use super::{LeaseArgument, PgTestQueryTypeControlStatement};

    #[test]
    fn parse_valid_ping_queries() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse("SELECT 1").expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_ping_queries_with_trailing_semicolon() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse("select 1;").expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_ping_queries_with_intercalate_new_line() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse(
                "SELECT
            1"
            )
            .expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_simple_release_command() {
        assert_eq!(
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(String::from(
                "test-42"
            ))),
            PgTestQueryTypeControlStatement::parse("SELECT pgtest_release('test-42')")
                .expect("query successfully parsed")
        );
    }

    #[test]
    fn parse_valid_middle_quote_characters_release_command() {
        assert_eq!(
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(String::from(
                "suite's test"
            ))),
            PgTestQueryTypeControlStatement::parse("SELECT pgtest_release('suite''s test')")
                .expect("query successfully parsed")
        );
    }

    #[test]
    fn parse_valid_ping_whitespace_and_case_variations() {
        for query in [" SeLeCt\t1 ", "\tSELECT\r\n1 ;\n", "SELECT 1\t;\r\n"] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Ping),
                "expected a ping for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_literals_preserves_and_decodes_the_value() {
        // Empty IDs are syntactically valid; lease validation is a separate
        // step.
        let cases = [
            ("SELECT pgtest_release('')", ""),
            ("SELECT pgtest_release('$1')", "$1"),
            ("SELECT pgtest_release('MiXeD Case')", "MiXeD Case"),
            ("SELECT pgtest_release('  test-42  ')", "  test-42  "),
            ("SELECT pgtest_release('prueba-ñ-🦀')", "prueba-ñ-🦀"),
            ("SELECT pgtest_release('line\nbreak')", "line\nbreak"),
            ("SELECT pgtest_release('a''b''c')", "a'b'c"),
            ("SELECT pgtest_release('''test''')", "'test'"),
            ("SELECT pgtest_release('''''')", "''"),
            (
                "SELECT pgtest_release('SELECT 1; DROP DATABASE example;')",
                "SELECT 1; DROP DATABASE example;",
            ),
        ];

        for (query, expected) in cases {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                    expected.to_owned()
                ))),
                "incorrect literal value for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_whitespace_and_case_variations() {
        for query in [
            " select PGTEST_RELEASE('test-42'); ",
            "SELECT\npgtest_release(\t'test-42'\r\n) ;",
            "SELECT pgtest_release ('test-42')",
            "\tSeLeCt\r\nPgTeSt_ReLeAsE \t( 'test-42' ) ;\n",
        ] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                    "test-42".to_owned()
                ))),
                "expected a literal release for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_parameters_use_zero_based_index() {
        for query in [
            "SELECT pgtest_release($1)",
            "SELECT pgtest_release($1::text)",
            "select PGTEST_RELEASE($1::TEXT);",
            "SELECT pgtest_release( $1 :: text )",
            "SELECT pgtest_release($1\n::\tTeXt);",
            "SELECT pgtest_release ( $1::text );",
        ] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { index: 0 })),
                "expected the first parameter for {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_invalid_ping_queries() {
        for query in [
            "",
            " \t\r\n",
            ";",
            "SELECT",
            "SELECT1",
            "SELECT 10",
            "SELECT 01",
            "SELECT 1.0",
            "SELECT 1 extra",
            "SELECT 1 + 1",
            "SELECT 1, 1",
            "SELECT '1'",
            "SELECT 1;;",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_release_literals() {
        for query in [
            "SELECT pgtest_release()",
            "SELECT pgtest_release(test-42)",
            "SELECT pgtest_release(\"test-42\")",
            "SELECT pgtest_release(NULL)",
            "SELECT pgtest_release(1)",
            "SELECT pgtest_release('unfinished)",
            "SELECT pgtest_release('suite's test')",
            "SELECT pgtest_release('test-42''')'",
            "SELECT pgtest_release('test-42'",
            "SELECT pgtest_release 'test-42')",
            "SELECT pgtest_release('a', 'b')",
            "SELECT pgtest_release('a' 'b')",
            "SELECT pgtest_release('test-42'))",
            "SELECT pgtest_release('test-42');;",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_unsupported_parameters_and_casts() {
        for query in [
            "SELECT pgtest_release($0)",
            "SELECT pgtest_release($2)",
            "SELECT pgtest_release($10)",
            "SELECT pgtest_release($01)",
            "SELECT pgtest_release($-1)",
            "SELECT pgtest_release($ 1)",
            "SELECT pgtest_release($1::)",
            "SELECT pgtest_release($1::varchar)",
            "SELECT pgtest_release($1::integer)",
            "SELECT pgtest_release($1::text_extra)",
            "SELECT pgtest_release($1::text::text)",
            "SELECT pgtest_release($1, $2)",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_other_commands_and_trailing_sql() {
        for query in [
            "BEGIN",
            "COMMIT",
            "DROP DATABASE example",
            "SELECT other_function('test-42')",
            "SELECTpgtest_release('test-42')",
            "SELECT pgtest_release_extra('test-42')",
            "SELECT 1; SELECT 1",
            "SELECT pgtest_release('test-42'); SELECT 1",
            "SELECT pgtest_release($1::text); SELECT 1",
            "SELECT pgtest_release('test-42') trailing",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }
}
