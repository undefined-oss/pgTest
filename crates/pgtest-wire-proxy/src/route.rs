//! Interpret startup parameters without looking up or allocating a database.

use pgtest::LeaseId;
use pgwire::messages::startup::Startup;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Route<'a> {
    Control,
    Application { template: &'a str, lease: LeaseId },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RouteError {
    ReplicationUnsupported,
    MissingDatabase,
    InvalidDatabase,
    TemplateMismatch,
}

impl RouteError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::ReplicationUnsupported => "0A000",
            Self::MissingDatabase | Self::TemplateMismatch => "3D000",
            Self::InvalidDatabase => "22023",
        }
    }

    pub(crate) fn message(&self) -> &'static str {
        match self {
            Self::ReplicationUnsupported => "replication connections are not supported",
            Self::MissingDatabase => "database is required in the connection URI",
            Self::InvalidDatabase => "expected template/lease-id with a valid lease ID",
            Self::TemplateMismatch => "database does not match the configured template",
        }
    }
}

/// Validate startup and the configured template before the caller attaches a
/// lease. The reserved control database is independent of the template name.
#[hotpath::measure]
pub(crate) fn parse<'a>(
    startup: &'a Startup,
    configured_template: &str,
) -> Result<Route<'a>, RouteError> {
    let parameters = &startup.parameters;
    if parameters.contains_key("replication") {
        return Err(RouteError::ReplicationUnsupported);
    }
    let database = parameters.get("database").ok_or(RouteError::MissingDatabase)?;
    if database == "pgtest" {
        return Ok(Route::Control);
    }
    let (template, lease) = database.split_once('/').ok_or(RouteError::InvalidDatabase)?;
    if template.is_empty() {
        return Err(RouteError::InvalidDatabase);
    }
    // LeaseId owns the shared length/UTF-8/separator rules. Do not trim or
    // normalize: case and whitespace are part of the lease's identity.
    let lease = LeaseId::new(lease).map_err(|_| RouteError::InvalidDatabase)?;
    if template != configured_template {
        return Err(RouteError::TemplateMismatch);
    }
    Ok(Route::Application { template, lease })
}

#[cfg(test)]
mod tests {
    use pgwire::messages::startup::Startup;

    use super::{Route, RouteError, parse};

    fn startup(database: &str) -> Startup {
        let mut startup = Startup::new();
        startup.parameters.insert("database".into(), database.into());
        startup
    }

    #[test]
    fn application_route_preserves_template_and_lease_identity() {
        for lease in ["demo".to_owned(), " Mixed Case 雪 ".to_owned(), "é".repeat(128)] {
            let startup = startup(&format!("Template/{lease}"));
            let Route::Application { template, lease: parsed } =
                parse(&startup, "Template").unwrap()
            else {
                panic!("expected an application route");
            };
            assert_eq!(template, "Template");
            assert_eq!(parsed.as_ref(), lease);
            assert_eq!(startup.parameters["database"], format!("Template/{lease}"));
        }
    }

    #[test]
    fn invalid_application_names_are_rejected() {
        for database in [
            "".to_owned(),
            "template".to_owned(),
            "/demo".to_owned(),
            "template/".to_owned(),
            "template/a/b".to_owned(),
            "template/a\0b".to_owned(),
            format!("template/{}", "a".repeat(257)),
            format!("template/{}", "é".repeat(129)),
        ] {
            let error = parse(&startup(&database), "template").unwrap_err();
            assert_eq!(error, RouteError::InvalidDatabase, "{database:?}");
            assert_eq!(error.code(), "22023");
        }
    }

    #[test]
    fn database_is_required_even_when_user_is_present() {
        let mut startup = Startup::new();
        startup.parameters.insert("user".into(), "alice".into());
        let error = parse(&startup, "template").unwrap_err();
        assert_eq!(error, RouteError::MissingDatabase);
        assert_eq!(error.code(), "3D000");
    }

    #[test]
    fn control_route_is_reserved_and_replication_is_rejected_first() {
        assert_eq!(parse(&startup("pgtest"), "template").unwrap(), Route::Control);
        // Like the existing proxy, reject any replication parameter, even
        // "false", before interpreting the database name.
        for database in [None, Some("pgtest"), Some("template/demo")] {
            for replication in ["true", "database", "false"] {
                let mut startup = database.map_or_else(Startup::new, startup);
                startup.parameters.insert("replication".into(), replication.into());
                let error = parse(&startup, "template").unwrap_err();
                assert_eq!(error, RouteError::ReplicationUnsupported);
                assert_eq!(error.code(), "0A000");
            }
        }
    }

    #[test]
    fn template_matching_is_exact_and_follows_lease_validation() {
        for database in ["other/demo", "Template/demo", " template/demo"] {
            let error = parse(&startup(database), "template").unwrap_err();
            assert_eq!(error, RouteError::TemplateMismatch);
            assert_eq!(error.code(), "3D000");
        }
        // Malformed routes retain their original error, even for another
        // template.
        assert_eq!(parse(&startup("other/a/b"), "template"), Err(RouteError::InvalidDatabase));
        assert!(matches!(
            parse(&startup("template/demo"), "template"),
            Ok(Route::Application { .. })
        ));
        assert!(matches!(parse(&startup("pgtest/demo"), "pgtest"), Ok(Route::Application { .. })));
    }
}
