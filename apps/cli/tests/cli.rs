use std::{process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    time::timeout,
};

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pgtest"));
    command.kill_on_drop(true).stdout(Stdio::piped()).stderr(Stdio::piped());
    // These must neither satisfy required flags nor override argument defaults.
    command
        .env("PGTEST_PG_HOST", "invalid.invalid")
        .env("PGTEST_PG_PORT", "not-a-port")
        .env("PGTEST_CREATION_POOL_CONNECTION", "0")
        .env("PGTEST_POOL_INITIAL_SIZE", "invalid")
        .env("RUST_LOG", "off");
    command
}

#[tokio::test]
async fn help_version_and_completion_work_without_postgres() {
    for args in [
        vec!["--help"],
        vec!["serve", "--help"],
        vec!["--version"],
        vec!["version"],
        vec!["version", "--help"],
        vec!["--bpaf-complete-style-bash"],
        vec!["--bpaf-complete-style-zsh"],
        vec!["--bpaf-complete-style-fish"],
    ] {
        let output =
            timeout(Duration::from_secs(5), command().args(&args).output()).await.unwrap().unwrap();
        assert!(output.status.success(), "{args:?}: {:?}", output);
        assert!(!output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
    for (args, expected) in [
        (vec!["--bpaf-complete-rev=7", ""], "serve"),
        (vec!["--bpaf-complete-rev=7", ""], "version"),
        (vec!["--bpaf-complete-rev=7", "serve", "--pg-"], "--pg-host"),
        (vec!["--bpaf-complete-rev=7", "serve", "--unix-socket-dir", ""], "_files -/"),
    ] {
        let output =
            timeout(Duration::from_secs(5), command().args(&args).output()).await.unwrap().unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(expected), "{args:?}: {text}");
    }
    let output = command()
        .args(["serve", "--listen-addr", "127.0.0.1", "--listen-port", "0"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--pg-host"));
}

#[tokio::test]
async fn version_reports_compiled_metadata_and_ignores_runtime_overrides() {
    let version = option_env!("PGTEST_VERSION").filter(|value| !value.is_empty()).unwrap_or("dev");
    let commit =
        option_env!("PGTEST_COMMIT_SHA").filter(|value| !value.is_empty()).unwrap_or("dev");
    let expected = format!("Version: {version} (commit {commit})");
    for arg in ["version", "--version", "-V"] {
        let output = timeout(
            Duration::from_secs(5),
            command()
                .arg(arg)
                .env("PGTEST_VERSION", "runtime-version")
                .env("PGTEST_COMMIT_SHA", "runtime-commit")
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.status.success(), "{arg}: {output:?}");
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim_end(), expected);
        assert!(output.stderr.is_empty());
    }
}

#[cfg(unix)]
mod unix {
    use pgtest_database_operations::{config::PostgresConfig, testcontainer::pg_container_config};
    use tokio::{process::Child, task::JoinHandle};
    use tokio_postgres::{Client, Config, NoTls};

    use super::*;

    fn server_command(config: &PostgresConfig) -> Command {
        let mut command = command();
        command.args([
            "serve",
            "--pg-host",
            &config.pgtest_pg_host,
            "--pg-port",
            &config.pgtest_pg_port.to_string(),
            "--pg-user",
            &config.pgtest_pg_user,
            "--pg-database",
            &config.pgtest_pg_database,
            "--pool-initial-size",
            "2",
            "--pool-starvation-threshold",
            "0",
        ]);
        command
    }

    async fn connect(config: &Config) -> (Client, JoinHandle<Result<(), tokio_postgres::Error>>) {
        let (client, connection) = config.connect(NoTls).await.unwrap();
        (client, tokio::spawn(connection))
    }

    async fn ready(
        child: &mut Child,
        tcp: bool,
        unix: bool,
    ) -> (Option<std::net::SocketAddr>, JoinHandle<String>) {
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let mut address = None;
        let mut unix_ready = false;
        while (tcp && address.is_none()) || (unix && !unix_ready) {
            let line = lines.next_line().await.unwrap().expect("server exited before listening");
            if line.contains("pgtest TCP listening") {
                address = Some(line.split("address=").nth(1).unwrap().trim().parse().unwrap());
            }
            if line.contains("pgtest Unix listening") {
                unix_ready = true;
            }
        }
        let logs = tokio::spawn(async move {
            let mut text = String::new();
            lines.into_inner().read_to_string(&mut text).await.unwrap();
            text
        });
        (address, logs)
    }

    async fn lifecycle(tcp: bool, unix: bool, signal: &str) {
        let upstream = pg_container_config().await;
        // Short paths also fit macOS sockaddr_un's smaller path limit.
        let directory = tempfile::Builder::new().prefix("pgcli-").tempdir_in("/tmp").unwrap();
        let mut command = server_command(&upstream);
        if tcp {
            command.args(["--listen-addr", "127.0.0.1", "--listen-port", "0"]);
        }
        if unix {
            command
                .arg("--unix-socket-dir")
                .arg(directory.path())
                .args(["--unix-socket-port", "7432"]);
        }
        let mut child = command.spawn().unwrap();
        let (address, logs) = ready(&mut child, tcp, unix).await;
        let socket = directory.path().join(".s.PGSQL.7432");
        assert_eq!(socket.exists(), unix);
        let mut frontend = Config::new();
        frontend.user("postgres").dbname(&format!("{}/lease-1", upstream.pgtest_pg_database));
        if let Some(address) = address {
            frontend.host("127.0.0.1").port(address.port());
        } else {
            frontend.host_path(directory.path()).port(7432);
        }
        let (application, application_task) = connect(&frontend).await;
        let row = application.query_one("SELECT current_database(), 42::int4", &[]).await.unwrap();
        let database: String = row.get(0);
        assert!(database.starts_with(&format!("{}_", upstream.pgtest_pg_database)));
        assert_eq!(row.get::<_, i32>(1), 42);
        if tcp && unix {
            let mut unix_frontend = Config::new();
            unix_frontend
                .host_path(directory.path())
                .port(7432)
                .user("postgres")
                .dbname(&format!("{}/lease-1", upstream.pgtest_pg_database));
            let (second, task) = connect(&unix_frontend).await;
            assert_eq!(
                second
                    .query_one("SELECT current_database()", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                database
            );
            drop(second);
            task.await.unwrap().unwrap();
        }
        let mut control_config = frontend.clone();
        control_config.dbname("pgtest");
        let (control, control_task) = connect(&control_config).await;
        assert!(
            control
                .query_one("SELECT pgtest_release($1::text)", &[&"lease-1"])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        let _ = application_task.await.unwrap();
        assert!(application.is_closed());
        assert!(frontend.connect(NoTls).await.is_err());

        // Leave both an application session and a control session open at
        // shutdown.
        frontend.dbname(&format!("{}/lease-2", upstream.pgtest_pg_database));
        let (active, active_task) = connect(&frontend).await;
        assert!(
            Command::new("kill")
                .args([signal, &child.id().unwrap().to_string()])
                .status()
                .await
                .unwrap()
                .success()
        );
        let status = child.wait().await.unwrap();
        let logs = logs.await.unwrap();
        assert!(status.success(), "server shutdown failed: {logs}");
        let _ = active_task.await.unwrap();
        let _ = control_task.await.unwrap();
        assert!(active.is_closed());
        assert!(control.is_closed());
        assert!(!socket.exists());
        if let Some(address) = address {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
    }

    #[tokio::test]
    async fn signals_interrupt_a_stalled_upstream_startup() {
        timeout(Duration::from_secs(10), async {
            for signal in ["-INT", "-TERM"] {
                let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let mut child = command()
                    .args([
                        "serve",
                        "--pg-host",
                        "127.0.0.1",
                        "--pg-port",
                        &upstream.local_addr().unwrap().port().to_string(),
                        "--pg-user",
                        "postgres",
                        "--pg-database",
                        "template",
                        "--listen-addr",
                        "127.0.0.1",
                        "--listen-port",
                        "0",
                    ])
                    .spawn()
                    .unwrap();
                // Accept the database connection but never finish startup.
                let (_connection, _) = upstream.accept().await.unwrap();
                assert!(
                    Command::new("kill")
                        .args([signal, &child.id().unwrap().to_string()])
                        .status()
                        .await
                        .unwrap()
                        .success()
                );
                assert!(child.wait().await.unwrap().success());
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn tcp_only_serves_and_shuts_down_on_sigterm() {
        timeout(Duration::from_secs(45), lifecycle(true, false, "-TERM")).await.unwrap();
    }

    #[tokio::test]
    async fn unix_only_serves_and_removes_socket_on_sigint() {
        timeout(Duration::from_secs(45), lifecycle(false, true, "-INT")).await.unwrap();
    }

    #[tokio::test]
    async fn combined_listeners_share_leases_and_shut_down_together() {
        timeout(Duration::from_secs(45), lifecycle(true, true, "-TERM")).await.unwrap();
    }

    #[tokio::test]
    async fn unix_bind_failure_closes_tcp_and_preserves_existing_file() {
        timeout(Duration::from_secs(45), async {
            let upstream = pg_container_config().await;
            let directory = tempfile::Builder::new().prefix("pgcli-").tempdir_in("/tmp").unwrap();
            let socket = directory.path().join(".s.PGSQL.6432");
            std::fs::write(&socket, "keep").unwrap();
            let output = server_command(&upstream)
                .args(["--listen-addr", "127.0.0.1", "--listen-port", "0", "--unix-socket-dir"])
                .arg(directory.path())
                .output()
                .await
                .unwrap();
            assert!(!output.status.success());
            let logs = String::from_utf8(output.stderr).unwrap();
            assert!(logs.contains("failed to start Unix listener"), "{logs}");
            let line = logs.lines().find(|line| line.contains("pgtest TCP listening")).unwrap();
            let address: std::net::SocketAddr =
                line.split("address=").nth(1).unwrap().trim().parse().unwrap();
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
            assert_eq!(std::fs::read_to_string(socket).unwrap(), "keep");
        })
        .await
        .unwrap();
    }
}
