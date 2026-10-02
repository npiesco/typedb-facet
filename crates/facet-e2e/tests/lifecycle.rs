/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 7: connection lifecycle. Clients that never finish authentication and
//! sessions that sit idle past their timeout are closed by Facet and give their
//! connection slot back, proven against real TypeDB, real Facet and real psql.

use std::{
    ffi::OsStr,
    time::{Duration, Instant},
};

use facet_e2e::{
    DATABASE, DEFINE_PROJECTION, FacetLimits, FacetOptions, FacetProcess, HeldPsql, PgTarget,
    TypeDbProcess, assert_psql_success, facet_binary, psql_binary, run_psql,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const PEOPLE: &str = "SELECT id, name FROM people ORDER BY id";

fn start_facet(
    typedb_origin: &str,
    workspace: &std::path::Path,
    limits: FacetLimits,
) -> FacetProcess {
    FacetProcess::start_with_options(
        &facet_binary().expect("Facet binary under test"),
        typedb_origin,
        workspace,
        &FacetOptions {
            limits,
            ..FacetOptions::default()
        },
    )
    .expect("start Facet with lifecycle [limits]")
}

fn assert_people(psql: &OsStr, target: &PgTarget) {
    let select = run_psql(psql, target, PEOPLE).expect("run psql");
    assert_psql_success(&select, PEOPLE);
    assert_eq!(
        String::from_utf8_lossy(&select.stdout)
            .lines()
            .collect::<Vec<_>>(),
        ["1|Alice", "2|Bob"]
    );
}

/// Reads until the server closes the socket, failing if it stays open past `limit`.
async fn read_until_closed(socket: &mut TcpStream, limit: Duration) -> (Vec<u8>, Duration) {
    let started = Instant::now();
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let remaining = limit.saturating_sub(started.elapsed());
        match tokio::time::timeout(remaining, socket.read(&mut buffer)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return (received, started.elapsed()),
            Ok(Ok(read)) => received.extend_from_slice(&buffer[..read]),
            Err(_) => panic!(
                "Facet kept the socket open for {limit:?}; received {:?}",
                String::from_utf8_lossy(&received)
            ),
        }
    }
}

fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut body = 196_608_u32.to_be_bytes().to_vec();
    for (key, value) in [("user", user), ("database", database)] {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut message = u32::try_from(body.len() + 4)
        .expect("startup length")
        .to_be_bytes()
        .to_vec();
    message.extend_from_slice(&body);
    message
}

#[test]
fn invalid_lifecycle_timeouts_are_rejected_at_startup() {
    for (key, limits) in [
        (
            "authentication_timeout",
            FacetLimits {
                authentication_timeout: Some("0s".to_owned()),
                ..FacetLimits::default()
            },
        ),
        (
            "idle_session_timeout",
            FacetLimits {
                idle_session_timeout: Some("soon".to_owned()),
                ..FacetLimits::default()
            },
        ),
    ] {
        let workspace = tempfile::tempdir().expect("create Facet E2E workspace");
        let result = FacetProcess::start_with_options(
            &facet_binary().expect("Facet binary under test"),
            "http://127.0.0.1:9",
            workspace.path(),
            &FacetOptions {
                limits,
                ..FacetOptions::default()
            },
        );
        let error = match result {
            Ok(_) => panic!("Facet started with an invalid [limits] {key}"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(&format!("invalid [limits] {key} ")),
            "Facet must reject the bad {key} by name; got: {error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authentication_timeout_closes_stalled_clients_and_frees_their_slot() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let facet = start_facet(
        typedb.http_origin(),
        workspace.path(),
        FacetLimits {
            max_connections: Some(1),
            authentication_timeout: Some("1s".to_owned()),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();
    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");

    // A client that connects and never speaks holds the only slot until the
    // authentication timeout closes it.
    let mut silent = TcpStream::connect(target.address)
        .await
        .expect("open silent socket");
    let (_, waited) = read_until_closed(&mut silent, Duration::from_secs(10)).await;
    assert!(
        waited >= Duration::from_millis(800),
        "the silent socket closed before the 1s authentication timeout: {waited:?}"
    );
    assert_people(&psql, &target);

    // A client that starts SCRAM and never answers the challenge is cancelled
    // with PostgreSQL's authentication-timeout error.
    let mut stalled = TcpStream::connect(target.address)
        .await
        .expect("open stalled socket");
    stalled
        .write_all(&startup_message(&target.user, DATABASE))
        .await
        .expect("send StartupMessage");
    let (received, waited) = read_until_closed(&mut stalled, Duration::from_secs(10)).await;
    assert!(
        waited >= Duration::from_millis(800),
        "the stalled SCRAM exchange closed before the 1s timeout: {waited:?}"
    );
    let received = String::from_utf8_lossy(&received);
    assert!(
        received.contains("SCRAM-SHA-256"),
        "Facet must have offered SCRAM before the timeout; got {received:?}"
    );
    assert!(
        received.contains("57014") && received.contains("canceling authentication due to timeout"),
        "the timeout must carry PostgreSQL's 57014 text; got {received:?}"
    );
    assert_people(&psql, &target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_session_timeout_terminates_idle_clients_and_frees_their_slot() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let facet = start_facet(
        typedb.http_origin(),
        workspace.path(),
        FacetLimits {
            max_connections: Some(1),
            idle_session_timeout: Some("1s".to_owned()),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();
    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");

    let mut idle = HeldPsql::open(&psql, &target).expect("open idle psql");
    idle.query_one(&format!("{PEOPLE};"), "2|Bob")
        .expect("the held session is live before it idles");

    let started = Instant::now();
    loop {
        let admitted = run_psql(&psql, &target, PEOPLE).expect("client after idle");
        if admitted.status.success() {
            break;
        }
        assert!(
            String::from_utf8_lossy(&admitted.stderr).contains("sorry, too many clients already"),
            "only the cap may refuse the client; got: {}",
            String::from_utf8_lossy(&admitted.stderr)
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the idle session never released its slot"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "the slot was free before the idle timeout could have fired"
    );

    let after = idle.query_one(&format!("{PEOPLE};"), "2|Bob");
    assert!(
        after.is_err(),
        "an idle-terminated session must not answer queries"
    );
    assert!(
        idle.stderr().contains("connection")
            && (idle.stderr().contains("lost")
                || idle.stderr().contains("closed")
                || idle
                    .stderr()
                    .contains("terminating connection due to idle-session timeout")),
        "psql must report the server-side termination; stderr: {}",
        idle.stderr()
    );

    // libpq only reads when it next writes, and Windows discards the FATAL
    // once that write is reset. tokio-postgres reads unsolicited messages, so it
    // observes the 57P05 ErrorResponse Facet sends before closing.
    let config = format!(
        "host={} port={} user={} password={} dbname={}",
        target.address.ip(),
        target.address.port(),
        target.user,
        target.password,
        target.database
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres must authenticate against Facet");
    let driver = tokio::spawn(connection);
    let rows = client
        .simple_query(PEOPLE)
        .await
        .expect("the tokio-postgres session is live before it idles");
    assert!(!rows.is_empty(), "the live session must return rows");
    let idled = Instant::now();
    let closed = tokio::time::timeout(Duration::from_secs(10), driver)
        .await
        .expect("Facet must terminate the idle tokio-postgres session")
        .expect("connection task must not panic");
    assert!(
        idled.elapsed() >= Duration::from_millis(800),
        "the session closed before the idle timeout elapsed"
    );
    let error = closed.expect_err("an idle-terminated connection must end with an error");
    let db = error
        .as_db_error()
        .unwrap_or_else(|| panic!("Facet must send an ErrorResponse, got: {error}"));
    assert_eq!(db.code().code(), "57P05");
    assert_eq!(db.severity(), "FATAL");
    assert_eq!(
        db.message(),
        "terminating connection due to idle-session timeout"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_sessions_outlive_the_idle_timeout() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let facet = start_facet(
        typedb.http_origin(),
        workspace.path(),
        FacetLimits {
            idle_session_timeout: Some("1s".to_owned()),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();
    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");

    let mut active = HeldPsql::open(&psql, &target).expect("open active psql");
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(4) {
        active
            .query_one(&format!("{PEOPLE};"), "2|Bob")
            .expect("a session querying every 400ms must stay open");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert!(
        !active.stderr().contains("idle-session timeout"),
        "an active session was terminated; stderr: {}",
        active.stderr()
    );
}
