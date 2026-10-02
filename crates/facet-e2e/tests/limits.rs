/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 6: bounded memory. Projection rows, TypeDB response bytes and client
//! connections are capped, proven against real TypeDB, real Facet and real psql.

use std::{
    ffi::OsStr,
    process::Output,
    time::{Duration, Instant},
};

use facet_e2e::{
    DEFINE_PROJECTION, FacetLimits, FacetOptions, FacetProcess, HeldPsql, INSERT_THIRD, PgTarget,
    TypeDbProcess, assert_psql_success, facet_binary, psql_binary, run_psql,
};

const SECOND_PROJECTION: &str = concat!(
    "define projection everyone(id: integer, name: string) as ",
    r#"'match $p isa person; fetch { "id": $p.id, "name": $p.name };';"#
);

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn rows(psql: &OsStr, target: &PgTarget, sql: &str) -> Vec<String> {
    let select = run_psql(psql, target, sql).expect("run psql");
    assert_psql_success(&select, sql);
    String::from_utf8_lossy(&select.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

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
            refresh_interval: "1s".to_owned(),
            limits,
            ..FacetOptions::default()
        },
    )
    .expect("start Facet with [limits]")
}

#[test]
fn zero_limits_are_rejected_at_startup() {
    for (key, limits) in [
        (
            "max_rows",
            FacetLimits {
                max_rows: Some(0),
                ..FacetLimits::default()
            },
        ),
        (
            "max_response_bytes",
            FacetLimits {
                max_response_bytes: Some(0),
                ..FacetLimits::default()
            },
        ),
        (
            "max_connections",
            FacetLimits {
                max_connections: Some(0),
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
            Ok(_) => panic!("Facet started with [limits] {key} = 0"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(&format!(
                "invalid [limits] {key} = 0; expected a positive integer"
            )),
            "Facet must reject {key} = 0 by name; got: {error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_cap_rejects_definitions_and_keeps_previous_rows_on_refresh() {
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
            max_rows: Some(2),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();

    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION at the row cap");
    assert_eq!(
        rows(&psql, &target, "SELECT id, name FROM people ORDER BY id"),
        ["1|Alice", "2|Bob"]
    );

    typedb
        .execute_typeql("write", INSERT_THIRD, true)
        .await
        .expect("send third row to TypeDB")
        .expect_success("insert third row")
        .expect("third row committed");

    let expected_log = format!(
        "Facet refresh of {}.people kept previous rows: projection \"people\" exceeded [limits] max_rows = 2",
        facet_e2e::DATABASE
    );
    let started = Instant::now();
    while !facet.stderr().contains(&expected_log) {
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "refresh never reported the row cap; stderr:\n{}",
            facet.stderr()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        rows(&psql, &target, "SELECT id, name FROM people ORDER BY id"),
        ["1|Alice", "2|Bob"],
        "an over-cap refresh must keep the previous generation"
    );

    let over = run_psql(&psql, &target, SECOND_PROJECTION).expect("define over cap");
    assert!(
        !over.status.success(),
        "DEFINE over the row cap must fail; stdout: {}",
        String::from_utf8_lossy(&over.stdout)
    );
    assert!(
        stderr(&over).contains("ERROR:  projection \"everyone\" exceeded [limits] max_rows = 2"),
        "DEFINE must name the projection and the cap; got: {}",
        stderr(&over)
    );
    let missing = run_psql(&psql, &target, "SELECT id FROM everyone").expect("select missing");
    assert!(
        !missing.status.success(),
        "a rejected DEFINE must not publish a projection; stdout: {}",
        String::from_utf8_lossy(&missing.stdout)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_cap_sqlstate_is_program_limit_exceeded() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_three()
        .await
        .expect("schema and three rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let facet = start_facet(
        typedb.http_origin(),
        workspace.path(),
        FacetLimits {
            max_rows: Some(2),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let sql = format!("\\set VERBOSITY verbose\n{DEFINE_PROJECTION}");
    let mut command = std::process::Command::new(&psql);
    let target = facet.target();
    let over = command
        .arg("--no-psqlrc")
        .arg("--host")
        .arg(target.address.ip().to_string())
        .arg("--port")
        .arg(target.address.port().to_string())
        .arg("--username")
        .arg(&target.user)
        .arg("--dbname")
        .arg(&target.database)
        .arg("--set")
        .arg("ON_ERROR_STOP=1")
        .env("PGPASSWORD", &target.password)
        .env("PGSSLMODE", "disable")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("psql stdin")
                .write_all(sql.as_bytes())?;
            child.wait_with_output()
        })
        .expect("run verbose psql");
    assert!(
        stderr(&over)
            .contains("ERROR:  54000: projection \"people\" exceeded [limits] max_rows = 2"),
        "row-cap errors must carry SQLSTATE 54000; got: {}",
        stderr(&over)
    );
    let missing = run_psql(&psql, &target, "SELECT id FROM people").expect("select missing");
    assert!(
        !missing.status.success(),
        "an over-cap DEFINE must not publish rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_byte_cap_rejects_oversized_typedb_answers() {
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
            max_response_bytes: Some(64),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();
    let over = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define over byte cap");
    assert!(
        !over.status.success(),
        "DEFINE over the byte cap must fail; stdout: {}",
        String::from_utf8_lossy(&over.stdout)
    );
    assert!(
        stderr(&over).contains(
            "ERROR:  TypeDB response for projection \"people\" exceeded [limits] max_response_bytes = 64"
        ),
        "DEFINE must name the byte cap; got: {}",
        stderr(&over)
    );
    let missing = run_psql(&psql, &target, "SELECT id FROM people").expect("select missing");
    assert!(
        !missing.status.success(),
        "an over-cap DEFINE must not publish rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_cap_refuses_extra_clients_and_releases_on_disconnect() {
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
            max_connections: Some(2),
            ..FacetLimits::default()
        },
    );
    let psql = psql_binary();
    let target = facet.target();
    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");

    let mut first = HeldPsql::open(&psql, &target).expect("open first held connection");
    first
        .query_one("SELECT id, name FROM people ORDER BY id;", "2|Bob")
        .expect("first held connection is live");
    let mut second = HeldPsql::open(&psql, &target).expect("open second held connection");
    second
        .query_one("SELECT id, name FROM people ORDER BY id;", "2|Bob")
        .expect("second held connection is live");

    let refused =
        run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id").expect("third client");
    assert!(
        !refused.status.success() && refused.stdout.is_empty(),
        "a third client must be refused at max_connections = 2; stdout: {}",
        String::from_utf8_lossy(&refused.stdout)
    );
    assert!(
        stderr(&refused).contains("FATAL:  sorry, too many clients already"),
        "the refusal must match PostgreSQL's 53300 text; got: {}",
        stderr(&refused)
    );

    first
        .kill()
        .expect("kill the first held psql without Terminate");

    let started = Instant::now();
    loop {
        let admitted = run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id")
            .expect("client after release");
        if admitted.status.success() {
            assert_eq!(
                String::from_utf8_lossy(&admitted.stdout)
                    .lines()
                    .collect::<Vec<_>>(),
                ["1|Alice", "2|Bob"]
            );
            break;
        }
        assert!(
            stderr(&admitted).contains("sorry, too many clients already"),
            "only the cap may refuse the client; got: {}",
            stderr(&admitted)
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a killed client's slot was never released"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    second
        .query_one("SELECT id, name FROM people ORDER BY id;", "2|Bob")
        .expect("the surviving held connection is unaffected");
}
