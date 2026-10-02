/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 8: real analytics clients. DuckDB's postgres extension and dbt-postgres
//! read TypeDB projections through Facet at the same time, proven against real
//! TypeDB, real Facet, real DuckDB and real dbt.

use std::{
    path::Path,
    process::{Child, Command, Output, Stdio},
};

use facet_e2e::{
    DEFINE_PROJECTION, FacetProcess, PgTarget, TypeDbProcess, assert_psql_success, dbt_binary,
    facet_binary, psql_binary, python_binary, run_psql,
};

const DUCKDB_SCRIPT: &str = r#"
import os, duckdb
dsn = os.environ["FACET_DSN"].replace("'", "''")
con = duckdb.connect()
con.execute("LOAD postgres")
con.execute(f"ATTACH '{dsn}' AS f (TYPE postgres, READ_ONLY)")
for name, kind in con.execute(
    "SELECT column_name, data_type FROM duckdb_columns() "
    "WHERE database_name = 'f' AND table_name = 'people' ORDER BY column_index"
).fetchall():
    print(f"column|{name}|{kind}")
for row in con.execute("SELECT id, name FROM f.public.people ORDER BY id").fetchall():
    print(f"row|{row[0]}|{row[1]}")
for row in con.execute("SELECT count(*), max(name) FROM f.public.people").fetchall():
    print(f"aggregate|{row[0]}|{row[1]}")
"#;

const DUCKDB_EXPECTED: [&str; 5] = [
    "column|id|BIGINT",
    "column|name|VARCHAR",
    "row|1|Alice",
    "row|2|Bob",
    "aggregate|2|Bob",
];

fn libpq_dsn(target: &PgTarget) -> String {
    format!(
        "host={} port={} user={} password={} dbname={} sslmode=disable",
        target.address.ip(),
        target.address.port(),
        target.user,
        target.password,
        target.database
    )
}

fn spawn_duckdb(target: &PgTarget) -> Child {
    Command::new(python_binary().expect("FACET_E2E_PYTHON"))
        .arg("-c")
        .arg(DUCKDB_SCRIPT)
        .env("FACET_DSN", libpq_dsn(target))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn DuckDB client")
}

fn write_dbt_project(directory: &Path, target: &PgTarget) {
    std::fs::create_dir_all(directory.join("models")).expect("create dbt models directory");
    std::fs::write(
        directory.join("dbt_project.yml"),
        "name: facet_clients\nversion: '1.0'\nconfig-version: 2\nprofile: facet_clients\nmodel-paths: ['models']\n",
    )
    .expect("write dbt_project.yml");
    std::fs::write(
        directory.join("profiles.yml"),
        format!(
            "facet_clients:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n      host: {}\n      port: {}\n      user: {}\n      password: {}\n      dbname: {}\n      schema: public\n      threads: 2\n      sslmode: disable\n",
            target.address.ip(),
            target.address.port(),
            target.user,
            target.password,
            target.database
        ),
    )
    .expect("write profiles.yml");
    std::fs::write(
        directory.join("models").join("sources.yml"),
        "version: 2\nsources:\n  - name: facet\n    schema: public\n    tables:\n      - name: people\n        columns:\n          - name: id\n            data_tests: [not_null, unique]\n          - name: name\n            data_tests: [not_null]\n",
    )
    .expect("write sources.yml");
}

fn spawn_dbt(directory: &Path, arguments: &[&str]) -> Child {
    Command::new(dbt_binary().expect("FACET_E2E_DBT"))
        .args(arguments)
        .arg("--profiles-dir")
        .arg(directory)
        .arg("--project-dir")
        .arg(directory)
        .current_dir(directory)
        .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "False")
        .env("DBT_USE_COLORS", "False")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn dbt")
}

fn finished(child: Child, what: &str) -> String {
    let output: Output = child.wait_with_output().expect("wait for client");
    let stdout = String::from_utf8_lossy(&output.stdout).replace("\r\n", "\n");
    assert!(
        output.status.success(),
        "{what} failed with {:?}\nstdout:\n{stdout}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_and_dbt_read_projections_concurrently() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("Facet binary under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("start Facet");
    let target = facet.target();
    let define = run_psql(&psql_binary(), &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");

    let show_project = workspace.path().join("dbt-show");
    let test_project = workspace.path().join("dbt-test");
    write_dbt_project(&show_project, &target);
    write_dbt_project(&test_project, &target);

    // Every client is started before any is awaited, so their sessions overlap
    // on the same Facet server.
    let duckdb_first = spawn_duckdb(&target);
    let duckdb_second = spawn_duckdb(&target);
    let dbt_show = spawn_dbt(
        &show_project,
        &[
            "--quiet",
            "show",
            "--inline",
            "select id, name from {{ source('facet', 'people') }} order by id",
            "--output",
            "json",
        ],
    );
    let dbt_test = spawn_dbt(&test_project, &["test"]);

    for (child, what) in [
        (duckdb_first, "first DuckDB client"),
        (duckdb_second, "second DuckDB client"),
    ] {
        let stdout = finished(child, what);
        assert_eq!(
            stdout.lines().collect::<Vec<_>>(),
            DUCKDB_EXPECTED,
            "{what}"
        );
    }

    let show = finished(dbt_show, "dbt show");
    let json_start = show.find('{').expect("dbt show prints JSON");
    let shown: serde_json::Value =
        serde_json::from_str(show[json_start..].trim()).expect("parse dbt show JSON");
    assert_eq!(
        shown["show"],
        serde_json::json!([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    );

    let tested = finished(dbt_test, "dbt test");
    assert!(
        tested.contains("PASS=3 WARN=0 ERROR=0 SKIP=0"),
        "dbt test must pass all three source tests:\n{tested}"
    );
}
