/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 3 RED tests for durable definitions and DDL lock scope.

use std::{
    ffi::OsString,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use facet_e2e::{
    DEFINE_PROJECTION, DelayedHttpProxy, FacetProcess, PgTarget, TypeDbProcess,
    assert_psql_success, facet_binary, psql_binary, run_psql,
};
use facet_state::StateStore;

const DEFINE_NAMES: &str = concat!(
    "define projection people_names(name: string) as ",
    r#"'match $p isa person; fetch { "name": $p.name };';"#
);

const FAILED_REDEFINE: &str = concat!(
    "define projection people(id: integer, name: integer) as ",
    r#"'match $p isa person; fetch { "id": $p.id, "name": $p.name };';"#
);

const DEFINE_MIXED_CASE: &str = concat!(
    "define projection People(ID: integer, Name: string) as ",
    r#"'match $p isa person; fetch { "ID": $p.id, "Name": $p.name };';"#
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_definition_survives_facet_process_restart() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create persistent Facet workspace");
    let binary = facet_binary().expect("Facet binary under test");
    let psql = psql_binary();

    {
        let facet = FacetProcess::start(&binary, typedb.http_origin(), workspace.path())
            .expect("start first owned Facet process");
        let define =
            run_psql(&psql, &facet.target(), DEFINE_MIXED_CASE).expect("define projection");
        assert_psql_success(&define, "initial DEFINE PROJECTION");
        assert_people_rows(&psql, &facet.target(), "before restart");
        let undefine = run_psql(&psql, &facet.target(), "UNDEFINE PROJECTION People;")
            .expect("undefine first successful generation");
        assert_psql_success(&undefine, "UNDEFINE PROJECTION");
        let redefine =
            run_psql(&psql, &facet.target(), DEFINE_MIXED_CASE).expect("redefine projection");
        assert_psql_success(&redefine, "second successful DEFINE PROJECTION");
        assert_people_rows(&psql, &facet.target(), "after redefine");
    }

    let state_path = workspace.path().join("facet-state.sqlite3");
    let stored = StateStore::open(&state_path)
        .expect("open real Facet SQLite state")
        .load_all()
        .expect("load persisted generations");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].database, "facet-e2e");
    assert_eq!(stored[0].generation, 2);
    assert_eq!(stored[0].definition.name(), "People");
    assert_eq!(
        stored[0]
            .definition
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["ID", "Name"],
        "persisted definition must be the same one materialized and published"
    );

    let restarted = FacetProcess::start(&binary, typedb.http_origin(), workspace.path())
        .expect("restart Facet with the identical state path");
    let select = run_psql(
        &psql,
        &restarted.target(),
        "SELECT id, name FROM people ORDER BY id",
    )
    .expect("query persisted definition after restart");
    assert!(
        select.status.success()
            && String::from_utf8_lossy(&select.stdout)
                .lines()
                .eq(["1|Alice", "2|Bob"]),
        "projection definition did not survive restart on state path {}.\nstdout:\n{}\nstderr:\n{}",
        state_path.display(),
        String::from_utf8_lossy(&select.stdout),
        String::from_utf8_lossy(&select.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_define_does_not_block_select_on_existing_projection() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let typedb_address = typedb
        .http_origin()
        .strip_prefix("http://")
        .expect("plain loopback TypeDB origin")
        .parse()
        .expect("TypeDB HTTP socket address");
    let delayed_typedb =
        DelayedHttpProxy::start(typedb_address).expect("start retained TypeDB forwarding proxy");
    let workspace = tempfile::tempdir().expect("create Facet lock-scope workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("Facet binary under test"),
        &delayed_typedb.origin(),
        workspace.path(),
    )
    .expect("start Facet through the real TypeDB proxy");
    let psql = psql_binary();
    let target = facet.target();

    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define existing projection");
    assert_psql_success(&define, "initial DEFINE PROJECTION");
    assert_people_rows(&psql, &target, "before delayed DEFINE");

    delayed_typedb.arm_query_response();
    let define_thread = spawn_psql(psql.clone(), target.clone(), DEFINE_NAMES);
    let response_is_blocked = delayed_typedb.wait_for_delayed_response(Duration::from_secs(10));
    if !response_is_blocked {
        let define = define_thread.join().expect("join DEFINE process");
        panic!(
            "transparent proxy never received a real /v1/query response to delay.\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&define.stdout),
            String::from_utf8_lossy(&define.stderr)
        );
    }
    assert!(
        !define_thread.is_finished(),
        "different DEFINE completed while its real TypeDB response was still blocked"
    );

    let (select_tx, select_rx) = mpsc::channel();
    let select_psql = psql.clone();
    let select_target = target.clone();
    let select_started = Instant::now();
    let select_thread = thread::spawn(move || {
        let output = run_psql(
            &select_psql,
            &select_target,
            "SELECT id, name FROM people ORDER BY id",
        );
        let _ = select_tx.send(output);
    });

    let select = select_rx.recv_timeout(Duration::from_secs(5));
    let define_remained_blocked = !define_thread.is_finished();
    delayed_typedb.release_response();
    let define = define_thread.join().expect("join delayed DEFINE process");
    select_thread
        .join()
        .expect("join concurrent SELECT process");

    let select = select.unwrap_or_else(|_| {
        panic!(
            "SELECT on an existing projection did not complete within five seconds while a different DEFINE waited on real TypeDB"
        )
    });
    let select = select.expect("run concurrent SELECT");
    assert_psql_success(&select, "concurrent SELECT");
    assert_eq!(
        String::from_utf8_lossy(&select.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["1|Alice", "2|Bob"]
    );
    assert!(
        define_remained_blocked,
        "different DEFINE completed before the proxy released its real TypeDB response"
    );
    assert_psql_success(&define, "delayed DEFINE after release");
    eprintln!(
        "existing SELECT completed in {:?} while different DEFINE remained blocked",
        select_started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_redefine_keeps_previous_generation() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create generation workspace");
    let binary = facet_binary().expect("Facet binary under test");
    let psql = psql_binary();

    {
        let facet = FacetProcess::start(&binary, typedb.http_origin(), workspace.path())
            .expect("start first Facet generation");
        let define =
            run_psql(&psql, &facet.target(), DEFINE_PROJECTION).expect("define initial generation");
        assert_psql_success(&define, "initial DEFINE PROJECTION");

        let failed =
            run_psql(&psql, &facet.target(), FAILED_REDEFINE).expect("submit failed redefine");
        assert!(
            !failed.status.success(),
            "type-invalid redefine unexpectedly succeeded:\n{}",
            String::from_utf8_lossy(&failed.stdout)
        );
        assert!(
            String::from_utf8_lossy(&failed.stderr).contains("expected integer projection cell"),
            "redefine failed for the wrong reason:\n{}",
            String::from_utf8_lossy(&failed.stderr)
        );
        assert_people_rows(
            &psql,
            &facet.target(),
            "after failed redefine in the live process",
        );
    }

    let restarted = FacetProcess::start(&binary, typedb.http_origin(), workspace.path())
        .expect("restart Facet after failed redefine");
    let select = run_psql(
        &psql,
        &restarted.target(),
        "SELECT id, name FROM people ORDER BY id",
    )
    .expect("read prior generation after restart");
    assert!(
        select.status.success()
            && String::from_utf8_lossy(&select.stdout)
                .lines()
                .eq(["1|Alice", "2|Bob"]),
        "failed redefine did not preserve the prior durable generation.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&select.stdout),
        String::from_utf8_lossy(&select.stderr)
    );
}

fn assert_people_rows(psql: &std::ffi::OsStr, target: &PgTarget, point: &str) {
    let select = run_psql(psql, target, "SELECT id, name FROM people ORDER BY id")
        .expect("query people projection");
    assert_psql_success(&select, point);
    assert_eq!(
        String::from_utf8_lossy(&select.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["1|Alice", "2|Bob"],
        "unexpected people rows {point}"
    );
}

fn spawn_psql(
    psql: OsString,
    target: PgTarget,
    sql: &'static str,
) -> thread::JoinHandle<std::process::Output> {
    thread::spawn(move || {
        run_psql(&psql, &target, sql)
            .unwrap_or_else(|error| panic!("could not run psql for {sql}: {error}"))
    })
}
