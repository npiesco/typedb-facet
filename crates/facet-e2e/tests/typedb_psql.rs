/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! RED test for the first standalone Facet vertical slice.

use facet_e2e::{
    DEFINE_PROJECTION, FacetProcess, TypeDbProcess, assert_psql_success, facet_binary, psql_binary,
    run_psql,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typedb_projection_round_trip_through_psql() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and data must enter real TypeDB through HTTP v1");

    let workspace = tempfile::tempdir().expect("create Facet E2E workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("the real Facet process must expose its retained pgwire listener");
    let psql = psql_binary();
    let target = facet.target();

    let missing = run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id")
        .expect("run psql before projection definition");
    assert!(
        !missing.status.success(),
        "an undefined projection unexpectedly returned rows: {}",
        String::from_utf8_lossy(&missing.stdout)
    );

    let defined =
        run_psql(&psql, &target, DEFINE_PROJECTION).expect("run projection DDL through psql");
    assert_psql_success(&defined, "DEFINE PROJECTION");

    let selected = run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id")
        .expect("select projection through psql");
    assert_psql_success(&selected, "projection SELECT");
    let selected_stdout = String::from_utf8_lossy(&selected.stdout);
    assert_eq!(
        selected_stdout.lines().collect::<Vec<_>>(),
        vec!["1|Alice", "2|Bob"],
        "psql must return exactly the rows inserted through TypeDB HTTP"
    );

    let undefined =
        run_psql(&psql, &target, "UNDEFINE PROJECTION people;").expect("run UNDEFINE through psql");
    assert_psql_success(&undefined, "UNDEFINE PROJECTION");

    let gone = run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id")
        .expect("select undefined projection through psql");
    assert!(
        !gone.status.success(),
        "an undefined projection remained queryable: {}",
        String::from_utf8_lossy(&gone.stdout)
    );
}
