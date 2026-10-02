/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 4 tests for bounded background refresh and TypeDB token expiry.

use std::time::{Duration, Instant};

use facet_e2e::{
    DEFINE_PROJECTION, DelayedHttpProxy, FacetProcess, INSERT_THIRD, PgTarget, TypeDbProcess,
    assert_psql_success, facet_binary, psql_binary, run_psql,
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

fn proxy_for(typedb: &TypeDbProcess) -> DelayedHttpProxy {
    let address = typedb
        .http_origin()
        .strip_prefix("http://")
        .expect("plain loopback TypeDB origin")
        .parse()
        .expect("TypeDB HTTP socket address");
    DelayedHttpProxy::start(address).expect("start counting TypeDB proxy")
}

fn people_rows(psql: &std::ffi::OsStr, target: &PgTarget) -> Vec<String> {
    let select = run_psql(psql, target, "SELECT id, name FROM people ORDER BY id")
        .expect("query people projection");
    assert_psql_success(&select, "SELECT from people");
    String::from_utf8_lossy(&select.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

async fn wait_for_rows(
    psql: &std::ffi::OsStr,
    target: &PgTarget,
    expected: &[&str],
    bound: Duration,
) -> Duration {
    let started = Instant::now();
    loop {
        let rows = people_rows(psql, target);
        if rows.iter().map(String::as_str).eq(expected.iter().copied()) {
            return started.elapsed();
        }
        assert!(
            started.elapsed() <= bound,
            "projection did not converge to {expected:?} within {bound:?}; last rows {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_publishes_typedb_writes_within_two_intervals() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let proxy = proxy_for(&typedb);
    let workspace = tempfile::tempdir().expect("create Facet refresh workspace");
    let facet = FacetProcess::start_with_refresh(
        &facet_binary().expect("Facet binary under test"),
        &proxy.origin(),
        workspace.path(),
        "5s",
    )
    .expect("start Facet with 5s refresh");
    let psql = psql_binary();
    let target = facet.target();

    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");
    assert_eq!(people_rows(&psql, &target), ["1|Alice", "2|Bob"]);

    typedb
        .execute_typeql("write", INSERT_THIRD, true)
        .await
        .expect("send third row to TypeDB")
        .expect_success("insert third row")
        .expect("third row committed");

    let converged = wait_for_rows(
        &psql,
        &target,
        &["1|Alice", "2|Bob", "3|Carol"],
        REFRESH_INTERVAL * 2,
    )
    .await;
    eprintln!("refresh published TypeDB write after {converged:?}");

    tokio::time::sleep(REFRESH_INTERVAL * 2).await;
    let queries = proxy.query_requests();
    let signins = proxy.signin_requests();
    assert!(
        queries >= 3,
        "background refresh issued only {queries} TypeDB queries"
    );
    assert_eq!(
        signins, 1,
        "Facet must reuse its TypeDB token across {queries} queries, signed in {signins} times"
    );
    assert_eq!(proxy.unauthorized_responses(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_refresh_keeps_serving_previous_rows() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let proxy = proxy_for(&typedb);
    let workspace = tempfile::tempdir().expect("create Facet outage workspace");
    let facet = FacetProcess::start_with_refresh(
        &facet_binary().expect("Facet binary under test"),
        &proxy.origin(),
        workspace.path(),
        "5s",
    )
    .expect("start Facet with 5s refresh");
    let psql = psql_binary();
    let target = facet.target();

    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");
    assert_eq!(people_rows(&psql, &target), ["1|Alice", "2|Bob"]);

    drop(typedb);
    let connections_before_outage = proxy.accepted_connections();
    tokio::time::sleep(REFRESH_INTERVAL * 2 + Duration::from_secs(1)).await;
    let refresh_attempts = proxy.accepted_connections() - connections_before_outage;
    assert!(
        refresh_attempts >= 1,
        "no refresh reached TypeDB's address during the outage; the failure path was not exercised"
    );
    assert_eq!(
        people_rows(&psql, &target),
        ["1|Alice", "2|Bob"],
        "a failed refresh must keep the last good generation"
    );
}

#[test]
fn zero_refresh_interval_is_rejected_at_startup() {
    let workspace = tempfile::tempdir().expect("create Facet config workspace");
    let error = match FacetProcess::start_with_refresh(
        &facet_binary().expect("Facet binary under test"),
        "http://127.0.0.1:9",
        workspace.path(),
        "0s",
    ) {
        Ok(_) => panic!("Facet accepted a zero refresh interval"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("invalid refresh interval \"0s\""),
        "unexpected startup failure: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_reauthenticates_after_typedb_token_expiry() {
    let typedb = TypeDbProcess::start_with_token_expiration_seconds(2)
        .await
        .expect("the real TypeDB server must start with 2s tokens");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let proxy = proxy_for(&typedb);
    let workspace = tempfile::tempdir().expect("create Facet token workspace");
    let facet = FacetProcess::start_with_refresh(
        &facet_binary().expect("Facet binary under test"),
        &proxy.origin(),
        workspace.path(),
        "5s",
    )
    .expect("start Facet with 5s refresh");
    let psql = psql_binary();
    let target = facet.target();

    let define = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define projection");
    assert_psql_success(&define, "DEFINE PROJECTION");
    assert_eq!(people_rows(&psql, &target), ["1|Alice", "2|Bob"]);

    tokio::time::sleep(Duration::from_secs(3)).await;
    typedb
        .execute_typeql("write", INSERT_THIRD, true)
        .await
        .expect("send third row to TypeDB")
        .expect_success("insert third row")
        .expect("third row committed");

    let converged = wait_for_rows(
        &psql,
        &target,
        &["1|Alice", "2|Bob", "3|Carol"],
        REFRESH_INTERVAL * 2,
    )
    .await;
    eprintln!("refresh converged after token expiry in {converged:?}");

    let unauthorized = proxy.unauthorized_responses();
    let signins = proxy.signin_requests();
    assert!(
        unauthorized >= 1,
        "the expired TypeDB token was never presented; the 401 path was not exercised"
    );
    assert!(
        signins >= 2,
        "Facet did not re-authenticate after {unauthorized} HTTP 401 responses"
    );
}
