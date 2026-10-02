/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 5: a Facet listener reachable off loopback must require TLS and
//! SCRAM, proven with real psql/libpq against the real Facet process.

use std::{
    ffi::OsStr,
    net::{Ipv4Addr, SocketAddr},
    process::Output,
};

use facet_e2e::{
    DEFINE_PROJECTION, FacetOptions, FacetProcess, PgTarget, TlsMaterial, TypeDbProcess,
    assert_psql_success, facet_binary, psql_binary, run_psql_env,
};

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn verify_full(root: &OsStr) -> [(&'static str, &OsStr); 2] {
    [
        ("PGSSLMODE", OsStr::new("verify-full")),
        ("PGSSLROOTCERT", root),
    ]
}

#[test]
fn non_loopback_listener_without_tls_is_rejected_at_startup() {
    let workspace = tempfile::tempdir().expect("create Facet E2E workspace");
    let result = FacetProcess::start_with_options(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable"),
        "http://127.0.0.1:9",
        workspace.path(),
        &FacetOptions {
            listen: "0.0.0.0:0".to_owned(),
            ..FacetOptions::default()
        },
    );
    let error = match result {
        Ok(_) => panic!("Facet served plaintext on a non-loopback listener"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("non-loopback PostgreSQL listener 0.0.0.0:0 requires [postgres.tls]"),
        "Facet must refuse a non-loopback listener without TLS; got: {error}"
    );
}

#[test]
fn tls_listener_with_missing_certificate_is_rejected_at_startup() {
    let workspace = tempfile::tempdir().expect("create Facet E2E workspace");
    let missing = workspace.path().join("absent-server.crt");
    let key = workspace.path().join("absent-server.key");
    let result = FacetProcess::start_with_options(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable"),
        "http://127.0.0.1:9",
        workspace.path(),
        &FacetOptions {
            listen: "0.0.0.0:0".to_owned(),
            tls: Some((missing.clone(), key)),
            ..FacetOptions::default()
        },
    );
    let error = match result {
        Ok(_) => panic!("Facet started with a TLS certificate that does not exist"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("could not load TLS certificate")
            && error.contains(&missing.display().to_string()),
        "Facet must name the unreadable certificate; got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_loopback_listener_requires_verified_tls_and_scram() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and data must enter real TypeDB through HTTP v1");

    let workspace = tempfile::tempdir().expect("create Facet E2E workspace");
    let material = TlsMaterial::generate(workspace.path(), "facet")
        .expect("generate a CA and a localhost server certificate");
    let impostor =
        TlsMaterial::generate(workspace.path(), "impostor").expect("generate an unrelated CA");

    let facet = FacetProcess::start_with_options(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable"),
        typedb.http_origin(),
        workspace.path(),
        &FacetOptions {
            listen: "0.0.0.0:0".to_owned(),
            tls: Some((
                material.server_certificate.clone(),
                material.server_private_key.clone(),
            )),
            ..FacetOptions::default()
        },
    )
    .expect("Facet must start a TLS listener on all interfaces");
    assert!(
        facet.postgres_address().ip().is_unspecified(),
        "the listener under test must be bound off loopback, got {}",
        facet.postgres_address()
    );
    let target: PgTarget = facet.target().through(SocketAddr::from((
        Ipv4Addr::LOCALHOST,
        facet.postgres_address().port(),
    )));
    let psql = psql_binary();
    let trusted = verify_full(material.ca_certificate.as_os_str());

    let defined = run_psql_env(&psql, &target, DEFINE_PROJECTION, &trusted)
        .expect("run DEFINE PROJECTION over verified TLS");
    assert_psql_success(&defined, "DEFINE PROJECTION over verify-full TLS");
    let selected = run_psql_env(
        &psql,
        &target,
        "SELECT id, name FROM people ORDER BY id",
        &trusted,
    )
    .expect("run SELECT over verified TLS");
    assert_psql_success(&selected, "SELECT over verify-full TLS");
    assert_eq!(
        String::from_utf8_lossy(&selected.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["1|Alice", "2|Bob"],
        "rows served over TLS must match TypeDB"
    );

    let plaintext = run_psql_env(
        &psql,
        &target,
        "SELECT id, name FROM people ORDER BY id",
        &[("PGSSLMODE", OsStr::new("disable"))],
    )
    .expect("run psql without TLS");
    assert!(
        !plaintext.status.success() && plaintext.stdout.is_empty(),
        "a plaintext session was served on a TLS listener: {}",
        String::from_utf8_lossy(&plaintext.stdout)
    );
    assert!(
        stderr(&plaintext).contains("Facet requires TLS on this listener"),
        "plaintext refusal must be explicit; got: {}",
        stderr(&plaintext)
    );

    let untrusted = run_psql_env(
        &psql,
        &target,
        "SELECT 1",
        &verify_full(impostor.ca_certificate.as_os_str()),
    )
    .expect("run psql trusting an unrelated CA");
    assert!(
        !untrusted.status.success(),
        "libpq accepted Facet's certificate under an unrelated CA"
    );
    assert!(
        stderr(&untrusted).contains("certificate verify failed"),
        "the failure must be certificate verification; got: {}",
        stderr(&untrusted)
    );

    let wrong_password = PgTarget {
        password: "not-the-facet-password".to_owned(),
        ..target.clone()
    };
    let rejected = run_psql_env(&psql, &wrong_password, "SELECT 1", &trusted)
        .expect("run psql with a wrong password over TLS");
    assert!(
        !rejected.status.success() && rejected.stdout.is_empty(),
        "a wrong password was accepted over TLS"
    );
    assert!(
        stderr(&rejected).contains(&format!(
            "FATAL:  Password authentication failed for user \"{}\"",
            target.user
        )),
        "SCRAM must reject the wrong password over TLS; got: {}",
        stderr(&rejected)
    );
}
