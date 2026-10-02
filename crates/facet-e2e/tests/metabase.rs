/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 9: real Metabase reads TypeDB projections through Facet's secured BI
//! preset: an off-loopback listener that requires verify-full TLS and
//! SCRAM-SHA-256. Proven against real TypeDB, real Facet and the real
//! open-source Metabase JAR with its bundled PostgreSQL JDBC driver.

use std::{
    ffi::OsStr,
    fs::File,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use facet_e2e::{
    DEFINE_PROJECTION, FacetOptions, FacetProcess, PgTarget, TlsMaterial, TypeDbProcess,
    assert_psql_success, facet_binary, java_binary, metabase_jar, psql_binary, run_psql_env,
};
use reqwest::Client;
use serde_json::{Value, json};

const ADMIN_EMAIL: &str = "facet-e2e@example.com";
const ADMIN_PASSWORD: &str = "Facet-e2e-metabase-1";

struct Metabase {
    child: Child,
    origin: String,
    log: std::path::PathBuf,
}

impl Metabase {
    async fn start(workspace: &Path) -> Self {
        let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("reserve a Metabase port")
            .local_addr()
            .expect("read the reserved port")
            .port();
        let log = workspace.join("metabase.log");
        let output = File::create(&log).expect("create Metabase log");
        let child = Command::new(java_binary().expect("FACET_E2E_JAVA"))
            .arg("-jar")
            .arg(metabase_jar().expect("FACET_E2E_METABASE_JAR"))
            .current_dir(workspace)
            .env("MB_JETTY_HOST", "127.0.0.1")
            .env("MB_JETTY_PORT", port.to_string())
            .env("MB_DB_TYPE", "h2")
            .env("MB_DB_FILE", workspace.join("metabase-app"))
            .env("MB_PLUGINS_DIR", workspace.join("metabase-plugins"))
            .env("MB_CHECK_FOR_UPDATES", "false")
            .env("MB_ANON_TRACKING_ENABLED", "false")
            .stdin(Stdio::null())
            .stdout(output.try_clone().expect("share Metabase log"))
            .stderr(output)
            .spawn()
            .expect("spawn Metabase");
        let mut metabase = Self {
            child,
            origin: format!("http://127.0.0.1:{port}"),
            log,
        };
        let client = Client::new();
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            if let Some(status) = metabase.child.try_wait().expect("poll Metabase") {
                panic!("Metabase exited with {status}:\n{}", metabase.log_text());
            }
            if let Ok(response) = client
                .get(format!("{}/api/health", metabase.origin))
                .send()
                .await
                && let Ok(body) = response.json::<Value>().await
                && body["status"] == "ok"
            {
                return metabase;
            }
            assert!(
                Instant::now() < deadline,
                "Metabase was not healthy within 600 seconds:\n{}",
                metabase.log_text()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Metabase {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Api {
    client: Client,
    origin: String,
    session: String,
}

impl Api {
    async fn setup(metabase: &Metabase) -> Self {
        let client = Client::new();
        let properties: Value = client
            .get(format!("{}/api/session/properties", metabase.origin))
            .send()
            .await
            .expect("read Metabase properties")
            .json()
            .await
            .expect("parse Metabase properties");
        let token = properties["setup-token"]
            .as_str()
            .expect("a fresh Metabase exposes its setup token")
            .to_owned();
        let response = client
            .post(format!("{}/api/setup", metabase.origin))
            .json(&json!({
                "token": token,
                "user": {
                    "first_name": "Facet",
                    "last_name": "E2E",
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                    "site_name": "Facet E2E",
                },
                "prefs": {"site_name": "Facet E2E", "site_locale": "en", "allow_tracking": false},
            }))
            .send()
            .await
            .expect("complete Metabase setup");
        let status = response.status();
        let body: Value = response.json().await.expect("parse Metabase setup");
        assert!(
            status.is_success(),
            "Metabase setup failed: {status} {body}"
        );
        Self {
            client,
            origin: metabase.origin.clone(),
            session: body["id"]
                .as_str()
                .expect("setup returns a session")
                .to_owned(),
        }
    }

    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.origin))
            .header("X-Metabase-Session", &self.session);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("call the Metabase API");
        let status = response.status().as_u16();
        let text = response.text().await.expect("read Metabase response");
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn add_database(&self, details: Value) -> (u16, Value) {
        self.call(
            reqwest::Method::POST,
            "/api/database",
            Some(json!({"engine": "postgres", "name": "Facet", "details": details})),
        )
        .await
    }

    async fn dataset(&self, query: Value) -> Value {
        let (status, body) = self
            .call(reqwest::Method::POST, "/api/dataset", Some(query.clone()))
            .await;
        assert!(
            (200..300).contains(&status) && body["status"] == "completed",
            "Metabase query {query} failed: {status} {body}"
        );
        body["data"]["rows"].clone()
    }
}

fn details(target: &PgTarget, password: &str, ssl: Value) -> Value {
    let mut details = json!({
        "host": "localhost",
        "port": target.address.port(),
        "dbname": target.database,
        "user": target.user,
        "password": password,
        "schema-filters-type": "all",
    });
    for (key, value) in ssl.as_object().expect("ssl options are an object") {
        details[key] = value.clone();
    }
    details
}

fn verify_full(ca: &Path) -> Value {
    json!({
        "ssl": true,
        "ssl-mode": "verify-full",
        "ssl-use-client-auth": false,
        "ssl-root-cert-options": "local",
        "ssl-root-cert-path": ca.display().to_string(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metabase_reads_projections_over_verified_tls_and_scram() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_two()
        .await
        .expect("schema and rows must enter real TypeDB");
    let workspace = tempfile::tempdir().expect("create Facet workspace");
    let material = TlsMaterial::generate(workspace.path(), "facet")
        .expect("generate a CA and a localhost server certificate");
    let facet = FacetProcess::start_with_options(
        &facet_binary().expect("Facet binary under test"),
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
    .expect("start Facet with the secured BI preset");
    let target = facet.target().through(SocketAddr::from((
        Ipv4Addr::LOCALHOST,
        facet.postgres_address().port(),
    )));
    let define = run_psql_env(
        &psql_binary(),
        &target,
        DEFINE_PROJECTION,
        &[
            ("PGSSLMODE", OsStr::new("verify-full")),
            ("PGSSLROOTCERT", material.ca_certificate.as_os_str()),
        ],
    )
    .expect("define projection over verified TLS");
    assert_psql_success(&define, "DEFINE PROJECTION");

    let metabase = Metabase::start(workspace.path()).await;
    let api = Api::setup(&metabase).await;

    // Facet refuses the plaintext startup; Metabase then retries the same
    // details with `ssl: true` and saves the TLS connection it was forced into.
    let (status, body) = api
        .add_database(details(&target, &target.password, json!({"ssl": false})))
        .await;
    assert!(
        (200..300).contains(&status) && body["details"]["ssl"] == json!(true),
        "Metabase must only reach Facet after upgrading plaintext to TLS: {status} {body}"
    );
    let upgraded = body["id"].as_i64().expect("saved database has an id");
    let (status, body) = api
        .call(
            reqwest::Method::DELETE,
            &format!("/api/database/{upgraded}"),
            None,
        )
        .await;
    assert!(
        (200..300).contains(&status),
        "remove the upgraded probe database: {status} {body}"
    );
    let (status, body) = api
        .add_database(details(
            &target,
            "not-the-facet-password",
            verify_full(&material.ca_certificate),
        ))
        .await;
    assert!(
        status >= 400
            && body
                .to_string()
                .to_lowercase()
                .contains("password authentication failed"),
        "SCRAM must reject a wrong password from Metabase: {status} {body}"
    );

    let (status, database) = api
        .add_database(details(
            &target,
            &target.password,
            verify_full(&material.ca_certificate),
        ))
        .await;
    assert!(
        (200..300).contains(&status),
        "Metabase must connect over verify-full TLS and SCRAM: {status} {database}\n{}",
        metabase.log_text()
    );
    let database_id = database["id"].as_i64().expect("database id");

    let deadline = Instant::now() + Duration::from_secs(300);
    let (table_id, fields) = loop {
        let (_, metadata) = api
            .call(
                reqwest::Method::GET,
                &format!("/api/database/{database_id}/metadata"),
                None,
            )
            .await;
        let people = metadata["tables"].as_array().and_then(|tables| {
            tables
                .iter()
                .find(|table| table["name"] == "people" && table["schema"] == "public")
        });
        if let Some(people) = people
            && people["fields"].as_array().is_some_and(|f| f.len() == 2)
        {
            let mut fields = people["fields"]
                .as_array()
                .expect("fields")
                .iter()
                .map(|field| {
                    (
                        field["name"].as_str().expect("field name").to_owned(),
                        field["base_type"].as_str().expect("base type").to_owned(),
                        field["id"].as_i64().expect("field id"),
                    )
                })
                .collect::<Vec<_>>();
            fields.sort();
            break (people["id"].as_i64().expect("table id"), fields);
        }
        assert!(
            Instant::now() < deadline,
            "Metabase did not sync public.people within 300 seconds: {metadata}\n{}\nFacet stderr:\n{}",
            metabase.log_text(),
            facet.stderr()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert_eq!(
        fields
            .iter()
            .map(|(name, base_type, _)| (name.as_str(), base_type.as_str()))
            .collect::<Vec<_>>(),
        [("id", "type/BigInteger"), ("name", "type/Text")],
        "Metabase must sync the projection's typed columns"
    );
    let id_field = fields[0].2;

    let native = api
        .dataset(json!({
            "database": database_id,
            "type": "native",
            "native": {"query": "SELECT id, name FROM people ORDER BY id"},
        }))
        .await;
    assert_eq!(native, json!([[1, "Alice"], [2, "Bob"]]), "native SQL rows");

    let question = api
        .dataset(json!({
            "database": database_id,
            "type": "query",
            "query": {
                "source-table": table_id,
                "order-by": [["asc", ["field", id_field, null]]],
            },
        }))
        .await;
    assert_eq!(
        question,
        json!([[1, "Alice"], [2, "Bob"]]),
        "GUI question rows"
    );

    let count = api
        .dataset(json!({
            "database": database_id,
            "type": "query",
            "query": {"source-table": table_id, "aggregation": [["count"]]},
        }))
        .await;
    assert_eq!(count, json!([[2]]), "GUI count aggregation");
}
