/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stage 2 RED tests for typed TypeDB-to-PostgreSQL values.

use std::collections::HashMap;

use facet_e2e::{
    BackendFrame, FacetProcess, PgTarget, PostgresProcess, RecordingProxy, TypeDbProcess,
    facet_binary, psql_binary, run_psql,
};
use serde_json::Value;

const SCALAR_SCHEMA: &str = r#"
define
  attribute scalar-boolean value boolean;
  attribute scalar-integer value integer;
  attribute scalar-double value double;
  attribute scalar-decimal value decimal;
  attribute scalar-date value date;
  attribute scalar-datetime value datetime;
  attribute scalar-fixed-tz value datetime-tz;
  attribute scalar-iana-tz value datetime-tz;
  attribute scalar-duration value duration;
  attribute scalar-string value string;
  entity scalar-row
    owns scalar-boolean,
    owns scalar-integer,
    owns scalar-double,
    owns scalar-decimal,
    owns scalar-date,
    owns scalar-datetime,
    owns scalar-fixed-tz,
    owns scalar-iana-tz,
    owns scalar-duration,
    owns scalar-string;
"#;

const SCALAR_INSERT: &str = r#"
insert
  $row isa scalar-row,
    has scalar-boolean true,
    has scalar-integer 42,
    has scalar-double 3.125,
    has scalar-decimal 123.00456dec,
    has scalar-date 2024-01-15,
    has scalar-datetime 2024-01-15T10:30:00.123456000,
    has scalar-fixed-tz 2024-01-15T10:30:00.123456000+05:30,
    has scalar-iana-tz 2024-07-15T10:30:00.123456000 America/New_York,
    has scalar-duration P1Y2M3DT4H5M6.123456S,
    has scalar-string "Facet text";
"#;

const SCALAR_FETCH: &str = r#"
match $row isa scalar-row;
fetch {
  "boolean": $row.scalar-boolean,
  "integer": $row.scalar-integer,
  "double": $row.scalar-double,
  "decimal": $row.scalar-decimal,
  "date": $row.scalar-date,
  "datetime": $row.scalar-datetime,
  "fixed_tz": $row.scalar-fixed-tz,
  "iana_tz": $row.scalar-iana-tz,
  "duration": $row.scalar-duration,
  "string": $row.scalar-string
};
"#;

const PRECISION_SCHEMA: &str = r#"
define
  attribute precise-datetime value datetime;
  attribute precise-tz value datetime-tz;
  attribute precise-duration value duration;
  attribute ambiguous-tz value datetime-tz;
  attribute nonfinite-double value double;
  entity precision-row
    owns precise-datetime,
    owns precise-tz,
    owns precise-duration,
    owns ambiguous-tz,
    owns nonfinite-double;
"#;

const PRECISION_INSERT: &str = r#"
insert
  $row isa precision-row,
    has precise-datetime 2024-01-15T10:30:00.123456789,
    has precise-tz 2024-01-15T10:30:00.123456789+05:30,
    has precise-duration PT0.123456789S;
"#;

const PRECISION_FETCH: &str = r#"
match $row isa precision-row;
fetch {
  "datetime": $row.precise-datetime,
  "datetime_tz": $row.precise-tz,
  "duration": $row.precise-duration
};
"#;

const STRUCT_SCHEMA: &str = r#"
define
  struct facet-payload: text value string;
  attribute struct-value value facet-payload;
  entity struct-row owns struct-value;
"#;

const STRUCT_INSERT: &str = r#"
insert
  $payload isa struct-value { text: "hello" };
  $row isa struct-row, has $payload;
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typedb_scalars_have_postgres_oids_and_valid_text() {
    let (typedb, facet, proxy) = scalar_topology()
        .await
        .expect("real scalar topology must start");
    let target = facet.target().through(proxy.address());
    let psql = psql_binary();
    let mut failures = Vec::new();

    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("boolean", "scalar-boolean", "boolean", 16, "t"),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("integer", "scalar-integer", "integer", 20, "42"),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("double", "scalar-double", "double", 701, "3.125"),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("decimal", "scalar-decimal", "decimal", 1700, "123.00456"),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("date", "scalar-date", "date", 1082, "2024-01-15"),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "datetime",
            "scalar-datetime",
            "datetime",
            1114,
            "2024-01-15 10:30:00.123456",
        ),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "fixed_tz",
            "scalar-fixed-tz",
            "datetime-tz",
            1184,
            "2024-01-15 10:30:00.123456+05:30",
        ),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "iana_tz",
            "scalar-iana-tz",
            "datetime-tz",
            1184,
            "2024-07-15 10:30:00.123456-04",
        ),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "duration",
            "scalar-duration",
            "duration",
            1186,
            "1 year 2 mons 3 days 04:05:06.123456",
        ),
        &mut failures,
    );
    verify_scalar_wire(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("string", "scalar-string", "string", 25, "Facet text"),
        &mut failures,
    );

    drop(typedb);
    assert!(
        failures.is_empty(),
        "typed scalar wire mismatches:\n{}",
        failures.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facet_scalar_text_is_accepted_by_postgresql() {
    let (typedb, facet, proxy) = scalar_topology()
        .await
        .expect("real scalar topology must start");
    let postgres = PostgresProcess::start()
        .await
        .expect("real PostgreSQL must start");
    let target = facet.target().through(proxy.address());
    let psql = psql_binary();
    let mut failures = Vec::new();
    let mut postgres_casts = Vec::new();

    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("boolean", "scalar-boolean", "boolean", 16, "t"),
        "boolean",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("integer", "scalar-integer", "integer", 20, "42"),
        "bigint",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("double", "scalar-double", "double", 701, "3.125"),
        "double precision",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("decimal", "scalar-decimal", "decimal", 1700, "123.00456"),
        "numeric",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("date", "scalar-date", "date", 1082, "2024-01-15"),
        "date",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "datetime",
            "scalar-datetime",
            "datetime",
            1114,
            "2024-01-15 10:30:00.123456",
        ),
        "timestamp",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "fixed_tz",
            "scalar-fixed-tz",
            "datetime-tz",
            1184,
            "2024-01-15 10:30:00.123456+05:30",
        ),
        "timestamptz",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "iana_tz",
            "scalar-iana-tz",
            "datetime-tz",
            1184,
            "2024-07-15 10:30:00.123456-04",
        ),
        "timestamptz",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new(
            "duration",
            "scalar-duration",
            "duration",
            1186,
            "1 year 2 mons 3 days 04:05:06.123456",
        ),
        "interval",
        &mut postgres_casts,
        &mut failures,
    );
    verify_postgres_acceptance(
        &psql,
        &target,
        &proxy,
        ScalarExpectation::new("string", "scalar-string", "string", 25, "Facet text"),
        "text",
        &mut postgres_casts,
        &mut failures,
    );

    if !postgres_casts.is_empty() {
        let cast = run_psql(
            &psql,
            &postgres.target(),
            &format!("SELECT {}", postgres_casts.join(", ")),
        )
        .expect("cast every Facet scalar in one real PostgreSQL session");
        let expected = std::iter::repeat_n("t", postgres_casts.len())
            .collect::<Vec<_>>()
            .join("|");
        if !cast.status.success()
            || String::from_utf8_lossy(&cast.stdout)
                .lines()
                .next()
                .map(str::trim)
                != Some(expected.as_str())
        {
            failures.push(format!(
                "PostgreSQL rejected the Facet scalar batch.\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&cast.stdout),
                String::from_utf8_lossy(&cast.stderr)
            ));
        }
    }

    drop(typedb);
    assert!(
        failures.is_empty(),
        "PostgreSQL rejected or never received Facet scalar text:\n{}",
        failures.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrepresentable_temporal_or_interval_value_is_rejected() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .create_database()
        .await
        .expect("create real TypeDB database");
    typedb
        .execute_typeql("schema", PRECISION_SCHEMA, true)
        .await
        .expect("send precision schema")
        .expect_success("define precision schema")
        .expect("real TypeDB must accept precision schema");
    typedb
        .execute_typeql("write", PRECISION_INSERT, true)
        .await
        .expect("send precision values")
        .expect_success("insert precision values")
        .expect("real TypeDB must store nanosecond values");

    let precision_body = typedb
        .execute_typeql("read", PRECISION_FETCH, false)
        .await
        .expect("fetch precision values")
        .expect_success("fetch precision values")
        .expect("real TypeDB must return precision values");
    let precision_answer = first_answer(&precision_body);
    assert_eq!(
        http_cell(precision_answer, "datetime"),
        &Value::String("2024-01-15T10:30:00.123456789".to_owned())
    );
    assert_eq!(
        http_cell(precision_answer, "datetime_tz"),
        &Value::String("2024-01-15T10:30:00.123456789+05:30".to_owned())
    );
    assert_eq!(
        http_cell(precision_answer, "duration"),
        &Value::String("PT0.123456789S".to_owned())
    );
    eprintln!("TypeDB precision HTTP answer: {precision_body}");

    let ambiguous = typedb
        .execute_typeql(
            "write",
            "insert $row isa precision-row, has ambiguous-tz \
             2024-11-03T01:30:00 America/New_York;",
            true,
        )
        .await
        .expect("send DST-ambiguous value");
    assert!(
        !ambiguous.status.is_success(),
        "TypeDB unexpectedly stored DST-ambiguous local time: {}",
        ambiguous.body
    );
    assert!(
        ambiguous.body.to_ascii_lowercase().contains("ambiguous"),
        "TypeDB DST rejection did not identify ambiguity: {}",
        ambiguous.body
    );
    eprintln!(
        "TypeDB DST ambiguity response: {} {}",
        ambiguous.status, ambiguous.body
    );

    assert_typeql_rejected(
        &typedb,
        "insert $row isa precision-row, has nonfinite-double NaN;",
        "NaN",
    )
    .await;
    assert_typeql_rejected(
        &typedb,
        "insert $row isa precision-row, has nonfinite-double Infinity;",
        "Infinity",
    )
    .await;
    assert_typeql_rejected(
        &typedb,
        "insert $row isa precision-row, has nonfinite-double -Infinity;",
        "-Infinity",
    )
    .await;

    let workspace = tempfile::tempdir().expect("create precision Facet workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("Facet binary under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("start real Facet");
    let proxy = RecordingProxy::start(facet.postgres_address()).expect("start Facet recorder");
    let target = facet.target().through(proxy.address());
    let psql = psql_binary();
    let mut failures = Vec::new();

    expect_materialization_rejection(
        &psql,
        &target,
        &proxy,
        "define projection precise_datetime(value: datetime) as \
         'match $r isa precision-row; fetch { \"value\": $r.precise-datetime };';",
        "22008",
        &mut failures,
    );
    expect_materialization_rejection(
        &psql,
        &target,
        &proxy,
        "define projection precise_tz(value: datetime-tz) as \
         'match $r isa precision-row; fetch { \"value\": $r.precise-tz };';",
        "22008",
        &mut failures,
    );
    expect_materialization_rejection(
        &psql,
        &target,
        &proxy,
        "define projection precise_duration(value: duration) as \
         'match $r isa precision-row; fetch { \"value\": $r.precise-duration };';",
        "22015",
        &mut failures,
    );

    assert!(
        failures.is_empty(),
        "unrepresentable values were not rejected precisely:\n{}",
        failures.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn struct_projection_is_rejected_explicitly() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .create_database()
        .await
        .expect("create real TypeDB database");
    typedb
        .execute_typeql("schema", STRUCT_SCHEMA, true)
        .await
        .expect("send struct schema")
        .expect_success("define struct schema")
        .expect("real TypeDB must accept struct schema");
    let struct_insert = typedb
        .execute_typeql("write", STRUCT_INSERT, true)
        .await
        .expect("send struct value");
    assert!(
        !struct_insert.status.is_success(),
        "TypeDB unexpectedly stored a struct value: {}",
        struct_insert.body
    );
    assert!(
        struct_insert.body.contains("REP254") && struct_insert.body.contains("Structs"),
        "TypeDB struct rejection changed: {}",
        struct_insert.body
    );
    eprintln!(
        "TypeDB accepted the struct schema but rejected struct storage with {}: {}",
        struct_insert.status, struct_insert.body
    );

    let workspace = tempfile::tempdir().expect("create struct Facet workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("Facet binary under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("start real Facet");
    let proxy = RecordingProxy::start(facet.postgres_address()).expect("start Facet recorder");
    let target = facet.target().through(proxy.address());
    let define = run_psql(
        &psql_binary(),
        &target,
        "define projection struct_payload(value: struct) as \
         'match $r isa struct-row; fetch { \"value\": $r.struct-value };';",
    )
    .expect("submit struct projection DDL through real psql");
    assert!(
        !define.status.success(),
        "struct projection unexpectedly succeeded"
    );
    let frames = proxy.drain_backend_frames();
    let fields = error_fields(&first_frame(&frames, b'E').payload);
    assert_eq!(
        fields.get(&b'C').map(String::as_str),
        Some("0A000"),
        "struct rejection must use feature-not-supported SQLSTATE"
    );
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some("struct projection columns are not supported"),
        "struct rejection must be explicit and stable"
    );
}

struct ScalarExpectation<'a> {
    projection: &'a str,
    attribute: &'a str,
    type_name: &'a str,
    oid: u32,
    text: &'a str,
}

impl<'a> ScalarExpectation<'a> {
    fn new(
        projection: &'a str,
        attribute: &'a str,
        type_name: &'a str,
        oid: u32,
        text: &'a str,
    ) -> Self {
        Self {
            projection,
            attribute,
            type_name,
            oid,
            text,
        }
    }

    fn ddl(&self) -> String {
        format!(
            "define projection scalar_{}(value: {}) as \
             'match $r isa scalar-row; fetch {{ \"value\": $r.{} }};';",
            self.projection, self.type_name, self.attribute
        )
    }

    fn select(&self) -> String {
        format!("SELECT value FROM scalar_{}", self.projection)
    }
}

async fn scalar_topology() -> facet_e2e::TestResult<(TypeDbProcess, FacetProcess, RecordingProxy)> {
    let typedb = TypeDbProcess::start().await?;
    typedb.create_database().await?;
    typedb
        .execute_typeql("schema", SCALAR_SCHEMA, true)
        .await?
        .expect_success("define scalar schema")?;
    typedb
        .execute_typeql("write", SCALAR_INSERT, true)
        .await?
        .expect_success("insert scalar row")?;
    let body = typedb
        .execute_typeql("read", SCALAR_FETCH, false)
        .await?
        .expect_success("fetch scalar row")?;
    assert_scalar_http_answer(&body);
    eprintln!("TypeDB scalar HTTP answer: {body}");

    let workspace = tempfile::tempdir()?;
    let workspace_path = workspace.keep();
    let facet = FacetProcess::start(&facet_binary()?, typedb.http_origin(), &workspace_path)?;
    let proxy = RecordingProxy::start(facet.postgres_address())?;
    Ok((typedb, facet, proxy))
}

fn assert_scalar_http_answer(body: &str) {
    let answer = first_answer(body);
    assert_eq!(http_cell(answer, "boolean"), &Value::Bool(true));
    assert_eq!(http_cell(answer, "integer"), &Value::from(42));
    assert_eq!(http_cell(answer, "double"), &Value::from(3.125));
    assert_eq!(
        http_cell(answer, "decimal"),
        &Value::String("123.00456dec".to_owned())
    );
    assert_eq!(
        http_cell(answer, "date"),
        &Value::String("2024-01-15".to_owned())
    );
    assert_eq!(
        http_cell(answer, "datetime"),
        &Value::String("2024-01-15T10:30:00.123456000".to_owned())
    );
    assert_eq!(
        http_cell(answer, "fixed_tz"),
        &Value::String("2024-01-15T10:30:00.123456000+05:30".to_owned())
    );
    assert_eq!(
        http_cell(answer, "iana_tz"),
        &Value::String("2024-07-15T10:30:00.123456000 America/New_York".to_owned())
    );
    assert_eq!(
        http_cell(answer, "duration"),
        &Value::String("P1Y2M3DT4H5M6.123456000S".to_owned())
    );
    assert_eq!(
        http_cell(answer, "string"),
        &Value::String("Facet text".to_owned())
    );
}

fn first_answer(body: &str) -> &serde_json::Map<String, Value> {
    let response: Value = serde_json::from_str(body).expect("valid TypeDB JSON response");
    let answer = response
        .get("answers")
        .and_then(Value::as_array)
        .and_then(|answers| answers.first())
        .and_then(Value::as_object)
        .expect("one TypeDB answer object");
    Box::leak(Box::new(answer.clone()))
}

fn http_cell<'a>(answer: &'a serde_json::Map<String, Value>, key: &str) -> &'a Value {
    let value = answer
        .get(key)
        .unwrap_or_else(|| panic!("missing TypeDB fetch key {key}"));
    match value {
        Value::Array(values) if values.len() == 1 => &values[0],
        Value::Object(object) if object.contains_key("value") => &object["value"],
        _ => value,
    }
}

fn verify_scalar_wire(
    psql: &std::ffi::OsStr,
    target: &PgTarget,
    proxy: &RecordingProxy,
    expectation: ScalarExpectation<'_>,
    failures: &mut Vec<String>,
) {
    let define = run_psql(psql, target, &expectation.ddl()).expect("submit scalar projection DDL");
    let define_frames = proxy.drain_backend_frames();
    if !define.status.success() {
        failures.push(format!(
            "{} DEFINE failed: {} | wire error: {}",
            expectation.projection,
            String::from_utf8_lossy(&define.stderr).trim(),
            describe_error(&define_frames)
        ));
        return;
    }

    let select = run_psql(psql, target, &expectation.select()).expect("query scalar projection");
    let frames = proxy.drain_backend_frames();
    if !select.status.success() {
        failures.push(format!(
            "{} SELECT failed: {}",
            expectation.projection,
            String::from_utf8_lossy(&select.stderr).trim()
        ));
        return;
    }

    let row_description = parse_row_description(&first_frame(&frames, b'T').payload);
    let data_row = parse_data_row(&first_frame(&frames, b'D').payload);
    if row_description != vec![("value".to_owned(), expectation.oid)] {
        failures.push(format!(
            "{} RowDescription was {row_description:?}, expected OID {}",
            expectation.projection, expectation.oid
        ));
    }
    if data_row != vec![Some(expectation.text.to_owned())] {
        failures.push(format!(
            "{} DataRow was {data_row:?}, expected {:?}",
            expectation.projection, expectation.text
        ));
    }
}

fn verify_postgres_acceptance(
    psql: &std::ffi::OsStr,
    facet_target: &PgTarget,
    proxy: &RecordingProxy,
    expectation: ScalarExpectation<'_>,
    postgres_type: &str,
    postgres_casts: &mut Vec<String>,
    failures: &mut Vec<String>,
) {
    let define =
        run_psql(psql, facet_target, &expectation.ddl()).expect("submit scalar projection DDL");
    let define_frames = proxy.drain_backend_frames();
    if !define.status.success() {
        failures.push(format!(
            "{} never reached PostgreSQL because DEFINE failed: {}",
            expectation.projection,
            describe_error(&define_frames)
        ));
        return;
    }

    let select =
        run_psql(psql, facet_target, &expectation.select()).expect("query scalar projection");
    let frames = proxy.drain_backend_frames();
    if !select.status.success() {
        failures.push(format!("{} SELECT failed", expectation.projection));
        return;
    }
    let value = parse_data_row(&first_frame(&frames, b'D').payload)
        .into_iter()
        .next()
        .flatten()
        .expect("non-null scalar text");
    let escaped = value.replace('\'', "''");
    postgres_casts.push(format!("'{escaped}'::{postgres_type} IS NOT NULL"));
}

fn expect_materialization_rejection(
    psql: &std::ffi::OsStr,
    target: &PgTarget,
    proxy: &RecordingProxy,
    ddl: &str,
    expected_code: &str,
    failures: &mut Vec<String>,
) {
    let response = run_psql(psql, target, ddl).expect("submit precision projection DDL");
    let frames = proxy.drain_backend_frames();
    if response.status.success() {
        failures.push(format!(
            "unrepresentable projection unexpectedly succeeded: {ddl}"
        ));
        return;
    }
    let fields = error_fields(&first_frame(&frames, b'E').payload);
    if fields.get(&b'C').map(String::as_str) != Some(expected_code)
        || !fields
            .get(&b'M')
            .is_some_and(|message| message.contains("sub-microsecond"))
    {
        failures.push(format!(
            "DDL {ddl} returned SQLSTATE/message {:?}/{:?}, expected {expected_code}/sub-microsecond",
            fields.get(&b'C'),
            fields.get(&b'M')
        ));
    }
}

async fn assert_typeql_rejected(typedb: &TypeDbProcess, query: &str, name: &str) {
    let response = typedb
        .execute_typeql("write", query, true)
        .await
        .expect("send non-finite TypeQL value");
    assert!(
        !response.status.is_success(),
        "TypeDB unexpectedly stored non-finite double {name}: {}",
        response.body
    );
    eprintln!(
        "TypeDB non-finite {name} response: {} {}",
        response.status, response.body
    );
}

fn first_frame(frames: &[BackendFrame], message_type: u8) -> &BackendFrame {
    frames
        .iter()
        .find(|frame| frame.message_type == message_type)
        .unwrap_or_else(|| panic!("missing backend frame {}", message_type as char))
}

fn parse_row_description(payload: &[u8]) -> Vec<(String, u32)> {
    let count = i16::from_be_bytes(payload[0..2].try_into().expect("column count")) as usize;
    let mut position = 2;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let end = payload[position..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| position + offset)
            .expect("column name terminator");
        let name = String::from_utf8(payload[position..end].to_vec()).expect("UTF-8 column name");
        position = end + 1;
        position += 4;
        position += 2;
        let oid = u32::from_be_bytes(
            payload[position..position + 4]
                .try_into()
                .expect("type OID"),
        );
        position += 4;
        position += 2;
        position += 4;
        position += 2;
        columns.push((name, oid));
    }
    columns
}

fn parse_data_row(payload: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes(payload[0..2].try_into().expect("field count")) as usize;
    let mut position = 2;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(
            payload[position..position + 4]
                .try_into()
                .expect("field length"),
        );
        position += 4;
        if length == -1 {
            fields.push(None);
        } else {
            let length = length as usize;
            fields.push(Some(
                String::from_utf8(payload[position..position + length].to_vec())
                    .expect("UTF-8 text field"),
            ));
            position += length;
        }
    }
    fields
}

fn describe_error(frames: &[BackendFrame]) -> String {
    frames
        .iter()
        .find(|frame| frame.message_type == b'E')
        .map(|frame| format!("{:?}", error_fields(&frame.payload)))
        .unwrap_or_else(|| "no ErrorResponse".to_owned())
}

fn error_fields(payload: &[u8]) -> HashMap<u8, String> {
    let mut fields = HashMap::new();
    let mut position = 0;
    while position < payload.len() && payload[position] != 0 {
        let field = payload[position];
        position += 1;
        let end = payload[position..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| position + offset)
            .expect("error field terminator");
        fields.insert(
            field,
            String::from_utf8(payload[position..end].to_vec()).expect("UTF-8 error field"),
        );
        position = end + 1;
    }
    fields
}
