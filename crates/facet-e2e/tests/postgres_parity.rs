/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Twelve live PostgreSQL parity cases migrated from the embedded source.
//!
//! Both sides are real servers reached through real psql sessions. Transparent
//! proxies retain and record the bytes each server actually emits.

use std::collections::HashMap;

use facet_e2e::{
    BackendFrame, DEFINE_PROJECTION, FacetProcess, PostgresProcess, RecordingProxy, TypeDbProcess,
    assert_psql_success, facet_binary, psql_binary, run_psql,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twelve_wire_behaviors_match_live_postgres() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_three()
        .await
        .expect("parity rows must be seeded through real TypeDB HTTP");
    let workspace = tempfile::tempdir().expect("create Facet parity workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("the real Facet process must expose pgwire");
    let postgres = PostgresProcess::start()
        .await
        .expect("the real PostgreSQL 16 reference server must start");
    let facet_proxy =
        RecordingProxy::start(facet.postgres_address()).expect("record Facet wire traffic");
    let postgres_proxy =
        RecordingProxy::start(postgres.target().address).expect("record PostgreSQL wire traffic");
    let facet_target = facet.target().through(facet_proxy.address());
    let postgres_target = postgres.target().through(postgres_proxy.address());
    let psql = psql_binary();
    let mut observed = 0_usize;

    let facet_select =
        run_psql(&psql, &facet_target, "SELECT '42' AS answer").expect("query Facet");
    assert_psql_success(&facet_select, "Facet SELECT");
    let postgres_select =
        run_psql(&psql, &postgres_target, "SELECT '42' AS answer").expect("query PostgreSQL");
    assert_psql_success(&postgres_select, "PostgreSQL SELECT");
    let facet_frames = facet_proxy.drain_backend_frames();
    let postgres_frames = postgres_proxy.drain_backend_frames();

    parity_case(
        &mut observed,
        "startup_handshake_message_types_match",
        || {
            assert_handshake_shape(&facet_frames);
            assert_handshake_shape(&postgres_frames);
        },
    );
    parity_case(&mut observed, "auth_ok_bytes_match_postgres", || {
        assert_eq!(auth_ok(&facet_frames).wire, auth_ok(&postgres_frames).wire);
    });
    parity_case(
        &mut observed,
        "parameter_status_structure_matches_postgres",
        || {
            assert_common_parameter_equal(&facet_frames, &postgres_frames, "client_encoding");
            assert_common_parameter_equal(&facet_frames, &postgres_frames, "server_encoding");
            assert_common_parameter_equal(&facet_frames, &postgres_frames, "DateStyle");
            assert_common_parameter_equal(&facet_frames, &postgres_frames, "integer_datetimes");
            assert_common_parameter_equal(
                &facet_frames,
                &postgres_frames,
                "standard_conforming_strings",
            );
        },
    );
    parity_case(
        &mut observed,
        "backend_key_data_structure_matches_postgres",
        || {
            assert_eq!(first(&facet_frames, b'K').payload.len(), 8);
            assert_eq!(first(&postgres_frames, b'K').payload.len(), 8);
        },
    );
    parity_case(
        &mut observed,
        "ready_for_query_bytes_match_postgres",
        || {
            assert_eq!(
                first(&facet_frames, b'Z').wire,
                first(&postgres_frames, b'Z').wire
            );
        },
    );
    parity_case(
        &mut observed,
        "query_select_1_row_description_matches",
        || {
            assert_eq!(
                first(&facet_frames, b'T').wire,
                first(&postgres_frames, b'T').wire
            );
        },
    );
    parity_case(&mut observed, "query_select_1_data_row_matches", || {
        assert_eq!(
            first(&facet_frames, b'D').wire,
            first(&postgres_frames, b'D').wire
        );
    });
    parity_case(
        &mut observed,
        "command_complete_bytes_match_postgres",
        || {
            assert_eq!(
                first(&facet_frames, b'C').wire,
                first(&postgres_frames, b'C').wire
            );
        },
    );

    let facet_null = run_psql(&psql, &facet_target, "SELECT inet_server_addr() AS empty")
        .expect("query Facet NULL");
    assert_psql_success(&facet_null, "Facet NULL SELECT");
    let postgres_null = run_psql(&psql, &postgres_target, "SELECT NULL::text AS empty")
        .expect("query PostgreSQL NULL");
    assert_psql_success(&postgres_null, "PostgreSQL NULL SELECT");
    let facet_null_frames = facet_proxy.drain_backend_frames();
    let postgres_null_frames = postgres_proxy.drain_backend_frames();
    parity_case(&mut observed, "query_select_null_data_row_matches", || {
        assert_eq!(
            first(&facet_null_frames, b'D').wire,
            first(&postgres_null_frames, b'D').wire
        );
    });

    let facet_error = run_psql(&psql, &facet_target, "SELEKT bad_syntax").expect("bad Facet SQL");
    let postgres_error =
        run_psql(&psql, &postgres_target, "SELEKT bad_syntax").expect("bad PostgreSQL SQL");
    assert!(!facet_error.status.success());
    assert!(!postgres_error.status.success());
    let facet_error_frames = facet_proxy.drain_backend_frames();
    let postgres_error_frames = postgres_proxy.drain_backend_frames();
    parity_case(
        &mut observed,
        "error_response_structure_from_bad_query",
        || {
            assert_syntax_error(&first(&facet_error_frames, b'E').payload);
            assert_syntax_error(&first(&postgres_error_frames, b'E').payload);
        },
    );

    let facet_startups = facet_proxy.drain_startup_packets();
    let postgres_startups = postgres_proxy.drain_startup_packets();
    parity_case(
        &mut observed,
        "decode_startup_roundtrip_matches_what_we_send",
        || {
            let facet_parameters =
                startup_parameters(facet_startups.first().expect("Facet startup packet"));
            let postgres_parameters = startup_parameters(
                postgres_startups
                    .first()
                    .expect("PostgreSQL startup packet"),
            );
            assert_eq!(
                facet_parameters.get("user").map(String::as_str),
                Some("facet")
            );
            assert_eq!(
                facet_parameters.get("database").map(String::as_str),
                Some("facet-e2e")
            );
            assert_eq!(
                postgres_parameters.get("user").map(String::as_str),
                Some("postgres")
            );
            assert_eq!(
                postgres_parameters.get("database").map(String::as_str),
                Some("postgres")
            );
        },
    );

    let defined =
        run_psql(&psql, &facet_target, DEFINE_PROJECTION).expect("define Facet projection");
    assert_psql_success(&defined, "DEFINE PROJECTION");
    let _ = facet_proxy.drain_backend_frames();
    let facet_rows = run_psql(
        &psql,
        &facet_target,
        "SELECT id, name FROM people ORDER BY id",
    )
    .expect("query Facet rows");
    assert_psql_success(&facet_rows, "Facet multi-row SELECT");
    let postgres_rows = run_psql(
        &psql,
        &postgres_target,
        "SELECT * FROM (VALUES (1::bigint, 'Alice'::text), (2::bigint, 'Bob'::text), \
         (3::bigint, 'Carol'::text)) AS people(id, name) ORDER BY id",
    )
    .expect("query PostgreSQL rows");
    assert_psql_success(&postgres_rows, "PostgreSQL multi-row SELECT");
    let facet_row_frames = facet_proxy.drain_backend_frames();
    let postgres_row_frames = postgres_proxy.drain_backend_frames();
    parity_case(&mut observed, "multiple_rows_data_row_matches", || {
        assert_eq!(
            frames_of_type(&facet_row_frames, b'T'),
            frames_of_type(&postgres_row_frames, b'T')
        );
        assert_eq!(
            frames_of_type(&facet_row_frames, b'D'),
            frames_of_type(&postgres_row_frames, b'D')
        );
        assert_eq!(
            frames_of_type(&facet_row_frames, b'C'),
            frames_of_type(&postgres_row_frames, b'C')
        );
    });

    assert_eq!(
        observed, 12,
        "all migrated live-PostgreSQL parity cases must execute"
    );
}

fn parity_case(cases: &mut usize, name: &str, assertion: impl FnOnce()) {
    assertion();
    *cases += 1;
    eprintln!("observed live PostgreSQL parity case: {name}");
}

fn assert_handshake_shape(frames: &[BackendFrame]) {
    let ready = frames
        .iter()
        .position(|frame| frame.message_type == b'Z')
        .expect("ReadyForQuery in handshake");
    let handshake = &frames[..=ready];
    assert_eq!(
        handshake.first().map(|frame| frame.message_type),
        Some(b'R')
    );
    assert!(handshake.iter().any(|frame| frame.message_type == b'S'));
    assert!(handshake.iter().any(|frame| frame.message_type == b'K'));
    assert_eq!(handshake.last().map(|frame| frame.message_type), Some(b'Z'));
}

fn auth_ok(frames: &[BackendFrame]) -> &BackendFrame {
    frames
        .iter()
        .find(|frame| {
            frame.message_type == b'R'
                && frame.payload.get(..4) == Some(0_i32.to_be_bytes().as_slice())
        })
        .expect("AuthenticationOk frame")
}

fn first(frames: &[BackendFrame], message_type: u8) -> &BackendFrame {
    frames
        .iter()
        .find(|frame| frame.message_type == message_type)
        .unwrap_or_else(|| panic!("missing backend frame {}", message_type as char))
}

fn parameter<'a>(frames: &'a [BackendFrame], key: &str) -> &'a BackendFrame {
    frames
        .iter()
        .find(|frame| {
            frame.message_type == b'S'
                && frame
                    .payload
                    .split(|byte| *byte == 0)
                    .next()
                    .is_some_and(|candidate| candidate == key.as_bytes())
        })
        .unwrap_or_else(|| panic!("missing ParameterStatus {key}"))
}

fn assert_common_parameter_equal(facet: &[BackendFrame], postgres: &[BackendFrame], key: &str) {
    assert_eq!(parameter(facet, key).wire, parameter(postgres, key).wire);
}

fn frames_of_type(frames: &[BackendFrame], message_type: u8) -> Vec<Vec<u8>> {
    frames
        .iter()
        .filter(|frame| frame.message_type == message_type)
        .map(|frame| frame.wire.clone())
        .collect()
}

fn assert_syntax_error(payload: &[u8]) {
    let fields = error_fields(payload);
    assert_eq!(fields.get(&b'S').map(String::as_str), Some("ERROR"));
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("42601"));
    assert!(
        fields
            .get(&b'M')
            .is_some_and(|message| message.to_ascii_uppercase().contains("SELEKT"))
    );
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

fn startup_parameters(packet: &[u8]) -> HashMap<String, String> {
    assert!(packet.len() >= 9, "startup packet too short");
    let length = i32::from_be_bytes(packet[..4].try_into().expect("startup length")) as usize;
    assert_eq!(length, packet.len());
    let protocol = i32::from_be_bytes(packet[4..8].try_into().expect("protocol version"));
    assert_eq!(protocol, 196_608);

    let fields: Vec<&[u8]> = packet[8..]
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect();
    assert_eq!(
        fields.len() % 2,
        0,
        "startup parameters must be key/value pairs"
    );

    fields
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            (
                String::from_utf8(pair[0].to_vec()).expect("startup key"),
                String::from_utf8(pair[1].to_vec()).expect("startup value"),
            )
        })
        .collect()
}
