/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Observable equivalents of the 19 encoder goldens in the embedded source.
//!
//! Expected bytes originate from PostgreSQL 16 captures. Unlike the embedded
//! unit tests, these assertions record frames emitted by the real Facet
//! process through a transparent TCP proxy.

use std::collections::HashMap;

use facet_e2e::{
    BackendFrame, DEFINE_PROJECTION, FacetProcess, RecordingProxy, TypeDbProcess,
    assert_psql_success, facet_binary, psql_binary, run_psql,
};

const PG_AUTH_OK: &str = "520000000800000000";
const PG_PARAM_TIMEZONE: &str = "530000001154696d655a6f6e650055544300";
const PG_PARAM_CLIENT_ENCODING: &str = "5300000019636c69656e745f656e636f64696e67005554463800";
const PG_PARAM_SERVER_VERSION: &str = "53000000197365727665725f76657273696f6e0031362e313300";
const PG_PARAM_SERVER_ENCODING: &str = "53000000197365727665725f656e636f64696e67005554463800";
const PG_PARAM_DATESTYLE: &str = "5300000017446174655374796c650049534f2c204d445900";
const PG_PARAM_INT_DATETIMES: &str = "5300000019696e74656765725f6461746574696d6573006f6e00";
const PG_PARAM_STD_STRINGS: &str =
    "53000000237374616e646172645f636f6e666f726d696e675f737472696e6773006f6e00";
const PG_READY_IDLE: &str = "5a0000000549";
const PG_ROW_DESC_TEXT_42: &str =
    "540000001f0001616e737765720000000000000000000019ffffffffffff0000";
const PG_DATA_ROW_42: &str = "440000000c0001000000023432";
const PG_DATA_ROW_NULL: &str = "440000000a0001ffffffff";
const PG_COMMAND_SELECT_1: &str = "430000000d53454c454354203100";
const PG_COMMAND_SELECT_3: &str = "430000000d53454c454354203300";
const PG_ROW_DESC_PEOPLE: &str = "54000000320002696400000000000000000000140008ffffffff00006e616d650000000000000000000019ffffffffffff0000";
const PG_DATA_ROW_ALICE: &str = "44000000140002000000013100000005416c696365";
const PG_DATA_ROW_BOB: &str = "44000000120002000000013200000003426f62";
const PG_DATA_ROW_CAROL: &str = "440000001400020000000133000000054361726f6c";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nineteen_postgres_goldens_are_observable_on_the_facet_socket() {
    let typedb = TypeDbProcess::start()
        .await
        .expect("the real TypeDB server must start");
    typedb
        .seed_three()
        .await
        .expect("golden rows must be seeded through real TypeDB HTTP");
    let workspace = tempfile::tempdir().expect("create Facet golden workspace");
    let facet = FacetProcess::start(
        &facet_binary().expect("FACET_E2E_FACET_BIN must identify the Facet executable under test"),
        typedb.http_origin(),
        workspace.path(),
    )
    .expect("the real Facet process must expose pgwire");
    let proxy =
        RecordingProxy::start(facet.postgres_address()).expect("start transparent pgwire recorder");
    let target = facet.target().through(proxy.address());
    let psql = psql_binary();
    let mut observed = 0_usize;

    let select_42 =
        run_psql(&psql, &target, "SELECT '42' AS answer").expect("run text literal query");
    assert_psql_success(&select_42, "SELECT text literal");
    let first_frames = proxy.drain_backend_frames();

    observe(
        &mut observed,
        "auth_ok_matches_postgres_golden_bytes",
        || {
            assert_eq!(auth_ok(&first_frames).wire, hex(PG_AUTH_OK));
        },
    );
    observe(&mut observed, "parameter_status_timezone_matches", || {
        assert_eq!(
            parameter(&first_frames, "TimeZone").wire,
            hex(PG_PARAM_TIMEZONE)
        );
    });
    observe(
        &mut observed,
        "parameter_status_client_encoding_matches",
        || {
            assert_eq!(
                parameter(&first_frames, "client_encoding").wire,
                hex(PG_PARAM_CLIENT_ENCODING)
            );
        },
    );
    observe(
        &mut observed,
        "parameter_status_server_version_matches",
        || {
            assert_eq!(
                parameter(&first_frames, "server_version").wire,
                hex(PG_PARAM_SERVER_VERSION)
            );
        },
    );
    observe(
        &mut observed,
        "parameter_status_server_encoding_matches",
        || {
            assert_eq!(
                parameter(&first_frames, "server_encoding").wire,
                hex(PG_PARAM_SERVER_ENCODING)
            );
        },
    );
    observe(&mut observed, "parameter_status_datestyle_matches", || {
        assert_eq!(
            parameter(&first_frames, "DateStyle").wire,
            hex(PG_PARAM_DATESTYLE)
        );
    });
    observe(
        &mut observed,
        "parameter_status_integer_datetimes_matches",
        || {
            assert_eq!(
                parameter(&first_frames, "integer_datetimes").wire,
                hex(PG_PARAM_INT_DATETIMES)
            );
        },
    );
    observe(
        &mut observed,
        "parameter_status_standard_conforming_strings_matches",
        || {
            assert_eq!(
                parameter(&first_frames, "standard_conforming_strings").wire,
                hex(PG_PARAM_STD_STRINGS)
            );
        },
    );
    observe(
        &mut observed,
        "backend_key_data_matches_postgres_structure",
        || {
            let frame = first(&first_frames, b'K');
            assert_eq!(frame.payload.len(), 8);
            assert_eq!(frame.wire.len(), 13);
        },
    );
    observe(
        &mut observed,
        "ready_for_query_idle_matches_postgres",
        || {
            assert_eq!(first(&first_frames, b'Z').wire, hex(PG_READY_IDLE));
        },
    );
    observe(
        &mut observed,
        "row_description_select_42_matches_postgres",
        || {
            assert_eq!(first(&first_frames, b'T').wire, hex(PG_ROW_DESC_TEXT_42));
        },
    );
    observe(&mut observed, "data_row_select_42_matches_postgres", || {
        assert_eq!(first(&first_frames, b'D').wire, hex(PG_DATA_ROW_42));
    });
    observe(
        &mut observed,
        "command_complete_select_1_matches_postgres",
        || {
            assert_eq!(first(&first_frames, b'C').wire, hex(PG_COMMAND_SELECT_1));
        },
    );
    observe(
        &mut observed,
        "handshake_sequence_starts_with_auth_ends_with_ready",
        || {
            let ready = first_frames
                .iter()
                .position(|frame| frame.message_type == b'Z')
                .expect("handshake ReadyForQuery");
            assert_eq!(
                first_frames.first().map(|frame| frame.message_type),
                Some(b'R')
            );
            assert_eq!(first_frames[ready].message_type, b'Z');
        },
    );

    let select_null = run_psql(&psql, &target, "SELECT inet_server_addr() AS empty")
        .expect("run nullable expression");
    assert_psql_success(&select_null, "SELECT nullable expression");
    let null_frames = proxy.drain_backend_frames();
    observe(&mut observed, "data_row_null_matches_postgres", || {
        assert_eq!(first(&null_frames, b'D').wire, hex(PG_DATA_ROW_NULL));
    });

    let defined = run_psql(&psql, &target, DEFINE_PROJECTION).expect("define people projection");
    assert_psql_success(&defined, "DEFINE PROJECTION");
    let _ = proxy.drain_backend_frames();

    let selected = run_psql(&psql, &target, "SELECT id, name FROM people ORDER BY id")
        .expect("select people projection");
    assert_psql_success(&selected, "SELECT people");
    let people_frames = proxy.drain_backend_frames();
    observe(
        &mut observed,
        "command_complete_select_3_matches_postgres",
        || {
            assert_eq!(first(&people_frames, b'C').wire, hex(PG_COMMAND_SELECT_3));
        },
    );
    observe(&mut observed, "multi_row_data_rows_match_postgres", || {
        let mut rows = people_frames
            .iter()
            .filter(|frame| frame.message_type == b'D');
        assert_eq!(rows.next().expect("Alice row").wire, hex(PG_DATA_ROW_ALICE));
        assert_eq!(rows.next().expect("Bob row").wire, hex(PG_DATA_ROW_BOB));
        assert_eq!(rows.next().expect("Carol row").wire, hex(PG_DATA_ROW_CAROL));
        assert!(rows.next().is_none());
    });
    observe(
        &mut observed,
        "row_description_multi_column_matches_postgres",
        || {
            assert_eq!(first(&people_frames, b'T').wire, hex(PG_ROW_DESC_PEOPLE));
        },
    );

    let invalid = run_psql(&psql, &target, "SELEKT bad_syntax").expect("run invalid query");
    assert!(
        !invalid.status.success(),
        "invalid SQL unexpectedly succeeded"
    );
    let error_frames = proxy.drain_backend_frames();
    observe(
        &mut observed,
        "error_response_contains_correct_fields",
        || {
            let fields = error_fields(&first(&error_frames, b'E').payload);
            assert_eq!(fields.get(&b'S').map(String::as_str), Some("ERROR"));
            assert_eq!(fields.get(&b'C').map(String::as_str), Some("42601"));
            assert!(
                fields
                    .get(&b'M')
                    .is_some_and(|message| message.to_ascii_uppercase().contains("SELEKT"))
            );
        },
    );

    assert_eq!(
        observed, 19,
        "every migrated golden case must observe real Facet wire bytes"
    );
}

fn observe(cases: &mut usize, name: &str, assertion: impl FnOnce()) {
    assertion();
    *cases += 1;
    eprintln!("observed golden case: {name}");
}

fn first(frames: &[BackendFrame], message_type: u8) -> &BackendFrame {
    frames
        .iter()
        .find(|frame| frame.message_type == message_type)
        .unwrap_or_else(|| panic!("missing backend frame {}", message_type as char))
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

fn hex(value: &str) -> Vec<u8> {
    (0..value.len())
        .step_by(2)
        .map(|position| u8::from_str_radix(&value[position..position + 2], 16).expect("golden hex"))
        .collect()
}
