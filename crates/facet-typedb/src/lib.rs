/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Public TypeDB HTTP v1 adapter with declared-type scalar decoding.

#![forbid(unsafe_code)]

use std::str::FromStr;

use chrono::{DateTime, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use chrono_tz::Tz;
use facet_core::{Cell, DurationValue, ProjectionDefinition, ScalarType};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct TypeDbClient {
    http: Client,
    origin: String,
    username: String,
    password: String,
}

#[derive(Debug, Error)]
pub enum TypeDbError {
    #[error("TypeDB HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("TypeDB returned HTTP {status}: {body}")]
    Response { status: StatusCode, body: String },
    #[error("TypeDB response was invalid: {0}")]
    InvalidResponse(String),
    #[error("{message}")]
    Unrepresentable {
        sqlstate: &'static str,
        message: String,
    },
}

impl TypeDbError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Unrepresentable { sqlstate, .. } => sqlstate,
            Self::Http(_) | Self::Response { .. } | Self::InvalidResponse(_) => "XX000",
        }
    }
}

impl TypeDbClient {
    pub fn new(origin: String, username: String, password: String) -> Result<Self, TypeDbError> {
        let origin = origin.trim_end_matches('/').to_owned();
        let http = Client::builder().build()?;
        Ok(Self {
            http,
            origin,
            username,
            password,
        })
    }

    pub async fn materialize(
        &self,
        database: &str,
        definition: &ProjectionDefinition,
    ) -> Result<Vec<Vec<Option<Cell>>>, TypeDbError> {
        let request = json!({
            "databaseName": database,
            "transactionType": "read",
            "query": definition.source_query(),
            "queryOptions": {
                "includeInstanceTypes": false,
                "includeQueryStructure": false
            },
            "commit": false
        });
        let token = self.sign_in().await?;
        let (status, body) = self.post_query(&token, &request).await?;
        if !status.is_success() {
            return Err(TypeDbError::Response { status, body });
        }

        let value: Value = serde_json::from_str(&body)
            .map_err(|error| TypeDbError::InvalidResponse(error.to_string()))?;
        let answers = value
            .get("answers")
            .and_then(Value::as_array)
            .ok_or_else(|| TypeDbError::InvalidResponse("missing answers array".to_owned()))?;

        answers
            .iter()
            .map(|answer| {
                definition
                    .columns()
                    .iter()
                    .map(|column| decode_cell(answer.get(&column.name), column.scalar_type))
                    .collect()
            })
            .collect()
    }

    async fn post_query(
        &self,
        token: &str,
        request: &Value,
    ) -> Result<(StatusCode, String), TypeDbError> {
        let response = self
            .http
            .post(format!("{}/v1/query", self.origin))
            .bearer_auth(token)
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        Ok((status, body))
    }

    async fn sign_in(&self) -> Result<String, TypeDbError> {
        let response = self
            .http
            .post(format!("{}/v1/signin", self.origin))
            .json(&json!({
                "username": self.username,
                "password": self.password
            }))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(TypeDbError::Response { status, body });
        }
        let value: Value = serde_json::from_str(&body)
            .map_err(|error| TypeDbError::InvalidResponse(error.to_string()))?;
        value
            .get("token")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| TypeDbError::InvalidResponse("missing signin token".to_owned()))
    }
}

fn decode_cell(
    value: Option<&Value>,
    scalar_type: ScalarType,
) -> Result<Option<Cell>, TypeDbError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(value) = value.get("value") {
        return decode_cell(Some(value), scalar_type);
    }
    if let Some(values) = value.as_array() {
        return match values.as_slice() {
            [] => Ok(None),
            [value] => decode_cell(Some(value), scalar_type),
            _ => Err(TypeDbError::InvalidResponse(
                "projection cell contained multiple values".to_owned(),
            )),
        };
    }

    match scalar_type {
        ScalarType::Boolean => value
            .as_bool()
            .map(Cell::Boolean)
            .map(Some)
            .ok_or_else(|| invalid_cell("boolean", value)),
        ScalarType::Integer => value
            .as_i64()
            .map(Cell::Integer)
            .map(Some)
            .ok_or_else(|| invalid_cell("integer", value)),
        ScalarType::Double => {
            let value = value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| invalid_cell("finite double", value))?;
            Ok(Some(Cell::Double(value)))
        }
        ScalarType::Decimal => {
            let value = string_cell("decimal", value)?;
            let decimal = value
                .strip_suffix("dec")
                .ok_or_else(|| invalid_cell("decimal with dec suffix", value))?;
            Ok(Some(Cell::Decimal(Decimal::from_str(decimal).map_err(
                |error| TypeDbError::InvalidResponse(format!("invalid decimal {value:?}: {error}")),
            )?)))
        }
        ScalarType::Date => {
            let value = string_cell("date", value)?;
            Ok(Some(Cell::Date(
                NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
                    TypeDbError::InvalidResponse(format!("invalid date {value:?}: {error}"))
                })?,
            )))
        }
        ScalarType::DateTime => {
            let value = string_cell("datetime", value)?;
            let datetime = parse_naive_datetime(value)?;
            reject_submicrosecond(u64::from(datetime.nanosecond()), "datetime", "22008")?;
            Ok(Some(Cell::DateTime(datetime)))
        }
        ScalarType::DateTimeTz => {
            let value = string_cell("datetime-tz", value)?;
            let datetime = parse_datetime_tz(value)?;
            reject_submicrosecond(u64::from(datetime.nanosecond()), "datetime-tz", "22008")?;
            Ok(Some(Cell::DateTimeTz(datetime)))
        }
        ScalarType::Duration => {
            let value = string_cell("duration", value)?;
            let duration = parse_duration(value)?;
            reject_submicrosecond(duration.nanoseconds, "duration", "22015")?;
            Ok(Some(Cell::Duration(duration)))
        }
        ScalarType::String => value
            .as_str()
            .map(|value| Some(Cell::String(value.to_owned())))
            .ok_or_else(|| invalid_cell("string", value)),
    }
}

fn invalid_cell(expected: &str, value: impl std::fmt::Display) -> TypeDbError {
    TypeDbError::InvalidResponse(format!("expected {expected} projection cell, got {value}"))
}

fn string_cell<'a>(expected: &str, value: &'a Value) -> Result<&'a str, TypeDbError> {
    value.as_str().ok_or_else(|| invalid_cell(expected, value))
}

fn parse_naive_datetime(value: &str) -> Result<NaiveDateTime, TypeDbError> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").map_err(|error| {
        TypeDbError::InvalidResponse(format!("invalid datetime {value:?}: {error}"))
    })
}

fn parse_datetime_tz(value: &str) -> Result<DateTime<FixedOffset>, TypeDbError> {
    if let Some((datetime, timezone)) = value.rsplit_once(' ') {
        let local = parse_naive_datetime(datetime)?;
        let timezone = Tz::from_str(timezone).map_err(|error| {
            TypeDbError::InvalidResponse(format!(
                "invalid IANA timezone in datetime-tz {value:?}: {error}"
            ))
        })?;
        return match timezone.from_local_datetime(&local) {
            LocalResult::Single(datetime) => Ok(datetime.fixed_offset()),
            LocalResult::Ambiguous(_, _) => Err(TypeDbError::Unrepresentable {
                sqlstate: "22008",
                message: format!("datetime-tz {value:?} is ambiguous"),
            }),
            LocalResult::None => Err(TypeDbError::Unrepresentable {
                sqlstate: "22008",
                message: format!("datetime-tz {value:?} does not exist"),
            }),
        };
    }

    DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%:z").map_err(|error| {
        TypeDbError::InvalidResponse(format!(
            "invalid fixed-offset datetime-tz {value:?}: {error}"
        ))
    })
}

fn reject_submicrosecond(
    nanoseconds: u64,
    type_name: &str,
    sqlstate: &'static str,
) -> Result<(), TypeDbError> {
    if nanoseconds.is_multiple_of(1_000) {
        Ok(())
    } else {
        Err(TypeDbError::Unrepresentable {
            sqlstate,
            message: format!(
                "{type_name} contains sub-microsecond precision that PostgreSQL cannot preserve"
            ),
        })
    }
}

fn parse_duration(value: &str) -> Result<DurationValue, TypeDbError> {
    let body = value
        .strip_prefix('P')
        .ok_or_else(|| invalid_cell("ISO duration", value))?;
    let (date, time) = body.split_once('T').unwrap_or((body, ""));
    let date_parts = parse_duration_components(date, false)?;
    let time_parts = parse_duration_components(time, true)?;

    let years = component(&date_parts, 'Y')?;
    let months = component(&date_parts, 'M')?;
    let days = component(&date_parts, 'D')?;
    let hours = component(&time_parts, 'H')?;
    let minutes = component(&time_parts, 'M')?;
    let (seconds, fractional_nanoseconds) = seconds_component(&time_parts)?;

    let months = years
        .checked_mul(12)
        .and_then(|value| value.checked_add(months))
        .ok_or_else(|| invalid_cell("in-range duration", value))?;
    let total_seconds = hours
        .checked_mul(3_600)
        .and_then(|value| {
            minutes
                .checked_mul(60)
                .and_then(|minutes| value.checked_add(minutes))
        })
        .and_then(|value| value.checked_add(seconds))
        .ok_or_else(|| invalid_cell("in-range duration", value))?;
    let nanoseconds = total_seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fractional_nanoseconds))
        .ok_or_else(|| invalid_cell("in-range duration", value))?;

    Ok(DurationValue {
        months: u32::try_from(months).map_err(|_| invalid_cell("in-range duration", value))?,
        days: u32::try_from(days).map_err(|_| invalid_cell("in-range duration", value))?,
        nanoseconds,
    })
}

fn parse_duration_components(
    input: &str,
    allow_fraction: bool,
) -> Result<Vec<(char, String)>, TypeDbError> {
    let mut components = Vec::new();
    let mut number = String::new();
    for character in input.chars() {
        if character.is_ascii_digit() || (allow_fraction && character == '.') {
            number.push(character);
        } else if matches!(character, 'Y' | 'M' | 'D' | 'H' | 'S') && !number.is_empty() {
            components.push((character, std::mem::take(&mut number)));
        } else {
            return Err(invalid_cell("ISO duration", input));
        }
    }
    if number.is_empty() {
        Ok(components)
    } else {
        Err(invalid_cell("ISO duration", input))
    }
}

fn component(components: &[(char, String)], symbol: char) -> Result<u64, TypeDbError> {
    components
        .iter()
        .find(|(candidate, _)| *candidate == symbol)
        .map(|(_, value)| {
            value.parse().map_err(|error| {
                TypeDbError::InvalidResponse(format!("invalid duration component: {error}"))
            })
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn seconds_component(components: &[(char, String)]) -> Result<(u64, u64), TypeDbError> {
    let Some((_, value)) = components.iter().find(|(symbol, _)| *symbol == 'S') else {
        return Ok((0, 0));
    };
    let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
    let seconds = seconds.parse().map_err(|error| {
        TypeDbError::InvalidResponse(format!("invalid duration seconds: {error}"))
    })?;
    if fraction.len() > 9 {
        return Err(invalid_cell(
            "duration with at most 9 fractional digits",
            value,
        ));
    }
    let mut padded = fraction.to_owned();
    padded.extend(std::iter::repeat_n('0', 9 - fraction.len()));
    let nanoseconds = if padded.is_empty() {
        0
    } else {
        padded.parse().map_err(|error| {
            TypeDbError::InvalidResponse(format!("invalid duration fraction: {error}"))
        })?
    };
    Ok((seconds, nanoseconds))
}
