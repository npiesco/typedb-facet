/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Projection definitions, typed snapshots, and SQL execution.
//!
//! This adapts the protocol-neutral behavior from TypeDB's
//! `projection/definition.rs`, `projection/schema_parser.rs`,
//! `projection/catalog.rs`, and `projection/pgwire/{sql_parser,query_executor}.rs`
//! without depending on any TypeDB-internal crate.

#![forbid(unsafe_code)]

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Offset};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{Expr, ObjectNamePart, OrderByKind, SelectItem, SetExpr, Statement, TableFactor, Value},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ScalarType {
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "integer")]
    Integer,
    #[serde(rename = "double")]
    Double,
    #[serde(rename = "decimal")]
    Decimal,
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "datetime")]
    DateTime,
    #[serde(rename = "datetime-tz")]
    DateTimeTz,
    #[serde(rename = "duration")]
    Duration,
    #[serde(rename = "string")]
    String,
}

impl ScalarType {
    pub fn postgres_oid(self) -> u32 {
        match self {
            Self::Boolean => 16,
            Self::Integer => 20,
            Self::Double => 701,
            Self::Decimal => 1700,
            Self::Date => 1082,
            Self::DateTime => 1114,
            Self::DateTimeTz => 1184,
            Self::Duration => 1186,
            Self::String => 25,
        }
    }

    pub fn postgres_type_size(self) -> i16 {
        match self {
            Self::Boolean => 1,
            Self::Integer | Self::Double | Self::DateTime | Self::DateTimeTz => 8,
            Self::Date => 4,
            Self::Decimal | Self::Duration | Self::String => -1,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Decimal(Decimal),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    DateTimeTz(DateTime<chrono::FixedOffset>),
    Duration(DurationValue),
    String(String),
}

impl Cell {
    pub fn scalar_type(&self) -> ScalarType {
        match self {
            Self::Boolean(_) => ScalarType::Boolean,
            Self::Integer(_) => ScalarType::Integer,
            Self::Double(_) => ScalarType::Double,
            Self::Decimal(_) => ScalarType::Decimal,
            Self::Date(_) => ScalarType::Date,
            Self::DateTime(_) => ScalarType::DateTime,
            Self::DateTimeTz(_) => ScalarType::DateTimeTz,
            Self::Duration(_) => ScalarType::Duration,
            Self::String(_) => ScalarType::String,
        }
    }

    pub fn postgres_text(&self) -> String {
        match self {
            Self::Boolean(value) => if *value { "t" } else { "f" }.to_owned(),
            Self::Integer(value) => value.to_string(),
            Self::Double(value) => value.to_string(),
            Self::Decimal(value) => value.to_string(),
            Self::Date(value) => value.to_string(),
            Self::DateTime(value) => value.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            Self::DateTimeTz(value) => format_datetime_tz(value),
            Self::Duration(value) => value.postgres_text(),
            Self::String(value) => value.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationValue {
    pub months: u32,
    pub days: u32,
    pub nanoseconds: u64,
}

impl DurationValue {
    pub fn postgres_text(self) -> String {
        let mut parts = Vec::new();
        let years = self.months / 12;
        let months = self.months % 12;
        if years != 0 {
            parts.push(format!(
                "{years} {}",
                if years == 1 { "year" } else { "years" }
            ));
        }
        if months != 0 {
            parts.push(format!(
                "{months} {}",
                if months == 1 { "mon" } else { "mons" }
            ));
        }
        if self.days != 0 {
            parts.push(format!(
                "{} {}",
                self.days,
                if self.days == 1 { "day" } else { "days" }
            ));
        }

        if self.nanoseconds != 0 || parts.is_empty() {
            let total_seconds = self.nanoseconds / 1_000_000_000;
            let microseconds = (self.nanoseconds % 1_000_000_000) / 1_000;
            let hours = total_seconds / 3_600;
            let minutes = total_seconds % 3_600 / 60;
            let seconds = total_seconds % 60;
            if microseconds == 0 {
                parts.push(format!("{hours:02}:{minutes:02}:{seconds:02}"));
            } else {
                parts.push(format!(
                    "{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}"
                ));
            }
        }
        parts.join(" ")
    }
}

fn format_datetime_tz(value: &DateTime<chrono::FixedOffset>) -> String {
    let local = value.naive_local().format("%Y-%m-%d %H:%M:%S%.6f");
    let seconds = value.offset().fix().local_minus_utc();
    let sign = if seconds < 0 { '-' } else { '+' };
    let absolute = seconds.unsigned_abs();
    let hours = absolute / 3_600;
    let minutes = absolute % 3_600 / 60;
    if minutes == 0 {
        format!("{local}{sign}{hours:02}")
    } else {
        format!("{local}{sign}{hours:02}:{minutes:02}")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub scalar_type: ScalarType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionDefinition {
    name: String,
    columns: Vec<ColumnDefinition>,
    source_query: String,
}

impl ProjectionDefinition {
    pub fn new(
        name: String,
        columns: Vec<ColumnDefinition>,
        source_query: String,
    ) -> Result<Self, CoreError> {
        if name.is_empty() {
            return Err(CoreError::InvalidProjection(
                "projection name must not be empty".to_owned(),
            ));
        }
        if columns.is_empty() {
            return Err(CoreError::InvalidProjection(
                "projection must have at least one column".to_owned(),
            ));
        }
        if source_query.is_empty() {
            return Err(CoreError::InvalidProjection(
                "projection source query must not be empty".to_owned(),
            ));
        }

        let mut names = HashSet::new();
        for column in &columns {
            if !names.insert(column.name.to_ascii_lowercase()) {
                return Err(CoreError::InvalidProjection(format!(
                    "duplicate column name: '{}'",
                    column.name
                )));
            }
        }

        Ok(Self {
            name,
            columns,
            source_query,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    pub fn source_query(&self) -> &str {
        &self.source_query
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionStatement {
    Define(ProjectionDefinition),
    Undefine { name: String },
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("{0}")]
    InvalidProjection(String),
    #[error("projection DDL syntax error: {0}")]
    ProjectionSyntax(String),
    #[error("SQL syntax error: {0}")]
    SqlSyntax(String),
    #[error("unsupported SQL: {0}")]
    UnsupportedSql(String),
    #[error("struct projection columns are not supported")]
    StructProjectionUnsupported,
}

pub fn parse_projection_ddl(input: &str) -> Result<ProjectionStatement, CoreError> {
    let input = input.trim();
    if starts_with_keyword(input, "define") {
        parse_define(input)
    } else if starts_with_keyword(input, "undefine") {
        parse_undefine(input)
    } else {
        Err(CoreError::ProjectionSyntax(
            "expected DEFINE or UNDEFINE".to_owned(),
        ))
    }
}

fn parse_define(input: &str) -> Result<ProjectionStatement, CoreError> {
    let mut cursor = Cursor::new(input);
    cursor.keyword("define")?;
    cursor.keyword("projection")?;
    let name = cursor.identifier()?;
    cursor.character('(')?;

    let mut columns = Vec::new();
    loop {
        cursor.whitespace();
        if cursor.peek() == Some(')') {
            cursor.advance(')');
            break;
        }
        if !columns.is_empty() {
            cursor.character(',')?;
        }
        let column_name = cursor.identifier()?;
        cursor.character(':')?;
        let type_name = cursor.type_identifier()?;
        let scalar_type = match type_name.to_ascii_lowercase().as_str() {
            "boolean" | "bool" => ScalarType::Boolean,
            "integer" | "int" | "long" => ScalarType::Integer,
            "double" | "float" => ScalarType::Double,
            "decimal" => ScalarType::Decimal,
            "date" => ScalarType::Date,
            "datetime" => ScalarType::DateTime,
            "datetime-tz" | "datetimetz" => ScalarType::DateTimeTz,
            "duration" => ScalarType::Duration,
            "string" | "text" => ScalarType::String,
            "struct" => return Err(CoreError::StructProjectionUnsupported),
            other => {
                return Err(CoreError::InvalidProjection(format!(
                    "unknown projection type '{other}'"
                )));
            }
        };
        columns.push(ColumnDefinition {
            name: column_name,
            scalar_type,
        });
    }

    cursor.keyword("as")?;
    let source_query = cursor.quoted_string()?;
    cursor.character(';')?;
    cursor.finish()?;
    Ok(ProjectionStatement::Define(ProjectionDefinition::new(
        name,
        columns,
        source_query,
    )?))
}

fn parse_undefine(input: &str) -> Result<ProjectionStatement, CoreError> {
    let mut cursor = Cursor::new(input);
    cursor.keyword("undefine")?;
    cursor.keyword("projection")?;
    let name = cursor.identifier()?;
    cursor.character(';')?;
    cursor.finish()?;
    Ok(ProjectionStatement::Undefine { name })
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        && input
            .get(keyword.len()..)
            .and_then(|tail| tail.chars().next())
            .is_none_or(|character| character.is_ascii_whitespace())
}

struct Cursor<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn whitespace(&mut self) {
        while self
            .input
            .as_bytes()
            .get(self.position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.position += 1;
        }
    }

    fn keyword(&mut self, expected: &'static str) -> Result<(), CoreError> {
        self.whitespace();
        let found = self.identifier()?;
        if found.eq_ignore_ascii_case(expected) {
            Ok(())
        } else {
            Err(CoreError::ProjectionSyntax(format!(
                "expected keyword '{expected}', found '{found}'"
            )))
        }
    }

    fn identifier(&mut self) -> Result<String, CoreError> {
        self.whitespace();
        let start = self.position;
        while self
            .input
            .as_bytes()
            .get(self.position)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            self.position += 1;
        }
        if self.position == start {
            Err(CoreError::ProjectionSyntax(format!(
                "expected identifier at position {}",
                self.position
            )))
        } else {
            Ok(self.input[start..self.position].to_owned())
        }
    }

    fn type_identifier(&mut self) -> Result<String, CoreError> {
        self.whitespace();
        let start = self.position;
        while self
            .input
            .as_bytes()
            .get(self.position)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
        {
            self.position += 1;
        }
        if self.position == start {
            Err(CoreError::ProjectionSyntax(format!(
                "expected type at position {}",
                self.position
            )))
        } else {
            Ok(self.input[start..self.position].to_owned())
        }
    }

    fn character(&mut self, expected: char) -> Result<(), CoreError> {
        self.whitespace();
        if self.peek() == Some(expected) {
            self.advance(expected);
            Ok(())
        } else {
            Err(CoreError::ProjectionSyntax(format!(
                "expected '{expected}' at position {}",
                self.position
            )))
        }
    }

    fn quoted_string(&mut self) -> Result<String, CoreError> {
        self.whitespace();
        let quote = self
            .peek()
            .filter(|quote| matches!(quote, '\'' | '"'))
            .ok_or_else(|| {
                CoreError::ProjectionSyntax(format!(
                    "expected quoted source query at position {}",
                    self.position
                ))
            })?;
        self.advance(quote);
        let mut output = String::new();
        while let Some(character) = self.peek() {
            self.advance(character);
            if character == quote {
                if self.peek() == Some(quote) {
                    self.advance(quote);
                    output.push(quote);
                } else {
                    return Ok(output);
                }
            } else {
                output.push(character);
            }
        }
        Err(CoreError::ProjectionSyntax(
            "unterminated source query".to_owned(),
        ))
    }

    fn finish(&mut self) -> Result<(), CoreError> {
        self.whitespace();
        if self.position == self.input.len() {
            Ok(())
        } else {
            Err(CoreError::ProjectionSyntax(format!(
                "unexpected trailing input at position {}",
                self.position
            )))
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.position..)?.chars().next()
    }

    fn advance(&mut self, character: char) {
        self.position += character.len_utf8();
    }
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub definition: ProjectionDefinition,
    pub rows: Arc<Vec<Vec<Option<Cell>>>>,
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    projections: Arc<RwLock<HashMap<(String, String), Projection>>>,
}

impl Catalog {
    pub fn define(
        &self,
        database: &str,
        definition: ProjectionDefinition,
        rows: Vec<Vec<Option<Cell>>>,
    ) {
        self.projections
            .write()
            .expect("catalog write lock")
            .insert(
                projection_key(database, definition.name()),
                Projection {
                    definition,
                    rows: Arc::new(rows),
                },
            );
    }

    pub fn undefine(&self, database: &str, name: &str) -> bool {
        self.projections
            .write()
            .expect("catalog write lock")
            .remove(&projection_key(database, name))
            .is_some()
    }

    pub fn names(&self, database: &str) -> Vec<String> {
        let mut names = self
            .projections
            .read()
            .expect("catalog read lock")
            .iter()
            .filter(|((owner, _), _)| owner == database)
            .map(|(_, projection)| projection.definition.name().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn projection(&self, database: &str, name: &str) -> Option<Projection> {
        self.projections
            .read()
            .expect("catalog read lock")
            .get(&projection_key(database, name))
            .cloned()
    }
}

fn projection_key(database: &str, name: &str) -> (String, String) {
    (database.to_owned(), name.to_ascii_lowercase())
}

#[derive(Clone, Debug)]
pub struct ResultColumn {
    pub name: String,
    pub scalar_type: ScalarType,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Option<Cell>>>,
}

#[derive(Clone, Debug)]
pub struct QueryError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct SessionContext {
    pub database: String,
    pub user: String,
    pub server_port: u16,
}

pub fn execute_sql(
    catalog: &Catalog,
    sql: &str,
    session: &SessionContext,
) -> Result<QueryResult, QueryError> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|error| QueryError {
        code: "42601",
        message: format!("SQL syntax error: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(QueryError {
            code: "42601",
            message: "exactly one SQL statement is required".to_owned(),
        });
    }

    match statements.into_iter().next().expect("one statement") {
        Statement::Query(query) => execute_query(catalog, *query, session),
        other => Err(QueryError {
            code: "0A000",
            message: format!("unsupported SQL statement: {other}"),
        }),
    }
}

fn execute_query(
    catalog: &Catalog,
    query: sqlparser::ast::Query,
    session: &SessionContext,
) -> Result<QueryResult, QueryError> {
    if query.with.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return Err(unsupported("query modifiers"));
    }
    let order_by = match query.order_by {
        None => Vec::new(),
        Some(order_by) => match order_by.kind {
            OrderByKind::Expressions(expressions) => expressions
                .into_iter()
                .map(|expression| {
                    let column = match expression.expr {
                        Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
                        other => {
                            return Err(QueryError {
                                code: "0A000",
                                message: format!("unsupported ORDER BY expression: {other}"),
                            });
                        }
                    };
                    Ok((column, expression.options.asc != Some(false)))
                })
                .collect::<Result<Vec<_>, _>>()?,
            OrderByKind::All(_) => {
                return Err(QueryError {
                    code: "0A000",
                    message: "ORDER BY ALL is unsupported".to_owned(),
                });
            }
        },
    };

    let select = match *query.body {
        SetExpr::Select(select) => *select,
        other => {
            return Err(QueryError {
                code: "0A000",
                message: format!("unsupported query body: {other}"),
            });
        }
    };

    let grouped = !matches!(
        &select.group_by,
        sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    );
    if select.selection.is_some()
        || grouped
        || select.having.is_some()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.qualify.is_some()
    {
        return Err(unsupported("SELECT clauses"));
    }
    if select.from.is_empty() {
        if !order_by.is_empty() {
            return Err(unsupported("ORDER BY without FROM"));
        }
        return execute_expressions(&select.projection, session);
    }
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(QueryError {
            code: "0A000",
            message: "only one projection may appear in FROM".to_owned(),
        });
    }

    let table = match &select.from[0].relation {
        TableFactor::Table {
            name, args: None, ..
        } => {
            let parts = name
                .0
                .iter()
                .map(|part| match part {
                    ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
                    _ => Err(unsupported("projection name")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            match parts.as_slice() {
                [table] => table.to_ascii_lowercase(),
                [schema, table] if schema.eq_ignore_ascii_case("public") => {
                    table.to_ascii_lowercase()
                }
                [database, schema, table]
                    if database == &session.database && schema.eq_ignore_ascii_case("public") =>
                {
                    table.to_ascii_lowercase()
                }
                _ => return Err(unsupported("qualified relation")),
            }
        }
        other => {
            return Err(QueryError {
                code: "0A000",
                message: format!("unsupported table expression: {other}"),
            });
        }
    };

    let projection = catalog
        .projection(&session.database, &table)
        .ok_or_else(|| QueryError {
            code: "42P01",
            message: format!("relation \"{table}\" does not exist"),
        })?;

    let selected = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok(None),
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                Ok(Some((identifier.value.to_ascii_lowercase(), None)))
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(identifier),
                alias,
            } => Ok(Some((
                identifier.value.to_ascii_lowercase(),
                Some(alias.value.clone()),
            ))),
            other => Err(QueryError {
                code: "0A000",
                message: format!("unsupported SELECT item: {other}"),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let selected_indices = if selected.iter().any(Option::is_none) {
        if selected.len() != 1 {
            return Err(QueryError {
                code: "42601",
                message: "wildcard cannot be combined with named columns".to_owned(),
            });
        }
        projection
            .definition
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| (index, column.name.clone()))
            .collect::<Vec<_>>()
    } else {
        selected
            .into_iter()
            .map(|selected| {
                let (name, alias) = selected.expect("named selection");
                projection
                    .definition
                    .columns()
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(&name))
                    .map(|index| (index, alias.unwrap_or(name.clone())))
                    .ok_or_else(|| QueryError {
                        code: "42703",
                        message: format!("column \"{name}\" does not exist"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut row_indices = (0..projection.rows.len()).collect::<Vec<_>>();
    for (column, ascending) in order_by {
        let column_index = projection
            .definition
            .columns()
            .iter()
            .position(|candidate| candidate.name.eq_ignore_ascii_case(&column))
            .ok_or_else(|| QueryError {
                code: "42703",
                message: format!("column \"{column}\" does not exist"),
            })?;
        row_indices.sort_by(|left, right| {
            let ordering = compare_cells(
                projection.rows[*left][column_index].as_ref(),
                projection.rows[*right][column_index].as_ref(),
            );
            if ascending {
                ordering
            } else {
                ordering.reverse()
            }
        });
    }

    let columns = selected_indices
        .iter()
        .map(|(index, output_name)| ResultColumn {
            name: output_name.clone(),
            scalar_type: projection.definition.columns()[*index].scalar_type,
        })
        .collect();
    let rows = row_indices
        .into_iter()
        .map(|row_index| {
            selected_indices
                .iter()
                .map(|(column_index, _)| projection.rows[row_index][*column_index].clone())
                .collect()
        })
        .collect();
    Ok(QueryResult { columns, rows })
}

fn execute_expressions(
    projection: &[SelectItem],
    session: &SessionContext,
) -> Result<QueryResult, QueryError> {
    let mut columns = Vec::with_capacity(projection.len());
    let mut row = Vec::with_capacity(projection.len());
    for item in projection {
        let (expression, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => {
                return Err(QueryError {
                    code: "0A000",
                    message: format!("unsupported SELECT expression: {other}"),
                });
            }
        };

        let (default_name, value) = evaluate_expression(expression, session)?;
        columns.push(ResultColumn {
            name: alias.unwrap_or(default_name),
            scalar_type: ScalarType::String,
        });
        row.push(value.map(Cell::String));
    }
    Ok(QueryResult {
        columns,
        rows: vec![row],
    })
}

fn unsupported(what: &str) -> QueryError {
    QueryError {
        code: "0A000",
        message: format!("unsupported {what}"),
    }
}

fn evaluate_expression(
    expression: &Expr,
    session: &SessionContext,
) -> Result<(String, Option<String>), QueryError> {
    let rendered = expression.to_string();
    let lower = rendered.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "current_database()" => (
            "current_database".to_owned(),
            Some(session.database.clone()),
        ),
        "current_schema()" | "current_schema" => {
            ("current_schema".to_owned(), Some("public".to_owned()))
        }
        "current_user" | "session_user" | "user" => {
            ("current_user".to_owned(), Some(session.user.clone()))
        }
        "version()" => (
            "version".to_owned(),
            Some("PostgreSQL 16.13 (Facet PostgreSQL-compatible TypeDB sidecar)".to_owned()),
        ),
        "inet_server_addr()" => ("inet_server_addr".to_owned(), None),
        "inet_server_port()" => (
            "inet_server_port".to_owned(),
            Some(session.server_port.to_string()),
        ),
        _ => match expression {
            Expr::Value(value) => match &value.value {
                Value::SingleQuotedString(value) => (rendered, Some(value.clone())),
                Value::Number(value, _) => (rendered, Some(value.clone())),
                Value::Null => (rendered, None),
                _ => return Err(unsupported("literal")),
            },
            _ => return Err(unsupported("expression")),
        },
    })
}

fn compare_cells(left: Option<&Cell>, right: Option<&Cell>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(Cell::Boolean(left)), Some(Cell::Boolean(right))) => left.cmp(right),
        (Some(Cell::Integer(left)), Some(Cell::Integer(right))) => left.cmp(right),
        (Some(Cell::Double(left)), Some(Cell::Double(right))) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (Some(Cell::Decimal(left)), Some(Cell::Decimal(right))) => left.cmp(right),
        (Some(Cell::Date(left)), Some(Cell::Date(right))) => left.cmp(right),
        (Some(Cell::DateTime(left)), Some(Cell::DateTime(right))) => left.cmp(right),
        (Some(Cell::DateTimeTz(left)), Some(Cell::DateTimeTz(right))) => left.cmp(right),
        (Some(Cell::Duration(left)), Some(Cell::Duration(right))) => (
            left.months,
            left.days,
            left.nanoseconds,
        )
            .cmp(&(right.months, right.days, right.nanoseconds)),
        (Some(Cell::String(left)), Some(Cell::String(right))) => left.cmp(right),
        (Some(_), Some(_)) => Ordering::Equal,
    }
}
