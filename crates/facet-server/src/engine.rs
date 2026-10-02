/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! General SQL engine for statements outside Facet's byte-exact fast path.
//!
//! Projections are exposed to DataFusion through a live `SchemaProvider`, and
//! the PostgreSQL catalog plus client-compatibility rewrites come from
//! `datafusion-pg-catalog` (`setup_pg_catalog`, `PostgresCompatibilityParser`).

use std::{
    collections::HashMap,
    fmt::Debug,
    ops::ControlFlow,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use chrono::NaiveDate;
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
            IntervalMonthDayNanoArray, StringArray, TimestampMicrosecondArray,
        },
        datatypes::{DataType, Field, IntervalMonthDayNano, Schema},
        record_batch::RecordBatch,
    },
    catalog::{SchemaProvider, TableProvider},
    datasource::{MemTable, ViewTable, provider_as_source},
    error::{DataFusionError, Result as DfResult},
    logical_expr::{ColumnarValue, Expr, LogicalPlanBuilder, Volatility, col, lit},
    prelude::{DataFrame, SessionConfig, SessionContext as DfContext, create_udf},
    scalar::ScalarValue,
    sql::sqlparser::ast::{
        CopyLegacyOption, CopyOption, CopySource, CopyTarget, Ident, ObjectName, ObjectNamePart,
        Statement, TableFactor, VisitMut, VisitorMut,
    },
};
use datafusion_postgres::{
    arrow_pg::{datatypes::into_pg_type, encode_dataframe, encoder::encode_value},
    datafusion_pg_catalog::{
        pg_catalog::{context::EmptyContextProvider, setup_pg_catalog},
        sql::PostgresCompatibilityParser,
    },
};
use facet_core::{Catalog, Cell, ScalarType, SessionContext, execute_sql};
use futures::stream;
use pgwire::{
    api::{
        portal::Format,
        results::{CopyEncoder, CopyResponse, FieldFormat, FieldInfo, Response, Tag},
    },
    error::PgWireResult,
};

use crate::{encode_query_result, error_response};

pub(crate) struct Engine {
    catalog: Catalog,
    parser: PostgresCompatibilityParser,
    contexts: StdMutex<HashMap<String, Arc<DfContext>>>,
}

impl Engine {
    pub(crate) fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            parser: PostgresCompatibilityParser::new(),
            contexts: StdMutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn execute(
        &self,
        query: &str,
        session: &SessionContext,
    ) -> PgWireResult<Vec<Response>> {
        let statements = match self.parser.parse(query) {
            Ok(statements) => statements,
            Err(error) => {
                return Ok(vec![error_response(
                    "42601",
                    format!("SQL syntax error: {error}"),
                )]);
            }
        };
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let single = statements.len() == 1;
        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            let response = self
                .execute_statement(statement, single.then_some(query), session)
                .await?;
            let failed = matches!(response, Response::Error(_));
            responses.push(response);
            if failed {
                break;
            }
        }
        Ok(responses)
    }

    async fn execute_statement(
        &self,
        statement: Statement,
        original: Option<&str>,
        session: &SessionContext,
    ) -> PgWireResult<Response> {
        match &statement {
            Statement::StartTransaction { .. } => {
                return Ok(Response::TransactionStart(Tag::new("BEGIN")));
            }
            Statement::Commit { .. } => return Ok(Response::TransactionEnd(Tag::new("COMMIT"))),
            Statement::Rollback { .. } => {
                return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
            }
            Statement::Set(_) => return Ok(Response::Execution(Tag::new("SET"))),
            Statement::Discard { .. } => {
                return Ok(Response::Execution(Tag::new("DISCARD ALL")));
            }
            Statement::ShowVariable { variable } => {
                let name = variable
                    .iter()
                    .map(|part| part.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                return Ok(match show_value(&name, session) {
                    Some(value) => encode_query_result(facet_core::QueryResult {
                        columns: vec![facet_core::ResultColumn {
                            name: name.replace(' ', "_"),
                            scalar_type: ScalarType::String,
                        }],
                        rows: vec![vec![Some(Cell::String(value))]],
                    })?,
                    None => error_response(
                        "42704",
                        format!("unrecognized configuration parameter \"{name}\""),
                    ),
                });
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                ..
            } => {
                let binary = options.iter().any(|option| {
                    matches!(option, CopyOption::Format(format)
                        if format.value.eq_ignore_ascii_case("binary"))
                }) || legacy_options
                    .iter()
                    .any(|option| matches!(option, CopyLegacyOption::Binary));
                return Ok(match (source, to, target) {
                    (CopySource::Query(query), true, CopyTarget::Stdout) if binary => {
                        let statement = Statement::Query(query.clone());
                        match self.dataframe(statement, session).await {
                            Ok(frame) => copy_binary(frame).await?,
                            Err(error) => datafusion_error(error),
                        }
                    }
                    _ => error_response(
                        "0A000",
                        "only COPY (query) TO STDOUT in binary format is supported".to_owned(),
                    ),
                });
            }
            _ => {}
        }

        if matches!(statement, Statement::Query(_)) {
            let text = original.map_or_else(|| statement.to_string(), str::to_owned);
            if let Ok(result) = execute_sql(&self.catalog, text.trim(), session) {
                return encode_query_result(result);
            }
        }

        match self.dataframe(statement, session).await {
            Ok(frame) => match encode_dataframe(frame, &Format::UnifiedText, None).await {
                Ok(response) => Ok(Response::Query(response)),
                Err(error) => Ok(error_response("XX000", error.to_string())),
            },
            Err(error) => Ok(datafusion_error(error)),
        }
    }

    async fn dataframe(
        &self,
        mut statement: Statement,
        session: &SessionContext,
    ) -> DfResult<DataFrame> {
        let _ = statement.visit(&mut QualifyFacetCatalogTables);
        let context = self.context(session)?;
        let plan = context
            .state()
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                statement,
            )))
            .await?;
        context.execute_logical_plan(plan).await
    }

    fn context(&self, session: &SessionContext) -> DfResult<Arc<DfContext>> {
        let mut contexts = self.contexts.lock().expect("DataFusion contexts");
        if let Some(context) = contexts.get(&session.database) {
            return Ok(Arc::clone(context));
        }
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(&session.database, "public");
        let context = DfContext::new_with_config(config);
        let catalog = context.catalog(&session.database).ok_or_else(|| {
            DataFusionError::Configuration(format!("catalog {} was not created", session.database))
        })?;
        catalog.register_schema(
            "public",
            Arc::new(FacetSchema {
                catalog: self.catalog.clone(),
                database: session.database.clone(),
            }),
        )?;
        setup_pg_catalog(&context, &session.database, EmptyContextProvider)
            .map_err(|error| *error)?;
        let pg_catalog = catalog.schema("pg_catalog").ok_or_else(|| {
            DataFusionError::Configuration("pg_catalog schema was not created".to_owned())
        })?;
        catalog.register_schema("pg_catalog", Arc::new(FacetPgCatalog { inner: pg_catalog }))?;
        register_functions(&context, &self.catalog, session);
        let context = Arc::new(context);
        contexts.insert(session.database.clone(), Arc::clone(&context));
        Ok(context)
    }
}

fn register_functions(context: &DfContext, catalog: &Catalog, session: &SessionContext) {
    let version = ScalarValue::Utf8(Some(
        "PostgreSQL 16.13 (Facet PostgreSQL-compatible TypeDB sidecar)".to_owned(),
    ));
    context.register_udf(create_udf(
        "version",
        vec![],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(move |_| Ok(ColumnarValue::Scalar(version.clone()))),
    ));
    context.register_udf(create_udf(
        "inet_server_addr",
        vec![],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))),
    ));
    let port = i32::from(session.server_port);
    context.register_udf(create_udf(
        "inet_server_port",
        vec![],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(port))))),
    ));
    let catalog = catalog.clone();
    let database = session.database.clone();
    context.register_udf(create_udf(
        "to_regclass",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(move |arguments| {
            let resolve = |name: Option<&str>| {
                name.and_then(|name| {
                    let table = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
                    catalog
                        .projection(&database, table)
                        .map(|projection| projection.definition.name().to_owned())
                })
            };
            Ok(match &arguments[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(name)) => {
                    ColumnarValue::Scalar(ScalarValue::Utf8(resolve(name.as_deref())))
                }
                ColumnarValue::Array(array) => {
                    let names = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("to_regclass expects text".to_owned())
                        })?;
                    ColumnarValue::Array(Arc::new(
                        names.iter().map(resolve).collect::<StringArray>(),
                    ))
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "to_regclass expects text, got {other:?}"
                    )));
                }
            })
        }),
    ));
}

fn show_value(name: &str, session: &SessionContext) -> Option<String> {
    Some(
        match name {
            "server_version" => "16.13",
            "server_encoding" | "client_encoding" => "UTF8",
            "standard_conforming_strings" => "on",
            "timezone" | "time zone" => "UTC",
            "datestyle" => "ISO, MDY",
            "intervalstyle" => "postgres",
            "search_path" => "\"$user\", public",
            "transaction isolation level" | "transaction_isolation" => "read committed",
            "transaction_read_only" => "off",
            "max_identifier_length" => "63",
            "is_superuser" => "off",
            "session_authorization" => return Some(session.user.clone()),
            _ => return None,
        }
        .to_owned(),
    )
}

fn datafusion_error(error: DataFusionError) -> Response {
    let message = error.strip_backtrace();
    let code = match error.find_root() {
        DataFusionError::SchemaError(..) => "42703",
        DataFusionError::SQL(..) => "42601",
        DataFusionError::NotImplemented(_) => "0A000",
        DataFusionError::Plan(plan) if plan.starts_with("table") && plan.contains("not found") => {
            "42P01"
        }
        _ => "XX000",
    };
    error_response(code, message)
}

async fn copy_binary(frame: DataFrame) -> PgWireResult<Response> {
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let fields = Arc::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                Ok(FieldInfo::new(
                    field.name().clone(),
                    None,
                    None,
                    into_pg_type(field.data_type())?,
                    FieldFormat::Binary,
                ))
            })
            .collect::<PgWireResult<Vec<_>>>()?,
    );
    let batches = match frame.collect().await {
        Ok(batches) => batches,
        Err(error) => return Ok(datafusion_error(error)),
    };
    let mut encoder = CopyEncoder::new_binary(Arc::clone(&fields));
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            for (column, array) in batch.columns().iter().enumerate() {
                encode_value(
                    &mut encoder,
                    array,
                    row,
                    schema.field(column),
                    &fields[column],
                )?;
            }
            rows.push(Ok(encoder.take_copy()));
        }
    }
    Ok(Response::CopyOut(CopyResponse::new(
        1,
        fields.len(),
        stream::iter(rows),
    )))
}

const FACET_PG_CATALOG_TABLES: [&str; 1] = ["pg_indexes"];

/// Mirrors datafusion-pg-catalog's `PrependUnqualifiedPgTableName` for the
/// catalog relations Facet adds, because PostgreSQL searches `pg_catalog` first.
struct QualifyFacetCatalogTables;

impl VisitorMut for QualifyFacetCatalogTables {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name, args: None, ..
        } = factor
            && let [ObjectNamePart::Identifier(ident)] = name.0.as_slice()
            && FACET_PG_CATALOG_TABLES
                .iter()
                .any(|table| table.eq_ignore_ascii_case(&ident.value))
        {
            *name = ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("pg_catalog")),
                name.0[0].clone(),
            ]);
        }
        ControlFlow::Continue(())
    }
}

/// datafusion-pg-catalog's schema plus the relations it does not provide.
/// Projections have no indexes, so `pg_indexes` is empty, and no heap pages,
/// so `pg_class.relpages` is 0 instead of upstream's placeholder 1. DuckDB
/// would otherwise scan with `ctid` ranges that Facet relations cannot serve.
#[derive(Debug)]
struct FacetPgCatalog {
    inner: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl SchemaProvider for FacetPgCatalog {
    fn table_names(&self) -> Vec<String> {
        let mut names = self.inner.table_names();
        names.extend(FACET_PG_CATALOG_TABLES.map(str::to_owned));
        names
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("pg_indexes") {
            let schema = Arc::new(Schema::new(
                [
                    "schemaname",
                    "tablename",
                    "indexname",
                    "tablespace",
                    "indexdef",
                ]
                .map(|column| Field::new(column, DataType::Utf8, true))
                .to_vec(),
            ));
            return Ok(Some(Arc::new(MemTable::try_new(schema, vec![vec![]])?)));
        }
        let table = self.inner.table(name).await?;
        match table {
            Some(pg_class) if name.eq_ignore_ascii_case("pg_class") => {
                let columns = pg_class
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        if field.name() == "relpages" {
                            lit(0_i32).alias("relpages")
                        } else {
                            col(field.name())
                        }
                    })
                    .collect::<Vec<Expr>>();
                let plan =
                    LogicalPlanBuilder::scan("pg_class", provider_as_source(pg_class), None)?
                        .project(columns)?
                        .build()?;
                Ok(Some(Arc::new(ViewTable::new(plan, None))))
            }
            table => Ok(table),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("pg_indexes") || self.inner.table_exist(name)
    }
}

#[derive(Debug)]
struct FacetSchema {
    catalog: Catalog,
    database: String,
}

#[async_trait]
impl SchemaProvider for FacetSchema {
    fn table_names(&self) -> Vec<String> {
        self.catalog
            .names(&self.database)
            .into_iter()
            .map(|name| name.to_ascii_lowercase())
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some(projection) = self.catalog.projection(&self.database, name) else {
            return Ok(None);
        };
        let columns = projection.definition.columns();
        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        for (index, column) in columns.iter().enumerate() {
            let cells = projection
                .rows
                .iter()
                .map(|row| row[index].as_ref())
                .collect::<Vec<_>>();
            let array = cells_to_array(column.scalar_type, &cells)?;
            fields.push(Field::new(
                column.name.to_ascii_lowercase(),
                array.data_type().clone(),
                true,
            ));
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        Ok(Some(Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch]],
        )?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.catalog.projection(&self.database, name).is_some()
    }
}

fn mismatch(scalar_type: ScalarType, cell: &Cell) -> DataFusionError {
    DataFusionError::Internal(format!("cell {cell:?} does not match {scalar_type:?}"))
}

fn cells_to_array(scalar_type: ScalarType, cells: &[Option<&Cell>]) -> DfResult<ArrayRef> {
    macro_rules! collect {
        ($array:ty, $variant:ident, $convert:expr) => {{
            let values = cells
                .iter()
                .map(|cell| match cell {
                    None => Ok(None),
                    Some(Cell::$variant(value)) => $convert(value).map(Some),
                    Some(other) => Err(mismatch(scalar_type, other)),
                })
                .collect::<DfResult<Vec<_>>>()?;
            <$array>::from(values)
        }};
    }
    let array: ArrayRef = match scalar_type {
        ScalarType::Boolean => Arc::new(collect!(BooleanArray, Boolean, |v: &bool| Ok(*v))),
        ScalarType::Integer => Arc::new(collect!(Int64Array, Integer, |v: &i64| Ok(*v))),
        ScalarType::Double => Arc::new(collect!(Float64Array, Double, |v: &f64| Ok(*v))),
        ScalarType::String => Arc::new(collect!(StringArray, String, |v: &String| Ok(v.clone()))),
        ScalarType::Date => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            Arc::new(collect!(Date32Array, Date, |v: &NaiveDate| {
                i32::try_from((*v - epoch).num_days())
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            }))
        }
        ScalarType::DateTime => Arc::new(collect!(
            TimestampMicrosecondArray,
            DateTime,
            |v: &chrono::NaiveDateTime| Ok(v.and_utc().timestamp_micros())
        )),
        ScalarType::DateTimeTz => Arc::new(
            collect!(
                TimestampMicrosecondArray,
                DateTimeTz,
                |v: &chrono::DateTime<chrono::FixedOffset>| Ok(v.timestamp_micros())
            )
            .with_timezone("+00:00"),
        ),
        ScalarType::Duration => Arc::new(collect!(
            IntervalMonthDayNanoArray,
            Duration,
            |v: &facet_core::DurationValue| {
                let convert =
                    |error: std::num::TryFromIntError| DataFusionError::External(Box::new(error));
                Ok(IntervalMonthDayNano::new(
                    i32::try_from(v.months).map_err(convert)?,
                    i32::try_from(v.days).map_err(convert)?,
                    i64::try_from(v.nanoseconds).map_err(convert)?,
                ))
            }
        )),
        ScalarType::Decimal => {
            let scale = cells
                .iter()
                .filter_map(|cell| match cell {
                    Some(Cell::Decimal(value)) => Some(value.scale()),
                    _ => None,
                })
                .max()
                .unwrap_or(0);
            let array = collect!(Decimal128Array, Decimal, |v: &rust_decimal::Decimal| {
                10_i128
                    .checked_pow(scale - v.scale())
                    .and_then(|factor| v.mantissa().checked_mul(factor))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!("decimal {v} overflows NUMERIC(38)"))
                    })
            });
            Arc::new(array.with_precision_and_scale(
                38,
                i8::try_from(scale).map_err(|error| DataFusionError::External(Box::new(error)))?,
            )?)
        }
    };
    Ok(array)
}
