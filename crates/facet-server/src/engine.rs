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
            Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
            Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray, StructArray,
            TimestampMicrosecondArray,
        },
        buffer::{NullBuffer, OffsetBuffer},
        compute::cast,
        datatypes::{DataType, Field, Fields, IntervalMonthDayNano, Schema},
        record_batch::RecordBatch,
    },
    catalog::{
        SchemaProvider, TableProvider,
        information_schema::{INFORMATION_SCHEMA, InformationSchemaProvider},
    },
    common::ParamValues,
    datasource::{MemTable, TableType, ViewTable, provider_as_source},
    error::{DataFusionError, Result as DfResult},
    execution::FunctionRegistry,
    logical_expr::{
        ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF,
        ScalarUDFImpl, Signature, Volatility, col, lit,
    },
    prelude::{DataFrame, SessionConfig, SessionContext as DfContext, create_udf},
    scalar::ScalarValue,
    sql::sqlparser::ast::{
        AccessExpr, CastKind, CopyLegacyOption, CopyOption, CopySource, CopyTarget,
        DataType as SqlDataType, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
        FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement,
        Subscript, TableFactor, Value as SqlValue, VisitMut, VisitorMut,
    },
};
use datafusion_postgres::{
    arrow_pg::{
        datatypes::{arrow_schema_to_pg_fields, df::deserialize_parameters, into_pg_type},
        encode_dataframe,
        encoder::encode_value,
    },
    datafusion_pg_catalog::{
        pg_catalog::{context::EmptyContextProvider, setup_pg_catalog},
        sql::PostgresCompatibilityParser,
    },
};
use facet_core::{Catalog, Cell, ScalarType, SessionContext, execute_sql};
use futures::stream;
use pgwire::{
    api::{
        Type,
        portal::{Format, Portal},
        results::{CopyEncoder, CopyResponse, FieldFormat, FieldInfo, Response, Tag},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
};

use crate::{encode_query_result, error_response, starts_projection_ddl};

/// A statement stored by the extended query protocol's Parse message.
#[derive(Clone, Debug)]
pub(crate) enum Prepared {
    Empty,
    ProjectionDdl(String),
    /// Session statements answered without DataFusion: transactions, SET,
    /// DISCARD, SHOW and COPY.
    Direct(Box<Statement>),
    /// `plan` describes the result and parameters; Execute plans `statement`
    /// again so it reads the projection rows that are current at that moment.
    Query {
        statement: Box<Statement>,
        plan: Box<LogicalPlan>,
    },
}

impl Prepared {
    pub(crate) fn parameter_types(&self) -> PgWireResult<Vec<Type>> {
        match self {
            Self::Query { plan, .. } => ordered_parameter_types(plan)?
                .into_iter()
                .map(|data_type| data_type.map_or(Ok(Type::UNKNOWN), |t| into_pg_type(&t)))
                .collect(),
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) fn result_fields(&self, format: Option<&Format>) -> PgWireResult<Vec<FieldInfo>> {
        let format = format.unwrap_or(&Format::UnifiedText);
        match self {
            Self::Query { plan, .. }
                if !matches!(plan.as_ref(), LogicalPlan::Ddl(_) | LogicalPlan::Dml(_)) =>
            {
                arrow_schema_to_pg_fields(plan.schema().as_arrow(), format, None)
            }
            Self::Direct(statement) => match statement.as_ref() {
                Statement::ShowVariable { variable } => Ok(vec![FieldInfo::new(
                    show_name(variable).replace(' ', "_"),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(0),
                )]),
                _ => Ok(Vec::new()),
            },
            _ => Ok(Vec::new()),
        }
    }
}

fn ordered_parameter_types(plan: &LogicalPlan) -> PgWireResult<Vec<Option<DataType>>> {
    let mut types = plan
        .get_parameter_types()
        .map_err(|error| user_error(datafusion_error_info(error)))?
        .into_iter()
        .collect::<Vec<_>>();
    types.sort_by_key(|(name, _)| {
        name.trim_start_matches('$')
            .parse::<u32>()
            .unwrap_or(u32::MAX)
    });
    Ok(types.into_iter().map(|(_, data_type)| data_type).collect())
}

fn user_error(info: ErrorInfo) -> PgWireError {
    PgWireError::UserError(Box::new(info))
}

fn show_name(variable: &[Ident]) -> String {
    variable
        .iter()
        .map(|part| part.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

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

    pub(crate) async fn prepare(
        &self,
        query: &str,
        session: &SessionContext,
    ) -> PgWireResult<Prepared> {
        let trimmed = query.trim();
        if starts_projection_ddl(trimmed) {
            return Ok(Prepared::ProjectionDdl(trimmed.to_owned()));
        }
        let mut statements = self.parser.parse(trimmed).map_err(|error| {
            user_error(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!("SQL syntax error: {error}"),
            ))
        })?;
        let statement = match statements.len() {
            0 => return Ok(Prepared::Empty),
            1 => statements.remove(0),
            _ => {
                return Err(user_error(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "cannot insert multiple commands into a prepared statement".to_owned(),
                )));
            }
        };
        if matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Set(_)
                | Statement::Discard { .. }
                | Statement::ShowVariable { .. }
                | Statement::Copy { .. }
        ) {
            return Ok(Prepared::Direct(Box::new(statement)));
        }
        let plan = self
            .plan(statement.clone(), session)
            .await
            .map_err(|error| user_error(datafusion_error_info(error)))?;
        Ok(Prepared::Query {
            statement: Box::new(statement),
            plan: Box::new(plan),
        })
    }

    pub(crate) async fn execute_prepared(
        &self,
        portal: &Portal<Prepared>,
        session: &SessionContext,
    ) -> PgWireResult<Response> {
        match &portal.statement.statement {
            Prepared::Empty => Ok(Response::EmptyQuery),
            Prepared::ProjectionDdl(_) => Err(PgWireError::ApiError(
                "projection DDL is executed by the query handler".into(),
            )),
            Prepared::Direct(statement) => {
                self.execute_statement(statement.as_ref().clone(), None, session)
                    .await
            }
            Prepared::Query { statement, plan } => {
                let types = ordered_parameter_types(plan)?;
                let types = types.iter().map(Option::as_ref).collect::<Vec<_>>();
                let parameters = deserialize_parameters(portal, &types)?;
                let frame = match self
                    .bound_dataframe(statement.as_ref().clone(), session, parameters)
                    .await
                {
                    Ok(frame) => frame,
                    Err(error) => return Ok(datafusion_error(error)),
                };
                match encode_dataframe(frame, &portal.result_column_format, None).await {
                    Ok(response) => Ok(Response::Query(response)),
                    Err(error) => Ok(error_response("XX000", error.to_string())),
                }
            }
        }
    }

    async fn bound_dataframe(
        &self,
        statement: Statement,
        session: &SessionContext,
        parameters: ParamValues,
    ) -> DfResult<DataFrame> {
        let plan = self
            .plan(statement, session)
            .await?
            .replace_params_with_values(&parameters)?;
        self.context(session)
            .await?
            .execute_logical_plan(plan)
            .await
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
                let name = show_name(variable);
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
        statement: Statement,
        session: &SessionContext,
    ) -> DfResult<DataFrame> {
        let plan = self.plan(statement, session).await?;
        self.context(session)
            .await?
            .execute_logical_plan(plan)
            .await
    }

    async fn plan(
        &self,
        mut statement: Statement,
        session: &SessionContext,
    ) -> DfResult<LogicalPlan> {
        let _ = statement.visit(&mut QualifyFacetCatalogTables);
        let _ = statement.visit(&mut NullDescriptions);
        let _ = statement.visit(&mut ExpandArrays);
        self.context(session)
            .await?
            .state()
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                statement,
            )))
            .await
    }

    async fn context(&self, session: &SessionContext) -> DfResult<Arc<DfContext>> {
        if let Some(context) = self
            .contexts
            .lock()
            .expect("DataFusion contexts")
            .get(&session.database)
        {
            return Ok(Arc::clone(context));
        }
        let context = Arc::new(self.build_context(session).await?);
        Ok(Arc::clone(
            self.contexts
                .lock()
                .expect("DataFusion contexts")
                .entry(session.database.clone())
                .or_insert(context),
        ))
    }

    /// DataFusion's built-in `information_schema` is replaced by
    /// [`FacetInformationSchema`] so PostgreSQL clients find the relations and
    /// columns they query during metadata sync.
    async fn build_context(&self, session: &SessionContext) -> DfResult<DfContext> {
        let config =
            SessionConfig::new().with_default_catalog_and_schema(&session.database, "public");
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
        let mut arrays = Vec::new();
        for (table, columns) in PG_CATALOG_ARRAY_COLUMNS {
            arrays.push((
                table,
                array_columns_view(&context, pg_catalog.as_ref(), table, columns).await?,
            ));
        }
        catalog.register_schema(
            "pg_catalog",
            Arc::new(FacetPgCatalog {
                inner: pg_catalog,
                arrays,
            }),
        )?;
        register_functions(&context, &self.catalog, session);
        let information_schema = FacetInformationSchema::new(&context).await?;
        catalog.register_schema(INFORMATION_SCHEMA, Arc::new(information_schema))?;
        Ok(context)
    }
}

/// How the upstream export spells a PostgreSQL array column as text.
#[derive(Clone, Copy)]
enum ArrayText {
    /// `int2[]` such as `{1,2}` or the upstream export's `[1, 2]`.
    Braced,
    /// `int2vector` such as `1 2`.
    Vector,
}

/// Catalog columns PostgreSQL types as `int2[]`/`int2vector`, which the
/// upstream export stores as text, so `attnum = ANY(conkey)` and
/// `_pg_expandarray(indkey)` plan as they do against PostgreSQL.
const PG_CATALOG_ARRAY_COLUMNS: [(&str, &[(&str, ArrayText)]); 2] = [
    (
        "pg_constraint",
        &[
            ("conkey", ArrayText::Braced),
            ("confkey", ArrayText::Braced),
        ],
    ),
    ("pg_index", &[("indkey", ArrayText::Vector)]),
];

async fn array_columns_view(
    context: &DfContext,
    pg_catalog: &dyn SchemaProvider,
    table_name: &str,
    arrays: &[(&str, ArrayText)],
) -> DfResult<Arc<dyn TableProvider>> {
    let table = pg_catalog.table(table_name).await?.ok_or_else(|| {
        DataFusionError::Configuration(format!("pg_catalog.{table_name} was not created"))
    })?;
    let columns = table
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            let array = arrays
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, text)| *text);
            match array {
                Some(ArrayText::Braced) if field.data_type() == &DataType::Utf8 => format!(
                    "CASE WHEN \"{name}\" IS NULL \
                     OR btrim(replace(\"{name}\", ' ', ''), '{{}}[]') = '' THEN NULL \
                     ELSE CAST(string_to_array(btrim(replace(\"{name}\", ' ', ''), \
                     '{{}}[]'), ',') AS SMALLINT[]) END AS \"{name}\""
                ),
                Some(ArrayText::Vector) if field.data_type() == &DataType::Utf8 => format!(
                    "CASE WHEN \"{name}\" IS NULL OR btrim(\"{name}\") = '' THEN NULL \
                     ELSE CAST(string_to_array(btrim(\"{name}\"), ' ') AS SMALLINT[]) \
                     END AS \"{name}\""
                ),
                _ => format!("\"{name}\""),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let plan = context
        .state()
        .create_logical_plan(&format!("SELECT {columns} FROM pg_catalog.{table_name}"))
        .await?;
    Ok(Arc::new(ViewTable::new(plan, None)))
}

fn register_functions(context: &DfContext, catalog: &Catalog, session: &SessionContext) {
    context.register_udf(ScalarUDF::new_from_impl(ExpandArrayUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));
    if let Ok(quote_ident) = context.udf("quote_ident") {
        context.register_udf(ScalarUDF::new_from_impl(FormatUdf {
            signature: Signature::variadic_any(Volatility::Immutable),
            quote_ident,
        }));
    }
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

/// PostgreSQL `format(formatstr, ...)` with `%s`, `%I`, `%L` and `%%`.
/// `%I` delegates to datafusion-pg-catalog's `quote_ident`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatUdf {
    signature: Signature,
    quote_ident: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for FormatUdf {
    fn name(&self) -> &str {
        "format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = args.number_rows;
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let texts = args
            .args
            .iter()
            .map(|value| {
                let array = value.to_array(rows)?;
                let text = cast(&array, &DataType::Utf8)?;
                text.as_any()
                    .downcast_ref::<StringArray>()
                    .cloned()
                    .ok_or_else(|| DataFusionError::Internal("format cast to text".to_owned()))
            })
            .collect::<DfResult<Vec<StringArray>>>()?;
        let Some((template, values)) = texts.split_first() else {
            return Err(DataFusionError::Plan(
                "function format requires at least one argument".to_owned(),
            ));
        };
        let mut idents = Vec::with_capacity(values.len());
        for value in values {
            let quoted = self
                .quote_ident
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::new(value.clone()))],
                    arg_fields: vec![Arc::new(Field::new("ident", DataType::Utf8, true))],
                    number_rows: rows,
                    return_field: Arc::new(Field::new("quote_ident", DataType::Utf8, true)),
                    config_options: Arc::clone(&args.config_options),
                })?
                .to_array(rows)?;
            idents.push(cast(&quoted, &DataType::Utf8)?);
        }
        let idents = idents
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal("quote_ident returned non-text".into())
                })
            })
            .collect::<DfResult<Vec<_>>>()?;
        let mut output = Vec::with_capacity(rows);
        for row in 0..rows {
            if template.is_null(row) {
                output.push(None);
                continue;
            }
            let mut result = String::new();
            let mut next = 0;
            let mut chars = template.value(row).chars();
            while let Some(character) = chars.next() {
                if character != '%' {
                    result.push(character);
                    continue;
                }
                let specifier = chars.next().ok_or_else(|| {
                    DataFusionError::Execution("unterminated format() type specifier".to_owned())
                })?;
                if specifier == '%' {
                    result.push('%');
                    continue;
                }
                let value = values.get(next).ok_or_else(|| {
                    DataFusionError::Execution("too few arguments for format()".to_owned())
                })?;
                let present = !value.is_null(row);
                match specifier {
                    's' if present => result.push_str(value.value(row)),
                    's' => {}
                    'I' if present => result.push_str(idents[next].value(row)),
                    'I' => {
                        return Err(DataFusionError::Execution(
                            "null values cannot be formatted as an SQL identifier".to_owned(),
                        ));
                    }
                    'L' if present => {
                        result.push('\'');
                        result.push_str(&value.value(row).replace('\'', "''"));
                        result.push('\'');
                    }
                    'L' => result.push_str("NULL"),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "unrecognized format() type specifier \"{other}\""
                        )));
                    }
                }
                next += 1;
            }
            output.push(Some(result));
        }
        let array = StringArray::from(output);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(array)))
        }
    }
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
    Response::Error(Box::new(datafusion_error_info(error)))
}

fn datafusion_error_info(error: DataFusionError) -> ErrorInfo {
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
    ErrorInfo::new("ERROR".to_owned(), code.to_owned(), message)
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

/// Facet stores no comments, so `col_description`/`obj_description` are
/// `NULL::text`. Rewriting the whole call also drops its `regclass` argument,
/// which clients build with `format(...)::regclass` from non-literal names.
struct NullDescriptions;

impl VisitorMut for NullDescriptions {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<()> {
        if let SqlExpr::Function(function) = expr
            && let Some(ObjectNamePart::Identifier(ident)) = function.name.0.last()
            && ["col_description", "obj_description", "shobj_description"]
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&ident.value))
        {
            *expr = SqlExpr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(SqlExpr::Value(SqlValue::Null.into())),
                data_type: SqlDataType::Text,
                array: false,
                format: None,
            };
        }
        ControlFlow::Continue(())
    }
}

/// PostgreSQL's `information_schema._pg_expandarray(a)` is a set-returning
/// function of `(x, n)` records: each element with its 1-based position.
/// It becomes `unnest(facet_pg_expandarray(a))` over a list of such structs;
/// several `unnest`s in one projection zip as PostgreSQL's SRFs do. Record
/// field access `(r).x` becomes the struct subscript `(r)['x']`, which is the
/// form DataFusion plans for non-identifier roots.
struct ExpandArrays;

impl VisitorMut for ExpandArrays {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<()> {
        match expr {
            SqlExpr::Function(function)
                if function.name.0.last().is_some_and(|part| {
                    matches!(part, ObjectNamePart::Identifier(name)
                        if name.value.eq_ignore_ascii_case("_pg_expandarray"))
                }) =>
            {
                let mut inner = function.clone();
                inner.name = ObjectName::from(vec![Ident::new(EXPAND_ARRAY_UDF)]);
                let mut unnest = function.clone();
                unnest.name = ObjectName::from(vec![Ident::new("unnest")]);
                unnest.args = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        SqlExpr::Function(inner),
                    ))],
                    clauses: Vec::new(),
                });
                *expr = SqlExpr::Function(unnest);
            }
            SqlExpr::CompoundFieldAccess { root, access_chain }
                if !matches!(root.as_ref(), SqlExpr::Identifier(_)) =>
            {
                for access in access_chain.iter_mut() {
                    if let AccessExpr::Dot(SqlExpr::Identifier(field)) = access {
                        let name = if field.quote_style.is_some() {
                            field.value.clone()
                        } else {
                            field.value.to_lowercase()
                        };
                        *access = AccessExpr::Subscript(Subscript::Index {
                            index: SqlExpr::value(SqlValue::SingleQuotedString(name)),
                        });
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

const EXPAND_ARRAY_UDF: &str = "facet_pg_expandarray";

/// `facet_pg_expandarray(list)` returns the list's elements as `{x, n}`
/// structs, `n` being each element's 1-based position.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExpandArrayUdf {
    signature: Signature,
}

impl ExpandArrayUdf {
    fn element(arg_types: &[DataType]) -> DfResult<Field> {
        match arg_types {
            [DataType::List(field)] => Ok(Field::new("x", field.data_type().clone(), true)),
            [DataType::Null] => Ok(Field::new("x", DataType::Int16, true)),
            other => Err(DataFusionError::Plan(format!(
                "function _pg_expandarray(anyarray) does not accept {other:?}"
            ))),
        }
    }

    fn struct_fields(arg_types: &[DataType]) -> DfResult<Fields> {
        Ok(Fields::from(vec![
            Self::element(arg_types)?,
            Field::new("n", DataType::Int32, false),
        ]))
    }
}

impl ScalarUDFImpl for ExpandArrayUdf {
    fn name(&self) -> &str {
        EXPAND_ARRAY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::new_list(
            DataType::Struct(Self::struct_fields(arg_types)?),
            true,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0].to_array(args.number_rows)?;
        let arg_types = [array.data_type().clone()];
        let fields = Self::struct_fields(&arg_types)?;
        let (offsets, values, nulls) = match array.as_any().downcast_ref::<ListArray>() {
            Some(list) => (
                list.offsets().clone(),
                Arc::clone(list.values()),
                list.nulls().cloned(),
            ),
            None => (
                OffsetBuffer::new_zeroed(array.len()),
                datafusion::arrow::array::new_empty_array(fields[0].data_type()),
                Some(NullBuffer::new_null(array.len())),
            ),
        };
        let mut positions = vec![0_i32; values.len()];
        for window in offsets.windows(2) {
            let (start, stop) = (usize::try_from(window[0]), usize::try_from(window[1]));
            let (Ok(start), Ok(stop)) = (start, stop) else {
                return Err(DataFusionError::Internal("negative list offset".to_owned()));
            };
            for (position, slot) in positions[start..stop].iter_mut().enumerate() {
                *slot = i32::try_from(position + 1)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
        }
        let records = StructArray::try_new(
            fields.clone(),
            vec![values, Arc::new(Int32Array::from(positions))],
            None,
        )?;
        let output = ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Struct(fields), true)),
            offsets,
            Arc::new(records),
            nulls,
        )?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&output, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(output))
        })
    }
}
/// datafusion-pg-catalog's schema plus the relations it does not provide.
/// Projections have no indexes, so `pg_indexes` is empty, and no heap pages,
/// so `pg_class.relpages` is 0 instead of upstream's placeholder 1. DuckDB
/// would otherwise scan with `ctid` ranges that Facet relations cannot serve.
#[derive(Debug)]
struct FacetPgCatalog {
    inner: Arc<dyn SchemaProvider>,
    arrays: Vec<(&'static str, Arc<dyn TableProvider>)>,
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
        if let Some((_, table)) = self
            .arrays
            .iter()
            .find(|(table, _)| table.eq_ignore_ascii_case(name))
        {
            return Ok(Some(Arc::clone(table)));
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

/// PostgreSQL's `information_schema` for metadata-syncing clients. DataFusion's
/// own relations are delegated; `columns` is PostgreSQL's view over
/// `pg_catalog` (with `udt_name`, `is_identity`, `is_generated`), and the
/// constraint relations are empty because projections declare no keys.
#[derive(Debug)]
struct FacetInformationSchema {
    builtin: InformationSchemaProvider,
    columns: Arc<dyn TableProvider>,
}

const FACET_INFORMATION_SCHEMA_CONSTRAINTS: [(&str, &[&str]); 2] = [
    (
        "table_constraints",
        &[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_type",
            "is_deferrable",
            "initially_deferred",
            "enforced",
            "nulls_distinct",
        ],
    ),
    (
        "key_column_usage",
        &[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "position_in_unique_constraint",
        ],
    ),
];

const INFORMATION_SCHEMA_COLUMNS_SQL: &str = "SELECT \
    current_database() AS table_catalog, \
    n.nspname AS table_schema, \
    c.relname AS table_name, \
    a.attname AS column_name, \
    CAST(a.attnum AS INT) AS ordinal_position, \
    CAST(NULL AS TEXT) AS column_default, \
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, \
    format_type(a.atttypid, a.atttypmod) AS data_type, \
    current_database() AS udt_catalog, \
    'pg_catalog' AS udt_schema, \
    t.typname AS udt_name, \
    'NO' AS is_identity, \
    'NEVER' AS is_generated \
    FROM pg_catalog.pg_attribute a \
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid \
    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid \
    WHERE a.attnum > 0";

impl FacetInformationSchema {
    async fn new(context: &DfContext) -> DfResult<Self> {
        let plan = context
            .state()
            .create_logical_plan(INFORMATION_SCHEMA_COLUMNS_SQL)
            .await?;
        Ok(Self {
            builtin: InformationSchemaProvider::new(context.state().catalog_list().clone()),
            columns: Arc::new(ViewTable::new(plan, None)),
        })
    }

    fn constraint_table(name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some((_, columns)) = FACET_INFORMATION_SCHEMA_CONSTRAINTS
            .iter()
            .find(|(table, _)| table.eq_ignore_ascii_case(name))
        else {
            return Ok(None);
        };
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| {
                    let data_type = if column.contains("position") {
                        DataType::Int32
                    } else {
                        DataType::Utf8
                    };
                    Field::new(*column, data_type, true)
                })
                .collect::<Vec<_>>(),
        ));
        Ok(Some(Arc::new(MemTable::try_new(schema, vec![vec![]])?)))
    }
}

#[async_trait]
impl SchemaProvider for FacetInformationSchema {
    fn table_names(&self) -> Vec<String> {
        let mut names = self.builtin.table_names();
        names.extend(
            FACET_INFORMATION_SCHEMA_CONSTRAINTS
                .iter()
                .map(|(table, _)| (*table).to_owned()),
        );
        names
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("columns") {
            return Ok(Some(Arc::clone(&self.columns)));
        }
        if let Some(table) = Self::constraint_table(name)? {
            return Ok(Some(table));
        }
        self.builtin.table(name).await
    }

    async fn table_type(&self, name: &str) -> DfResult<Option<TableType>> {
        Ok(self.table_exist(name).then_some(TableType::View))
    }

    fn table_exist(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("columns")
            || FACET_INFORMATION_SCHEMA_CONSTRAINTS
                .iter()
                .any(|(table, _)| table.eq_ignore_ascii_case(name))
            || self.builtin.table_exist(name)
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
