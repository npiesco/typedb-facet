/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Loopback PostgreSQL-wire server for Facet.

#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    env,
    fmt::Debug,
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use facet_core::{
    Catalog, CoreError, ProjectionStatement, QueryError, QueryResult, ScalarType, SessionContext,
    execute_sql, parse_projection_ddl,
};
use facet_state::{PersistedProjection, StateStore};
use facet_typedb::TypeDbClient;
use futures::{Sink, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, PgWireServerHandlers, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            sasl::{
                SASLAuthStartupHandler,
                scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password},
            },
        },
        query::SimpleQueryHandler,
        results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, data::DataRow},
    tokio::process_socket,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    typedb: TypeDbConfig,
    postgres: PostgresConfig,
    state: StateConfig,
    #[serde(default)]
    refresh: Option<RefreshConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct TypeDbConfig {
    url: String,
    username: String,
    password_env: String,
}

#[derive(Clone, Debug, Deserialize)]
struct PostgresConfig {
    listen: SocketAddr,
    username: String,
    password_env: String,
}

#[derive(Clone, Debug, Deserialize)]
struct StateConfig {
    path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
struct RefreshConfig {
    interval: Option<String>,
}

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

fn parse_interval(input: &str) -> Result<Duration, ServerError> {
    let invalid = || ServerError::InvalidRefreshInterval(input.to_owned());
    let trimmed = input.trim();
    let split = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(invalid)?;
    let (number, unit) = trimmed.split_at(split);
    let number: u64 = number.parse().map_err(|_| invalid())?;
    let interval = match unit {
        "ms" => Duration::from_millis(number),
        "s" => Duration::from_secs(number),
        "m" => Duration::from_secs(number.checked_mul(60).ok_or_else(invalid)?),
        _ => return Err(invalid()),
    };
    if interval.is_zero() {
        return Err(invalid());
    }
    Ok(interval)
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("could not read Facet config: {0}")]
    ReadConfig(#[from] io::Error),
    #[error("could not parse Facet config: {0}")]
    ParseConfig(#[from] toml::de::Error),
    #[error("Facet Stage 1 only permits loopback PostgreSQL listeners, got {0}")]
    NonLoopback(SocketAddr),
    #[error("required secret environment variable {0} is not set")]
    MissingSecret(String),
    #[error("could not initialize TypeDB HTTP client: {0}")]
    TypeDb(#[from] facet_typedb::TypeDbError),
    #[error("could not use Facet state: {0}")]
    State(#[from] facet_state::StateError),
    #[error("invalid refresh interval {0:?}; expected a positive integer with ms, s or m")]
    InvalidRefreshInterval(String),
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self, ServerError> {
        let input = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&input)?;
        if !config.postgres.listen.ip().is_loopback() {
            return Err(ServerError::NonLoopback(config.postgres.listen));
        }
        config.refresh_interval()?;
        Ok(config)
    }

    fn refresh_interval(&self) -> Result<Duration, ServerError> {
        self.refresh
            .as_ref()
            .and_then(|refresh| refresh.interval.as_deref())
            .map_or(Ok(DEFAULT_REFRESH_INTERVAL), parse_interval)
    }
}

pub async fn serve(config: Config) -> Result<(), ServerError> {
    let refresh_interval = config.refresh_interval()?;
    let typedb_password = read_secret(&config.typedb.password_env)?;
    let postgres_password = read_secret(&config.postgres.password_env)?;
    let typedb = TypeDbClient::new(config.typedb.url, config.typedb.username, typedb_password)?;
    let state = StateStore::open(config.state.path)?;
    let catalog = Catalog::default();
    for persisted in state.load_all()? {
        let rows = typedb
            .materialize(&persisted.database, &persisted.definition)
            .await?;
        catalog.define(&persisted.database, persisted.definition, rows);
    }
    let listener = TcpListener::bind(config.postgres.listen).await?;
    let address = listener.local_addr()?;
    let handler = Arc::new(QueryHandler {
        catalog,
        typedb,
        state,
        ddl_locks: DdlLocks::default(),
        server_port: address.port(),
    });
    tokio::spawn(refresh_projections(
        Arc::downgrade(&handler),
        refresh_interval,
    ));
    let auth = Arc::new(FacetAuth::new(config.postgres.username, postgres_password));
    let handlers = Arc::new(Handlers { handler, auth });

    println!(
        "{}",
        serde_json::json!({
            "event": "ready",
            "postgres_address": address.to_string()
        })
    );
    io::stdout().flush()?;

    loop {
        let (socket, _) = listener.accept().await?;
        let handlers = Arc::clone(&handlers);
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, None, handlers).await {
                eprintln!("Facet pgwire connection failed: {error}");
            }
        });
    }
}

fn read_secret(name: &str) -> Result<String, ServerError> {
    env::var(name).map_err(|_| ServerError::MissingSecret(name.to_owned()))
}

async fn refresh_projections(handler: Weak<QueryHandler>, interval: Duration) {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some(handler) = handler.upgrade() else {
            return;
        };
        handler.refresh_all().await;
    }
}

#[derive(Debug)]
struct FacetAuth {
    username: String,
    salt: Vec<u8>,
    salted_password: Vec<u8>,
}

impl FacetAuth {
    fn new(username: String, password: String) -> Self {
        let salt = rand::random::<[u8; 16]>().to_vec();
        let salted_password = gen_salted_password(&password, &salt, SCRAM_ITERATIONS);
        Self {
            username,
            salt,
            salted_password,
        }
    }
}

#[async_trait]
impl AuthSource for FacetAuth {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let supplied = login.user().ok_or(PgWireError::UserNameRequired)?;
        if supplied != self.username {
            return Err(PgWireError::InvalidPassword(supplied.to_owned()));
        }
        Ok(Password::new(
            Some(self.salt.clone()),
            self.salted_password.clone(),
        ))
    }
}

struct Handlers {
    handler: Arc<QueryHandler>,
    auth: Arc<FacetAuth>,
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = "16.13".to_owned();
        parameters.server_encoding = "UTF8".to_owned();
        parameters.time_zone = "UTC".to_owned();
        parameters.date_style = "ISO, MDY".to_owned();
        parameters.interval_style = "postgres".to_owned();
        parameters.client_encoding = Some("UTF8".to_owned());
        let mut scram = ScramAuth::new(self.auth.clone());
        scram.set_iterations(SCRAM_ITERATIONS);
        Arc::new(SASLAuthStartupHandler::new(Arc::new(parameters)).with_scram(scram))
    }
}

struct QueryHandler {
    catalog: Catalog,
    typedb: TypeDbClient,
    state: StateStore,
    ddl_locks: DdlLocks,
    server_port: u16,
}

type ProjectionKey = (String, String);
type ProjectionLockMap = HashMap<ProjectionKey, Weak<AsyncMutex<()>>>;

#[derive(Default)]
struct DdlLocks {
    by_projection: StdMutex<ProjectionLockMap>,
}

impl DdlLocks {
    fn for_projection(&self, database: &str, projection: &str) -> Arc<AsyncMutex<()>> {
        let key = (database.to_owned(), projection.to_ascii_lowercase());
        let mut locks = self.by_projection.lock().expect("DDL lock registry");
        locks.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }
}

#[async_trait]
impl SimpleQueryHandler for QueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let trimmed = query.trim();
        if starts_projection_ddl(trimmed) {
            return Ok(vec![self.execute_projection_ddl(client, trimmed).await]);
        }

        let session = SessionContext {
            database: client
                .metadata()
                .get("database")
                .cloned()
                .unwrap_or_else(|| "typedb".to_owned()),
            user: client
                .metadata()
                .get("user")
                .cloned()
                .unwrap_or_else(|| "facet".to_owned()),
            server_port: self.server_port,
        };
        Ok(vec![match execute_sql(&self.catalog, trimmed, &session) {
            Ok(result) => encode_query_result(result)?,
            Err(error) => query_error_response(error),
        }])
    }
}

impl QueryHandler {
    async fn refresh_all(&self) {
        let persisted = match self.state.load_all() {
            Ok(persisted) => persisted,
            Err(error) => {
                eprintln!("Facet refresh could not load definitions: {error}");
                return;
            }
        };
        for projection in persisted {
            self.refresh_one(projection).await;
        }
    }

    async fn refresh_one(&self, projection: PersistedProjection) {
        let database = projection.database.as_str();
        let name = projection.definition.name().to_owned();
        let ddl_lock = self.ddl_locks.for_projection(database, &name);
        let _guard = ddl_lock.lock().await;
        // DDL may have replaced or removed this generation while the lock was contended.
        match self.state.load_all() {
            Ok(current) => {
                let unchanged = current.iter().any(|candidate| {
                    candidate.database == projection.database
                        && candidate.definition.name().eq_ignore_ascii_case(&name)
                        && candidate.generation == projection.generation
                });
                if !unchanged {
                    return;
                }
            }
            Err(error) => {
                eprintln!("Facet refresh could not recheck {database}.{name}: {error}");
                return;
            }
        }
        match self
            .typedb
            .materialize(database, &projection.definition)
            .await
        {
            Ok(rows) => self.catalog.define(database, projection.definition, rows),
            Err(error) => {
                eprintln!("Facet refresh of {database}.{name} kept previous rows: {error}");
            }
        }
    }

    async fn execute_projection_ddl<C>(&self, client: &C, query: &str) -> Response
    where
        C: ClientInfo,
    {
        match parse_projection_ddl(query) {
            Ok(ProjectionStatement::Define(definition)) => {
                let database = client
                    .metadata()
                    .get("database")
                    .cloned()
                    .unwrap_or_else(|| "typedb".to_owned());
                let ddl_lock = self.ddl_locks.for_projection(&database, definition.name());
                let _guard = ddl_lock.lock().await;
                match self.typedb.materialize(&database, &definition).await {
                    Ok(rows) => match self.state.persist_success(&database, &definition) {
                        Ok(_) => {
                            self.catalog.define(&database, definition, rows);
                            Response::Execution(Tag::new("DEFINE PROJECTION"))
                        }
                        Err(error) => error_response("XX000", error.to_string()),
                    },
                    Err(error) => error_response(error.sqlstate(), error.to_string()),
                }
            }
            Ok(ProjectionStatement::Undefine { name }) => {
                let database = client
                    .metadata()
                    .get("database")
                    .cloned()
                    .unwrap_or_else(|| "typedb".to_owned());
                let ddl_lock = self.ddl_locks.for_projection(&database, &name);
                let _guard = ddl_lock.lock().await;
                match self.state.delete(&database, &name) {
                    Ok(deleted) => {
                        let removed = self.catalog.undefine(&database, &name);
                        if deleted || removed {
                            Response::Execution(Tag::new("UNDEFINE PROJECTION"))
                        } else {
                            error_response("42704", format!("projection \"{name}\" does not exist"))
                        }
                    }
                    Err(error) => error_response("XX000", error.to_string()),
                }
            }
            Err(CoreError::StructProjectionUnsupported) => error_response(
                "0A000",
                "struct projection columns are not supported".to_owned(),
            ),
            Err(error) => error_response("42601", error.to_string()),
        }
    }
}

fn starts_projection_ddl(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    lower.starts_with("define projection") || lower.starts_with("undefine projection")
}

fn encode_query_result(result: QueryResult) -> PgWireResult<Response> {
    let fields = Arc::new(
        result
            .columns
            .iter()
            .map(|column| {
                FieldInfo::new(
                    column.name.clone(),
                    None,
                    None,
                    postgres_type(column.scalar_type),
                    FieldFormat::Text,
                )
                .with_type_size(column.scalar_type.postgres_type_size())
            })
            .collect::<Vec<_>>(),
    );
    let column_types = result
        .columns
        .iter()
        .map(|column| column.scalar_type)
        .collect::<Vec<_>>();
    let rows = stream::iter(result.rows.into_iter().map(move |row| {
        let mut data = BytesMut::new();
        for (index, cell) in row.into_iter().enumerate() {
            if let Some(cell) = cell {
                if cell.scalar_type() != column_types[index] {
                    return Err(PgWireError::ApiError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "cell {cell:?} does not match PostgreSQL OID {}",
                            column_types[index].postgres_oid()
                        ),
                    ))));
                }
                let text = cell.postgres_text();
                data.put_i32(
                    i32::try_from(text.len())
                        .map_err(|error| PgWireError::ApiError(Box::new(error)))?,
                );
                data.put_slice(text.as_bytes());
            } else {
                data.put_i32(-1);
            }
        }
        Ok(DataRow::new(
            data,
            i16::try_from(column_types.len())
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?,
        ))
    }));
    Ok(Response::Query(QueryResponse::new(fields, rows)))
}

fn postgres_type(scalar_type: ScalarType) -> Type {
    match scalar_type {
        ScalarType::Boolean => Type::BOOL,
        ScalarType::Integer => Type::INT8,
        ScalarType::Double => Type::FLOAT8,
        ScalarType::Decimal => Type::NUMERIC,
        ScalarType::Date => Type::DATE,
        ScalarType::DateTime => Type::TIMESTAMP,
        ScalarType::DateTimeTz => Type::TIMESTAMPTZ,
        ScalarType::Duration => Type::INTERVAL,
        ScalarType::String => Type::TEXT,
    }
}

fn query_error_response(error: QueryError) -> Response {
    error_response(error.code, error.message)
}

fn error_response(code: &'static str, message: String) -> Response {
    Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

pub fn is_loopback(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}
