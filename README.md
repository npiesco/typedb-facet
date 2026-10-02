# Facet

Facet is a sidecar that serves TypeDB data to SQL tools over the PostgreSQL
wire protocol. You define a *projection*: a TypeQL `fetch` query plus a typed
column list. Facet materializes the projection from TypeDB's HTTP API and
exposes it as a read-only PostgreSQL table, so `psql`, DuckDB, dbt and
Metabase can query TypeDB without a TypeDB driver.

## How it works

- **Projections.** A projection is defined through the PostgreSQL connection
  with Facet DDL. Facet runs the TypeQL query, checks every value against the
  declared column types, and stores the result as a typed snapshot.
- **Durable definitions.** Definitions persist in a SQLite state file and are
  re-materialized on start.
- **Background refresh.** Projections refresh on a fixed interval. If TypeDB
  is unreachable or a refresh breaks a limit, the previous rows stay served
  and the failure is logged.
- **Query engine.** Simple projection reads take a byte-exact fast path. All
  other SQL runs on Apache DataFusion with a PostgreSQL-compatible
  `pg_catalog` and `information_schema`, which BI and JDBC clients need for
  introspection.
- **Protocols.** Facet supports both the simple and the extended
  (Parse/Bind/Describe/Execute) query protocols, SCRAM-SHA-256
  authentication, and TLS.

## Build

You need Rust 1.97 or newer.

```sh
cargo build --release
```

This produces the binary at `target/release/facet` (`facet.exe` on Windows).

## Run

```sh
export FACET_TYPEDB_PASSWORD=...   # TypeDB password
export FACET_PG_PASSWORD=...       # password SQL clients use
facet serve --config facet.toml
```

When the listener is ready, Facet prints one JSON line to stdout:

```json
{"event":"ready","postgres_address":"127.0.0.1:5433"}
```

## Configuration reference

Facet reads a TOML file. Secrets are never stored in it: each `password_env`
key names an environment variable that holds the secret, and Facet refuses to
start if that variable is unset.

```toml
[typedb]
url = "http://127.0.0.1:8000"      # TypeDB HTTP API origin
username = "admin"
password_env = "FACET_TYPEDB_PASSWORD"

[postgres]
listen = "127.0.0.1:5433"          # SocketAddr; port 0 picks a free port
username = "facet"
password_env = "FACET_PG_PASSWORD"

# Optional on loopback. Required when `listen` is not a loopback address.
[postgres.tls]
certificate_path = "/etc/facet/server.crt"   # PEM certificate chain
private_key_path = "/etc/facet/server.key"   # PEM private key

[state]
path = "/var/lib/facet/state.sqlite3"

# Optional.
[refresh]
interval = "5s"

# Optional. Every key is optional.
[limits]
max_rows = 1000000
max_response_bytes = 268435456
max_connections = 100
authentication_timeout = "60s"
idle_session_timeout = "10m"
```

| Key | Default | Meaning |
|---|---|---|
| `typedb.url` | required | TypeDB HTTP API origin. |
| `typedb.username` | required | TypeDB user. |
| `typedb.password_env` | required | Environment variable holding the TypeDB password. |
| `postgres.listen` | required | Address the PostgreSQL listener binds. |
| `postgres.username` | required | The single user SQL clients authenticate as. |
| `postgres.password_env` | required | Environment variable holding that user's password. |
| `postgres.tls.certificate_path` | none | PEM certificate chain. |
| `postgres.tls.private_key_path` | none | PEM private key. |
| `state.path` | required | SQLite file that stores projection definitions. |
| `refresh.interval` | `5s` | How often each projection is re-materialized. |
| `limits.max_rows` | `1000000` | Maximum rows per projection (SQLSTATE `54000` when exceeded). |
| `limits.max_response_bytes` | `268435456` | Maximum TypeDB response size per projection. |
| `limits.max_connections` | `100` | Concurrent clients; extra clients get `53300`. |
| `limits.authentication_timeout` | `60s` | Time a client has to finish startup and authentication. |
| `limits.idle_session_timeout` | none | Closes sessions that stay idle this long. |

Durations are a positive integer followed by `ms`, `s` or `m`. Zero values and
unknown `[limits]` keys are rejected at start.

### Security

- Clients always authenticate with SCRAM-SHA-256.
- Facet refuses to start with a non-loopback `listen` address unless
  `[postgres.tls]` is configured.
- With TLS configured, plaintext clients are rejected with SQLSTATE `28000`.
  Connect with `sslmode=require` or stricter (`verify-full` is recommended).

## Projection DDL

Projections are managed with Facet DDL through any PostgreSQL connection. The
database name in the connection string selects the TypeDB database.

```sql
define projection people(id: integer, name: string) as
  'match $p isa person; fetch { "id": $p.id, "name": $p.name };';

SELECT id, name FROM people ORDER BY id;

undefine projection people;
```

Each `fetch` key must match a declared column.

| Facet type | Aliases | PostgreSQL type |
|---|---|---|
| `boolean` | `bool` | `bool` |
| `integer` | `int`, `long` | `int8` |
| `double` | `float` | `float8` |
| `decimal` | | `numeric` |
| `date` | | `date` |
| `datetime` | | `timestamp` |
| `datetime-tz` | `datetimetz` | `timestamptz` |
| `duration` | | `interval` |
| `string` | `text` | `text` |

Struct columns are rejected with SQLSTATE `0A000`. Redefining a projection
replaces it only when the new definition materializes successfully.

## BI preset (Metabase)

Facet works with Metabase's PostgreSQL driver, including database sync,
field and foreign-key discovery, and native queries. Use an off-loopback
listener with TLS:

```toml
[postgres]
listen = "0.0.0.0:5433"
username = "metabase"
password_env = "FACET_PG_PASSWORD"

[postgres.tls]
certificate_path = "/etc/facet/server.crt"
private_key_path = "/etc/facet/server.key"
```

In Metabase, add a PostgreSQL database with the Facet host, port, database
name, user and password. Metabase upgrades the connection to SSL on its own.
For `verify-full`, put the CA certificate that signed the server certificate
into Metabase's SSL root certificate setting.

## Tests

The `facet-e2e` crate runs real end-to-end tests. They start a real TypeDB
server, a real Facet binary, and real clients: `psql`, a reference PostgreSQL
16 for wire-level parity, DuckDB, dbt and Metabase. Point the harness at the
tools with these environment variables:

| Variable | Tool |
|---|---|
| `FACET_E2E_FACET_BIN` | Facet binary under test |
| `FACET_E2E_TYPEDB_BIN_WSL`, `FACET_E2E_TYPEDB_CONFIG_WSL` | TypeDB server binary and config, run under WSL |
| `FACET_E2E_PSQL`, `FACET_E2E_INITDB`, `FACET_E2E_POSTGRES` | PostgreSQL 16 tools |
| `FACET_E2E_PYTHON` | Python with the `duckdb` package |
| `FACET_E2E_DBT` | dbt with the PostgreSQL adapter |
| `FACET_E2E_JAVA`, `FACET_E2E_METABASE_JAR` | Java 21 and the Metabase jar |

```sh
cargo build -p facet-server --bin facet
cargo test --workspace --all-targets -- --test-threads=1
```

## License

Facet is licensed under the [Mozilla Public License 2.0](LICENSE).
