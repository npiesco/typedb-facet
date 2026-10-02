/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Real-process end-to-end coverage for Facet.

#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    ffi::{OsStr, OsString},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

pub type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub const DATABASE: &str = "facet-e2e";
pub const FACET_USER: &str = "facet";
pub const FACET_PASSWORD: &str = "facet-e2e-password";
pub const TYPEDB_USER: &str = "admin";
pub const TYPEDB_PASSWORD: &str = "password";

pub const SCHEMA_QUERY: &str = r#"
define
  attribute id value integer;
  attribute name value string;
  entity person owns id, owns name;
"#;

pub const INSERT_TWO: &str = r#"
insert
  $a isa person, has id 1, has name "Alice";
  $b isa person, has id 2, has name "Bob";
"#;

pub const INSERT_THIRD: &str = r#"
insert
  $c isa person, has id 3, has name "Carol";
"#;

pub const DEFINE_PROJECTION: &str = concat!(
    "define projection people(id: integer, name: string) as ",
    r#"'match $p isa person; fetch { "id": $p.id, "name": $p.name };';"#
);

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("armed child guard")
    }

    fn disarm(mut self) -> Child {
        self.child.take().expect("armed child guard")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct TypeDbProcess {
    child: Child,
    linux_pid: u32,
    grpc_address: SocketAddr,
    http_address: SocketAddr,
    _lock: File,
    _temp: TempDir,
    http_origin: String,
}

impl TypeDbProcess {
    pub async fn start() -> TestResult<Self> {
        Self::start_with_args(&[]).await
    }

    pub async fn start_with_token_expiration_seconds(seconds: u64) -> TestResult<Self> {
        Self::start_with_args(&[
            "--server.authentication.token-expiration-seconds".to_owned(),
            seconds.to_string(),
        ])
        .await
    }

    async fn start_with_args(extra_args: &[String]) -> TestResult<Self> {
        let binary = required_env("FACET_E2E_TYPEDB_BIN_WSL")?;
        let config = required_env("FACET_E2E_TYPEDB_CONFIG_WSL")?;
        let grpc_port = env_u16("FACET_E2E_TYPEDB_GRPC_PORT", 31_729)?;
        let http_port = env_u16("FACET_E2E_TYPEDB_HTTP_PORT", 38_000)?;

        let lock_path =
            env::temp_dir().join(format!("facet-e2e-typedb-{grpc_port}-{http_port}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.lock()?;

        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("typedb-data");
        let log_dir = temp.path().join("typedb-logs");
        let process_log_path = temp.path().join("typedb-process.log");
        let pid_path = temp.path().join("typedb.pid");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&log_dir)?;
        let process_log = File::create(&process_log_path)?;
        File::create(&pid_path)?;

        let data_dir_wsl = windows_path_to_wsl(&data_dir)?;
        let log_dir_wsl = windows_path_to_wsl(&log_dir)?;
        let pid_path_wsl = windows_path_to_wsl(&pid_path)?;
        let grpc_address: SocketAddr = format!("127.0.0.1:{grpc_port}").parse()?;
        let http_address: SocketAddr = format!("127.0.0.1:{http_port}").parse()?;

        let child = Command::new("wsl.exe")
            .arg("-e")
            .arg("sh")
            .arg("-c")
            .arg(r#"printf '%s' "$$" > "$1"; shift; exec "$@""#)
            .arg("facet-typedb-e2e")
            .arg(pid_path_wsl)
            .arg(binary)
            .arg("--config")
            .arg(config)
            .arg("--server.listen-address")
            .arg(grpc_address.to_string())
            .arg("--server.http.enabled")
            .arg("true")
            .arg("--server.http.listen-address")
            .arg(http_address.to_string())
            .arg("--server.pgwire.enabled")
            .arg("false")
            .arg("--storage.data-directory")
            .arg(data_dir_wsl)
            .arg("--logging.directory")
            .arg(log_dir_wsl)
            .arg("--diagnostics.reporting.metrics")
            .arg("false")
            .arg("--diagnostics.reporting.errors")
            .arg("false")
            .arg("--diagnostics.monitoring.enabled")
            .arg("false")
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(process_log.try_clone()?)
            .stderr(process_log)
            .spawn()?;
        let mut child = ChildGuard::new(child);

        let linux_pid = wait_for_linux_pid(child.child_mut(), &pid_path, &process_log_path)?;
        let http_origin = format!("http://{http_address}");
        let mut process = Self {
            child: child.disarm(),
            linux_pid,
            grpc_address,
            http_address,
            _lock: lock,
            _temp: temp,
            http_origin,
        };
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()?;
        let deadline = Instant::now() + Duration::from_secs(180);

        loop {
            if let Some(status) = process.child.try_wait()? {
                let log = std::fs::read_to_string(&process_log_path).unwrap_or_default();
                return Err(format!(
                    "TypeDB exited before health was ready: {status}\nprocess output:\n{log}"
                )
                .into());
            }

            if let Ok(response) = client
                .get(format!("{}/v1/health", process.http_origin))
                .send()
                .await
                && response.status().is_success()
            {
                break;
            }

            if Instant::now() >= deadline {
                let log = std::fs::read_to_string(&process_log_path).unwrap_or_default();
                return Err(format!(
                    "TypeDB did not become healthy within 180 seconds\nprocess output:\n{log}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        Ok(process)
    }

    pub fn http_origin(&self) -> &str {
        &self.http_origin
    }

    pub async fn seed_two(&self) -> TestResult<()> {
        self.seed(false).await
    }

    pub async fn seed_three(&self) -> TestResult<()> {
        self.seed(true).await
    }

    pub async fn create_database(&self) -> TestResult<()> {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let token = self.sign_in(&client).await?;
        let create = client
            .post(format!("{}/v1/databases/{DATABASE}", self.http_origin))
            .bearer_auth(&token)
            .body("")
            .send()
            .await?;
        let status = create.status();
        let body = create.text().await?;
        if status.is_success() || status == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(format!("TypeDB database creation failed with {status}: {body}").into())
        }
    }

    pub async fn execute_typeql(
        &self,
        transaction_type: &str,
        query: &str,
        commit: bool,
    ) -> TestResult<TypeDbHttpResponse> {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let token = self.sign_in(&client).await?;
        let response = client
            .post(format!("{}/v1/query", self.http_origin))
            .bearer_auth(token)
            .json(&json!({
                "databaseName": DATABASE,
                "transactionType": transaction_type,
                "query": query,
                "queryOptions": {
                    "includeInstanceTypes": false,
                    "includeQueryStructure": false
                },
                "commit": commit
            }))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        Ok(TypeDbHttpResponse { status, body })
    }

    async fn seed(&self, include_third: bool) -> TestResult<()> {
        self.create_database().await?;
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let token = self.sign_in(&client).await?;

        run_typeql(&client, &self.http_origin, &token, "schema", SCHEMA_QUERY).await?;
        run_typeql(&client, &self.http_origin, &token, "write", INSERT_TWO).await?;
        if include_third {
            run_typeql(&client, &self.http_origin, &token, "write", INSERT_THIRD).await?;
        }

        Ok(())
    }

    async fn sign_in(&self, client: &Client) -> TestResult<String> {
        let signin = client
            .post(format!("{}/v1/signin", self.http_origin))
            .json(&json!({
                "username": TYPEDB_USER,
                "password": TYPEDB_PASSWORD,
            }))
            .send()
            .await?;
        let signin_status = signin.status();
        let signin_body = signin.text().await?;
        if !signin_status.is_success() {
            return Err(format!("TypeDB signin failed with {signin_status}: {signin_body}").into());
        }
        Ok(serde_json::from_str::<Value>(&signin_body)?
            .get("token")
            .and_then(Value::as_str)
            .ok_or("TypeDB signin response did not contain a token")?
            .to_owned())
    }
}

#[derive(Debug)]
pub struct TypeDbHttpResponse {
    pub status: StatusCode,
    pub body: String,
}

impl TypeDbHttpResponse {
    pub fn expect_success(self, operation: &str) -> TestResult<String> {
        if self.status.is_success() {
            Ok(self.body)
        } else {
            Err(format!(
                "{operation} failed with TypeDB HTTP {}: {}",
                self.status, self.body
            )
            .into())
        }
    }
}

impl Drop for TypeDbProcess {
    fn drop(&mut self) {
        let _ = Command::new("wsl.exe")
            .arg("-e")
            .arg("kill")
            .arg("-TERM")
            .arg(self.linux_pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if endpoints_closed(self.grpc_address, self.http_address) {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let _ = Command::new("wsl.exe")
            .arg("-e")
            .arg("kill")
            .arg("-KILL")
            .arg(self.linux_pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.child.kill();
        let _ = self.child.wait();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && !endpoints_closed(self.grpc_address, self.http_address) {
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub struct FacetProcess {
    child: Child,
    _stderr: Arc<Mutex<String>>,
    postgres_address: SocketAddr,
}

impl FacetProcess {
    pub fn start(binary: &Path, typedb_origin: &str, workspace: &Path) -> TestResult<Self> {
        Self::start_with_refresh(binary, typedb_origin, workspace, "5s")
    }

    pub fn start_with_refresh(
        binary: &Path,
        typedb_origin: &str,
        workspace: &Path,
        refresh_interval: &str,
    ) -> TestResult<Self> {
        let config_path = workspace.join("facet.toml");
        let state_path = toml_string(&workspace.join("facet-state.sqlite3"));
        let config = format!(
            r#"[typedb]
url = "{typedb_origin}"
username = "{TYPEDB_USER}"
password_env = "FACET_TYPEDB_PASSWORD"

[postgres]
listen = "127.0.0.1:0"
username = "{FACET_USER}"
password_env = "FACET_PG_PASSWORD"

[state]
path = "{state_path}"

[refresh]
interval = "{refresh_interval}"
"#
        );
        std::fs::write(&config_path, config)?;

        let child = Command::new(binary)
            .arg("serve")
            .arg("--config")
            .arg(&config_path)
            .env("FACET_TYPEDB_PASSWORD", TYPEDB_PASSWORD)
            .env("FACET_PG_PASSWORD", FACET_PASSWORD)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut child = ChildGuard::new(child);

        let stdout = child
            .child_mut()
            .stdout
            .take()
            .ok_or("Facet stdout was not piped")?;
        let stderr = child
            .child_mut()
            .stderr
            .take()
            .ok_or("Facet stderr was not piped")?;
        let stderr_capture = Arc::new(Mutex::new(String::new()));
        let stderr_thread_capture = Arc::clone(&stderr_capture);

        thread::spawn(move || {
            let mut stderr = BufReader::new(stderr);
            let mut captured = String::new();
            let _ = stderr.read_to_string(&mut captured);
            if let Ok(mut destination) = stderr_thread_capture.lock() {
                *destination = captured;
            }
        });

        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if value.get("event").and_then(Value::as_str) == Some("ready")
                    && let Some(address) = value.get("postgres_address").and_then(Value::as_str)
                {
                    let _ = ready_tx.send(address.to_owned());
                    return;
                }
            }
        });

        let deadline = Instant::now() + Duration::from_secs(30);
        let postgres_address = loop {
            if let Ok(address) = ready_rx.try_recv() {
                break address.parse()?;
            }

            if let Some(status) = child.child_mut().try_wait()? {
                let stderr = stderr_capture
                    .lock()
                    .map(|value| value.clone())
                    .unwrap_or_default();
                return Err(format!(
                    "Facet exited before reporting readiness ({status}). stderr: {stderr}"
                )
                .into());
            }

            if Instant::now() >= deadline {
                return Err("Facet did not report readiness within 30 seconds".into());
            }
            thread::sleep(Duration::from_millis(50));
        };

        Ok(Self {
            child: child.disarm(),
            _stderr: stderr_capture,
            postgres_address,
        })
    }

    pub fn postgres_address(&self) -> SocketAddr {
        self.postgres_address
    }

    pub fn target(&self) -> PgTarget {
        PgTarget {
            address: self.postgres_address,
            user: FACET_USER.to_owned(),
            password: FACET_PASSWORD.to_owned(),
            database: DATABASE.to_owned(),
        }
    }
}

impl Drop for FacetProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub struct PostgresProcess {
    child: Child,
    _lock: File,
    _temp: TempDir,
    target: PgTarget,
}

impl PostgresProcess {
    pub async fn start() -> TestResult<Self> {
        let port = env_u16("FACET_E2E_POSTGRES_PORT", 35_433)?;
        let lock_path = env::temp_dir().join(format!("facet-e2e-postgres-{port}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.lock()?;

        let initdb = env::var_os("FACET_E2E_INITDB")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Program Files\PostgreSQL\16\bin\initdb.exe"));
        let postgres = env::var_os("FACET_E2E_POSTGRES")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Program Files\PostgreSQL\16\bin\postgres.exe"));
        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("postgres-data");
        let password_file = temp.path().join("postgres-password.txt");
        std::fs::write(&password_file, "test\n")?;

        let init = Command::new(initdb)
            .arg("--pgdata")
            .arg(&data_dir)
            .arg("--username")
            .arg("postgres")
            .arg("--pwfile")
            .arg(&password_file)
            .arg("--auth-host")
            .arg("scram-sha-256")
            .arg("--auth-local")
            .arg("trust")
            .arg("--encoding")
            .arg("UTF8")
            .arg("--no-locale")
            .output()?;
        if !init.status.success() {
            return Err(format!(
                "initdb failed.\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&init.stdout),
                String::from_utf8_lossy(&init.stderr)
            )
            .into());
        }

        let child = Command::new(postgres)
            .arg("-D")
            .arg(&data_dir)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-F")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut child = ChildGuard::new(child);

        let target = PgTarget {
            address: format!("127.0.0.1:{port}").parse()?,
            user: "postgres".to_owned(),
            password: "test".to_owned(),
            database: "postgres".to_owned(),
        };
        let psql = psql_binary();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.child_mut().try_wait()? {
                return Err(format!("PostgreSQL exited before readiness: {status}").into());
            }
            if let Ok(output) = run_psql(&psql, &target, "SELECT 1")
                && output.status.success()
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("PostgreSQL did not become ready within 60 seconds".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(Self {
            child: child.disarm(),
            _lock: lock,
            _temp: temp,
            target,
        })
    }

    pub fn target(&self) -> PgTarget {
        self.target.clone()
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone, Debug)]
pub struct PgTarget {
    pub address: SocketAddr,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl PgTarget {
    pub fn through(&self, address: SocketAddr) -> Self {
        Self {
            address,
            ..self.clone()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendFrame {
    pub message_type: u8,
    pub payload: Vec<u8>,
    pub wire: Vec<u8>,
}

pub struct RecordingProxy {
    address: SocketAddr,
    backend_frames: Arc<Mutex<Vec<BackendFrame>>>,
    startup_packets: Arc<Mutex<Vec<Vec<u8>>>>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl RecordingProxy {
    pub fn start(target: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let backend_frames = Arc::new(Mutex::new(Vec::new()));
        let startup_packets = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let accept_frames = Arc::clone(&backend_frames);
        let accept_startups = Arc::clone(&startup_packets);
        let accept_stop = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            for incoming in listener.incoming() {
                if accept_stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(client) = incoming else { return };
                let Ok(upstream) = TcpStream::connect(target) else {
                    return;
                };
                let frames = Arc::clone(&accept_frames);
                let startups = Arc::clone(&accept_startups);
                thread::spawn(move || proxy_connection(client, upstream, frames, startups));
            }
        });

        Ok(Self {
            address,
            backend_frames,
            startup_packets,
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn drain_backend_frames(&self) -> Vec<BackendFrame> {
        wait_until_stable(&self.backend_frames);
        self.backend_frames
            .lock()
            .map(|mut frames| frames.drain(..).collect())
            .unwrap_or_default()
    }

    pub fn drain_startup_packets(&self) -> Vec<Vec<u8>> {
        wait_until_stable(&self.startup_packets);
        self.startup_packets
            .lock()
            .map(|mut packets| packets.drain(..).collect())
            .unwrap_or_default()
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

pub struct DelayedHttpProxy {
    address: SocketAddr,
    gate: Arc<ResponseGate>,
    counters: Arc<HttpCounters>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl DelayedHttpProxy {
    pub fn start(target: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let gate = Arc::new(ResponseGate::default());
        let counters = Arc::new(HttpCounters::default());
        let stop = Arc::new(AtomicBool::new(false));

        let accept_gate = Arc::clone(&gate);
        let accept_counters = Arc::clone(&counters);
        let accept_stop = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            for incoming in listener.incoming() {
                if accept_stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(client) = incoming else { return };
                let Ok(upstream) = TcpStream::connect(target) else {
                    return;
                };
                let connection_gate = Arc::clone(&accept_gate);
                let connection_counters = Arc::clone(&accept_counters);
                thread::spawn(move || {
                    delayed_http_connection(client, upstream, connection_gate, connection_counters)
                });
            }
        });

        Ok(Self {
            address,
            gate,
            counters,
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    pub fn origin(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn arm_query_response(&self) {
        self.gate.arm();
    }

    pub fn wait_for_delayed_response(&self, timeout: Duration) -> bool {
        self.gate.wait_until_blocked(timeout)
    }

    pub fn release_response(&self) {
        self.gate.release();
    }

    pub fn signin_requests(&self) -> usize {
        self.counters.signin_requests.load(Ordering::Acquire)
    }

    pub fn query_requests(&self) -> usize {
        self.counters.query_requests.load(Ordering::Acquire)
    }

    pub fn unauthorized_responses(&self) -> usize {
        self.counters.unauthorized_responses.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct HttpCounters {
    signin_requests: AtomicUsize,
    query_requests: AtomicUsize,
    unauthorized_responses: AtomicUsize,
}

impl Drop for DelayedHttpProxy {
    fn drop(&mut self) {
        self.gate.release();
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Default)]
struct ResponseGate {
    state: Mutex<ResponseGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct ResponseGateState {
    armed: bool,
    blocked: bool,
    released: bool,
}

impl ResponseGate {
    fn arm(&self) {
        let mut state = self.state.lock().expect("HTTP delay gate");
        assert!(
            !state.armed && !state.blocked,
            "HTTP delay gate is already active"
        );
        state.armed = true;
        state.released = false;
    }

    fn request_should_delay(&self) -> bool {
        self.state.lock().expect("HTTP delay gate").armed
    }

    fn block_response(&self) {
        let mut state = self.state.lock().expect("HTTP delay gate");
        if !state.armed {
            return;
        }
        state.armed = false;
        state.blocked = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("HTTP delay gate wait");
        }
        state.blocked = false;
    }

    fn wait_until_blocked(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().expect("HTTP delay gate");
        while !state.blocked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, remaining)
                .expect("HTTP delay gate wait");
            state = next;
            if result.timed_out() && !state.blocked {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("HTTP delay gate");
        state.released = true;
        self.changed.notify_all();
    }
}

pub fn run_psql(psql: &OsStr, target: &PgTarget, sql: &str) -> TestResult<Output> {
    Ok(Command::new(psql)
        .arg("--no-psqlrc")
        .arg("--host")
        .arg(target.address.ip().to_string())
        .arg("--port")
        .arg(target.address.port().to_string())
        .arg("--username")
        .arg(&target.user)
        .arg("--dbname")
        .arg(&target.database)
        .arg("--no-align")
        .arg("--tuples-only")
        .arg("--field-separator")
        .arg("|")
        .arg("--set")
        .arg("ON_ERROR_STOP=1")
        .arg("--command")
        .arg(sql)
        .env("PGPASSWORD", &target.password)
        .env("PGSSLMODE", "disable")
        .env("PGCONNECT_TIMEOUT", "5")
        .output()?)
}

pub fn assert_psql_success(output: &Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn facet_binary() -> TestResult<PathBuf> {
    Ok(PathBuf::from(required_env("FACET_E2E_FACET_BIN")?))
}

pub fn psql_binary() -> OsString {
    env::var_os("FACET_E2E_PSQL").unwrap_or_else(|| OsString::from("psql.exe"))
}

async fn run_typeql(
    client: &Client,
    origin: &str,
    token: &str,
    transaction_type: &str,
    query: &str,
) -> TestResult<()> {
    let response = client
        .post(format!("{origin}/v1/query"))
        .bearer_auth(token)
        .json(&json!({
            "databaseName": DATABASE,
            "transactionType": transaction_type,
            "query": query,
            "commit": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("TypeDB {transaction_type} query failed with {status}: {body}").into());
    }
    Ok(())
}

fn proxy_connection(
    client: TcpStream,
    upstream: TcpStream,
    frames: Arc<Mutex<Vec<BackendFrame>>>,
    startups: Arc<Mutex<Vec<Vec<u8>>>>,
) {
    let Ok(client_read) = client.try_clone() else {
        return;
    };
    let Ok(upstream_read) = upstream.try_clone() else {
        return;
    };

    let backward = thread::spawn(move || backward_and_record(upstream_read, client, frames));
    forward_and_record(client_read, upstream, startups);
    let _ = backward.join();
}

fn backward_and_record(
    mut from: TcpStream,
    mut to: TcpStream,
    frames: Arc<Mutex<Vec<BackendFrame>>>,
) {
    let mut pending = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];

    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
        pending.extend_from_slice(&buffer[..read]);

        while pending.len() >= 5 {
            let length =
                i32::from_be_bytes(pending[1..5].try_into().expect("message length")) as usize;
            if length < 4 || pending.len() < length + 1 {
                break;
            }
            let wire: Vec<u8> = pending.drain(..length + 1).collect();
            let frame = BackendFrame {
                message_type: wire[0],
                payload: wire[5..].to_vec(),
                wire,
            };
            if let Ok(mut destination) = frames.lock() {
                destination.push(frame);
            }
        }
    }
    let _ = to.shutdown(Shutdown::Write);
}

fn forward_and_record(mut from: TcpStream, mut to: TcpStream, startups: Arc<Mutex<Vec<Vec<u8>>>>) {
    const SSL_REQUEST_CODE: i32 = 80_877_103;
    const GSSENC_REQUEST_CODE: i32 = 80_877_104;

    let mut pending = Vec::new();
    let mut in_startup = true;
    let mut buffer = [0_u8; 16 * 1024];

    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
        pending.extend_from_slice(&buffer[..read]);

        loop {
            if in_startup {
                if pending.len() < 4 {
                    break;
                }
                let length =
                    i32::from_be_bytes(pending[..4].try_into().expect("startup length")) as usize;
                if length < 4 || pending.len() < length {
                    break;
                }
                let packet: Vec<u8> = pending.drain(..length).collect();
                let code = (length == 8)
                    .then(|| i32::from_be_bytes(packet[4..8].try_into().expect("request code")));
                if !matches!(code, Some(SSL_REQUEST_CODE) | Some(GSSENC_REQUEST_CODE)) {
                    in_startup = false;
                    if let Ok(mut destination) = startups.lock() {
                        destination.push(packet);
                    }
                }
            } else {
                if pending.len() < 5 {
                    break;
                }
                let length =
                    i32::from_be_bytes(pending[1..5].try_into().expect("frontend length")) as usize;
                if length < 4 || pending.len() < length + 1 {
                    break;
                }
                pending.drain(..length + 1);
            }
        }
    }
    let _ = to.shutdown(Shutdown::Write);
}

fn delayed_http_connection(
    client: TcpStream,
    upstream: TcpStream,
    gate: Arc<ResponseGate>,
    counters: Arc<HttpCounters>,
) {
    let Ok(client_read) = client.try_clone() else {
        return;
    };
    let Ok(upstream_read) = upstream.try_clone() else {
        return;
    };
    let delay_this_response = Arc::new(AtomicBool::new(false));
    let backward_delay = Arc::clone(&delay_this_response);
    let backward_gate = Arc::clone(&gate);
    let backward_counters = Arc::clone(&counters);
    let backward = thread::spawn(move || {
        delayed_http_backward(
            upstream_read,
            client,
            backward_gate,
            backward_delay,
            backward_counters,
        )
    });
    delayed_http_forward(client_read, upstream, gate, delay_this_response, counters);
    let _ = backward.join();
}

fn delayed_http_forward(
    mut from: TcpStream,
    mut to: TcpStream,
    gate: Arc<ResponseGate>,
    delay_this_response: Arc<AtomicBool>,
    counters: Arc<HttpCounters>,
) {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if contains_bytes(&buffer[..read], b"POST /v1/signin ") {
            counters.signin_requests.fetch_add(1, Ordering::AcqRel);
        }
        if contains_bytes(&buffer[..read], b"POST /v1/query ") {
            counters.query_requests.fetch_add(1, Ordering::AcqRel);
            if gate.request_should_delay() {
                delay_this_response.store(true, Ordering::Release);
            }
        }
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
    }
    let _ = to.shutdown(Shutdown::Write);
}

fn delayed_http_backward(
    mut from: TcpStream,
    mut to: TcpStream,
    gate: Arc<ResponseGate>,
    delay_this_response: Arc<AtomicBool>,
    counters: Arc<HttpCounters>,
) {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if contains_bytes(&buffer[..read], b"HTTP/1.1 401 ") {
            counters
                .unauthorized_responses
                .fetch_add(1, Ordering::AcqRel);
        }
        if delay_this_response.swap(false, Ordering::AcqRel) {
            gate.block_response();
        }
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
    }
    let _ = to.shutdown(Shutdown::Write);
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn wait_until_stable<T>(items: &Mutex<Vec<T>>) {
    let mut previous = usize::MAX;
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(25));
        let current = items.lock().map(|items| items.len()).unwrap_or_default();
        if current == previous {
            return;
        }
        previous = current;
    }
}

fn required_env(name: &str) -> TestResult<OsString> {
    env::var_os(name).ok_or_else(|| format!("{name} must identify a real E2E prerequisite").into())
}

fn wait_for_linux_pid(
    child: &mut Child,
    pid_path: &Path,
    process_log_path: &Path,
) -> TestResult<u32> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(pid) = std::fs::read_to_string(pid_path)
            && let Ok(pid) = pid.trim().parse()
        {
            return Ok(pid);
        }

        if let Some(status) = child.try_wait()? {
            let log = std::fs::read_to_string(process_log_path).unwrap_or_default();
            return Err(format!(
                "TypeDB exited before reporting its owned Linux PID: {status}\nprocess output:\n{log}"
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err("TypeDB did not report its owned Linux PID within 30 seconds".into());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn endpoints_closed(grpc_address: SocketAddr, http_address: SocketAddr) -> bool {
    [grpc_address, http_address]
        .into_iter()
        .all(|address| TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_err())
}

fn env_u16(name: &str, default: u16) -> TestResult<u16> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn windows_path_to_wsl(path: &Path) -> TestResult<String> {
    let canonical = path.canonicalize()?;
    let text = canonical.to_string_lossy();
    let text = text.strip_prefix(r"\\?\").unwrap_or(&text);
    let bytes = text.as_bytes();
    if bytes.len() < 3 || bytes[1] != b':' || bytes[2] != b'\\' {
        return Err(format!("cannot map non-drive Windows path into WSL: {text}").into());
    }

    let drive = (bytes[0] as char).to_ascii_lowercase();
    let remainder = text[3..].replace('\\', "/");
    Ok(format!("/mnt/{drive}/{remainder}"))
}

fn toml_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}
