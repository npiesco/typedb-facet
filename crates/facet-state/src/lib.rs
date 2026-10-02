/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Durable, versioned projection-definition state.

#![forbid(unsafe_code)]

use std::{fs, path::PathBuf, time::Duration};

use facet_core::{ColumnDefinition, CoreError, ProjectionDefinition};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const SCHEMA_VERSION: i64 = 1;

#[derive(Clone, Debug)]
pub struct StateStore {
    path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PersistedProjection {
    pub database: String,
    pub definition: ProjectionDefinition,
    pub generation: u64,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("could not create Facet state directory: {0}")]
    CreateDirectory(#[from] std::io::Error),
    #[error("SQLite state error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("projection definition JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid persisted projection definition: {0}")]
    InvalidDefinition(#[from] CoreError),
    #[error("unsupported Facet state schema version {0}")]
    UnsupportedSchemaVersion(i64),
    #[error("projection generation overflow for {database}.{name}")]
    GenerationOverflow { database: String, name: String },
    #[error("invalid persisted projection generation {generation} for {database}.{name}")]
    InvalidGeneration {
        database: String,
        name: String,
        generation: i64,
    },
    #[error(
        "persisted projection name {stored_name:?} does not match key {key_name:?} in database {database:?}"
    )]
    NameMismatch {
        database: String,
        key_name: String,
        stored_name: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct StoredDefinition {
    name: String,
    columns: Vec<ColumnDefinition>,
    source_query: String,
}

impl StateStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StateError> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let store = Self { path };
        store.initialize()?;
        Ok(store)
    }

    pub fn load_all(&self) -> Result<Vec<PersistedProjection>, StateError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT database_name, projection_name, definition_json, generation
             FROM projection_definitions
             ORDER BY database_name, projection_name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        let mut projections = Vec::new();
        for row in rows {
            let (database, key_name, definition_json, generation) = row?;
            let stored: StoredDefinition = serde_json::from_str(&definition_json)?;
            if !stored.name.eq_ignore_ascii_case(&key_name) {
                return Err(StateError::NameMismatch {
                    database,
                    key_name,
                    stored_name: stored.name,
                });
            }
            let definition =
                ProjectionDefinition::new(stored.name, stored.columns, stored.source_query)?;
            let generation =
                u64::try_from(generation).map_err(|_| StateError::InvalidGeneration {
                    database: database.clone(),
                    name: key_name,
                    generation,
                })?;
            projections.push(PersistedProjection {
                database,
                definition,
                generation,
            });
        }
        Ok(projections)
    }

    pub fn persist_success(
        &self,
        database: &str,
        definition: &ProjectionDefinition,
    ) -> Result<u64, StateError> {
        let normalized = normalize_definition(definition);
        let projection_name = normalized.name.to_ascii_lowercase();
        let definition_json = serde_json::to_string(&normalized)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT generation FROM projection_generations
                 WHERE database_name = ?1 AND projection_name = ?2",
                params![database, projection_name],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        let generation = current
            .checked_add(1)
            .ok_or_else(|| StateError::GenerationOverflow {
                database: database.to_owned(),
                name: projection_name.clone(),
            })?;
        transaction.execute(
            "INSERT INTO projection_generations (
             database_name, projection_name, generation
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(database_name, projection_name) DO UPDATE SET
             generation = excluded.generation",
            params![database, projection_name, generation],
        )?;
        transaction.execute(
            "INSERT INTO projection_definitions (
                 database_name, projection_name, definition_json, generation
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(database_name, projection_name) DO UPDATE SET
                 definition_json = excluded.definition_json,
                 generation = excluded.generation",
            params![database, projection_name, definition_json, generation],
        )?;
        transaction.commit()?;
        Ok(u64::try_from(generation).expect("positive SQLite generation"))
    }

    pub fn delete(&self, database: &str, name: &str) -> Result<bool, StateError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deleted = transaction.execute(
            "DELETE FROM projection_definitions
             WHERE database_name = ?1 AND projection_name = ?2",
            params![database, name.to_ascii_lowercase()],
        )?;
        transaction.commit()?;
        Ok(deleted != 0)
    }

    fn initialize(&self) -> Result<(), StateError> {
        let mut connection = self.connection()?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            0 => {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "CREATE TABLE projection_definitions (
                         database_name TEXT NOT NULL,
                         projection_name TEXT NOT NULL,
                         definition_json TEXT NOT NULL,
                         generation INTEGER NOT NULL CHECK (generation > 0),
                         PRIMARY KEY (database_name, projection_name)
                     ) WITHOUT ROWID;
                     CREATE TABLE projection_generations (
                         database_name TEXT NOT NULL,
                         projection_name TEXT NOT NULL,
                         generation INTEGER NOT NULL CHECK (generation > 0),
                         PRIMARY KEY (database_name, projection_name)
                     ) WITHOUT ROWID;
                     PRAGMA user_version = 1;",
                )?;
                transaction.commit()?;
                Ok(())
            }
            SCHEMA_VERSION => Ok(()),
            other => Err(StateError::UnsupportedSchemaVersion(other)),
        }
    }

    fn connection(&self) -> Result<Connection, StateError> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;",
        )?;
        Ok(connection)
    }
}

fn normalize_definition(definition: &ProjectionDefinition) -> StoredDefinition {
    StoredDefinition {
        name: definition.name().to_owned(),
        columns: definition
            .columns()
            .iter()
            .map(|column| ColumnDefinition {
                name: column.name.clone(),
                scalar_type: column.scalar_type,
            })
            .collect(),
        source_query: definition.source_query().trim().to_owned(),
    }
}
