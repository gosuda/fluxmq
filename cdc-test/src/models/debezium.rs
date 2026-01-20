use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Debezium CDC event message
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DebeziumMessage {
    /// Before state (for UPDATE and DELETE operations)
    pub before: Option<User>,

    /// After state (for CREATE and UPDATE operations)
    pub after: Option<User>,

    /// Source metadata
    pub source: SourceMetadata,

    /// Operation type: c(create), u(update), d(delete), r(read/snapshot)
    pub op: String,

    /// Timestamp in milliseconds
    pub ts_ms: i64,
}

/// User table record
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub age: Option<i32>,

    /// Microsecond timestamp from PostgreSQL
    #[serde(default)]
    pub created_at: Option<i64>,

    #[serde(default)]
    pub updated_at: Option<i64>,
}

/// Debezium source metadata
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceMetadata {
    pub version: String,
    pub connector: String,
    pub name: String,
    pub ts_ms: i64,
    pub snapshot: String,
    pub db: String,
    pub schema: String,
    pub table: String,

    #[serde(rename = "txId")]
    pub tx_id: Option<i64>,

    pub lsn: Option<i64>,
}

/// Operation type enum
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Read, // Snapshot
}

impl DebeziumMessage {
    /// Parse operation type
    pub fn operation(&self) -> Operation {
        match self.op.as_str() {
            "c" => Operation::Create,
            "u" => Operation::Update,
            "d" => Operation::Delete,
            "r" => Operation::Read,
            _ => panic!("Unknown operation: {}", self.op),
        }
    }

    /// Convert timestamp to DateTime
    pub fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.ts_ms).unwrap_or_else(Utc::now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_message() {
        let json = r#"{
            "before": null,
            "after": {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 30
            },
            "source": {
                "version": "2.4.0",
                "connector": "postgresql",
                "name": "dbserver1",
                "ts_ms": 1705334400000,
                "snapshot": "false",
                "db": "testdb",
                "schema": "public",
                "table": "users",
                "txId": 123,
                "lsn": 12345678
            },
            "op": "c",
            "ts_ms": 1705334400000
        }"#;

        let msg: DebeziumMessage = serde_json::from_str(json).unwrap();

        assert_eq!(msg.operation(), Operation::Create);
        assert!(msg.before.is_none());
        assert!(msg.after.is_some());

        let user = msg.after.unwrap();
        assert_eq!(user.name, "Alice");
        assert_eq!(user.email, "alice@example.com");
    }

    #[test]
    fn test_parse_update_message() {
        let json = r#"{
            "before": {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 30
            },
            "after": {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 31
            },
            "source": {
                "version": "2.4.0",
                "connector": "postgresql",
                "name": "dbserver1",
                "ts_ms": 1705334400000,
                "snapshot": "false",
                "db": "testdb",
                "schema": "public",
                "table": "users"
            },
            "op": "u",
            "ts_ms": 1705334400000
        }"#;

        let msg: DebeziumMessage = serde_json::from_str(json).unwrap();

        assert_eq!(msg.operation(), Operation::Update);
        assert!(msg.before.is_some());
        assert!(msg.after.is_some());
    }

    #[test]
    fn test_parse_delete_message() {
        let json = r#"{
            "before": {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 30
            },
            "after": null,
            "source": {
                "version": "2.4.0",
                "connector": "postgresql",
                "name": "dbserver1",
                "ts_ms": 1705334400000,
                "snapshot": "false",
                "db": "testdb",
                "schema": "public",
                "table": "users"
            },
            "op": "d",
            "ts_ms": 1705334400000
        }"#;

        let msg: DebeziumMessage = serde_json::from_str(json).unwrap();

        assert_eq!(msg.operation(), Operation::Delete);
        assert!(msg.before.is_some());
        assert!(msg.after.is_none());
    }
}
