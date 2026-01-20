use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topic: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            kafka: KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "rust-cdc-test-group".to_string(),
                topic: "cdc.public.users".to_string(),
            },
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            kafka: KafkaConfig {
                brokers: std::env::var("KAFKA_BROKERS")
                    .unwrap_or_else(|_| "localhost:9092".to_string()),
                group_id: std::env::var("KAFKA_GROUP_ID")
                    .unwrap_or_else(|_| "rust-cdc-test-group".to_string()),
                topic: std::env::var("KAFKA_TOPIC")
                    .unwrap_or_else(|_| "cdc.public.users".to_string()),
            },
        }
    }
}
