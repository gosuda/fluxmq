use super::BrokerConfig;
use crate::Result;
use config::{Config, Environment};

impl BrokerConfig {
    pub fn from_env() -> Result<Self> {
        let settings = Config::builder()
            .add_source(Environment::with_prefix("FLUXMQ"))
            .build()
            .map_err(|e| crate::FluxmqError::Config(e.to_string()))?;

        let config = settings
            .try_deserialize::<BrokerConfig>()
            .map_err(|e| crate::FluxmqError::Config(e.to_string()))?;

        Ok(config)
    }
}
