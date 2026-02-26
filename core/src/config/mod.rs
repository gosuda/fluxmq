pub mod settings;

use crate::tls::TlsConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub broker_id: u32,
    pub data_dir: String,
    pub enable_replication: bool,
    pub enable_consumer_groups: bool,
    pub recovery_mode: bool,
    pub max_connections: usize,
    pub buffer_size: usize,
    pub segment_size: u64,
    pub retention_ms: u64,
    pub metrics_port: Option<u16>,

    // TLS configuration
    pub enable_tls: bool,
    pub tls_port: Option<u16>,
    pub tls_config: Option<TlsConfig>,

    // ACL configuration
    pub enable_acl: bool,
    pub acl_config_file: Option<String>,
    pub allow_everyone: bool,
    pub super_users: Vec<String>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9092,
            broker_id: 0,
            data_dir: "./data".to_string(),
            enable_replication: false,
            enable_consumer_groups: false,
            recovery_mode: false,
            max_connections: 1000,
            buffer_size: 8192,
            segment_size: 1024 * 1024 * 1024,      // 1GB
            retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            metrics_port: Some(8080),              // Default HTTP metrics port

            // TLS defaults
            enable_tls: false,
            tls_port: None,
            tls_config: None,

            // ACL defaults
            enable_acl: false,
            acl_config_file: None,
            allow_everyone: true, // Default to open access for development
            super_users: vec!["admin".to_string()], // Default admin user
        }
    }
}

impl BrokerConfig {
    /// Enable TLS with the provided configuration
    pub fn with_tls(mut self, cert_path: &str, key_path: &str, tls_port: Option<u16>) -> Self {
        self.enable_tls = true;
        self.tls_port = tls_port.or(Some(self.port + 1000)); // Default TLS port is regular port + 1000
        self.tls_config = Some(TlsConfig::new(cert_path, key_path));
        self
    }

    /// Enable TLS with mutual authentication (client certificates)
    pub fn with_mutual_tls(
        mut self,
        cert_path: &str,
        key_path: &str,
        ca_cert_path: &str,
        tls_port: Option<u16>,
    ) -> Self {
        self.enable_tls = true;
        self.tls_port = tls_port.or(Some(self.port + 1000));
        self.tls_config = Some(TlsConfig::new(cert_path, key_path).with_client_certs(ca_cert_path));
        self
    }

    /// Enable ACL with configuration file
    pub fn with_acl(mut self, acl_config_file: Option<&str>, allow_everyone: bool) -> Self {
        self.enable_acl = true;
        self.acl_config_file = acl_config_file.map(|s| s.to_string());
        self.allow_everyone = allow_everyone;
        self
    }

    /// Add a super user
    pub fn add_super_user(mut self, username: &str) -> Self {
        if !self.super_users.contains(&username.to_string()) {
            self.super_users.push(username.to_string());
        }
        self
    }

    /// Enable secure mode (ACLs required, no anonymous access)
    pub fn secure_mode(mut self) -> Self {
        self.enable_acl = true;
        self.allow_everyone = false;
        self
    }

    /// Validate configuration bounds to prevent division-by-zero and resource exhaustion
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.max_connections == 0 {
            return Err("max_connections must be > 0".to_string());
        }
        if self.buffer_size == 0 {
            return Err("buffer_size must be > 0".to_string());
        }
        if self.segment_size == 0 {
            return Err("segment_size must be > 0".to_string());
        }
        Ok(())
    }
}
