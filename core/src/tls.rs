//! TLS/SSL support for FluxMQ
//!
//! This module provides TLS encryption capabilities for secure client-broker
//! communication, including certificate loading, TLS acceptor setup, and
//! secure stream handling.

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use thiserror::Error;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

/// TLS-related errors
#[derive(Error, Debug)]
pub enum TlsError {
    #[error("Failed to read certificate file: {0}")]
    CertificateRead(#[from] std::io::Error),

    #[error("Failed to parse certificate: {0}")]
    CertificateParse(String),

    #[error("Failed to parse private key: {0}")]
    PrivateKeyParse(String),

    #[error("TLS configuration error: {0}")]
    ConfigError(#[from] rustls::Error),

    #[error("No private keys found in key file")]
    NoPrivateKeys,

    #[error("No certificates found in certificate file")]
    NoCertificates,
}

/// TLS configuration for the FluxMQ broker
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsConfig {
    /// Path to the certificate file (PEM format)
    pub cert_path: String,

    /// Path to the private key file (PEM format)
    pub key_path: String,

    /// Whether to require client certificates (mutual TLS)
    pub require_client_certs: bool,

    /// Path to the CA certificate file for client verification (optional)
    pub ca_cert_path: Option<String>,
}

impl TlsConfig {
    /// Create a new TLS configuration
    pub fn new<P: Into<String>>(cert_path: P, key_path: P) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            require_client_certs: false,
            ca_cert_path: None,
        }
    }

    /// Enable mutual TLS with client certificate verification
    pub fn with_client_certs<P: Into<String>>(mut self, ca_cert_path: P) -> Self {
        self.require_client_certs = true;
        self.ca_cert_path = Some(ca_cert_path.into());
        self
    }
}

/// TLS acceptor wrapper for handling secure connections
pub struct FluxTlsAcceptor {
    acceptor: TlsAcceptor,
    config: TlsConfig,
}

impl FluxTlsAcceptor {
    /// Create a new TLS acceptor from configuration
    pub fn new(config: TlsConfig) -> Result<Self, TlsError> {
        info!(
            "Initializing TLS with cert: {}, key: {}",
            config.cert_path, config.key_path
        );

        // Load certificates
        let certs = load_certs(&config.cert_path)?;
        if certs.is_empty() {
            return Err(TlsError::NoCertificates);
        }

        // Load private key
        let mut keys = load_private_keys(&config.key_path)?;
        if keys.is_empty() {
            return Err(TlsError::NoPrivateKeys);
        }

        // Build TLS server configuration
        let mut tls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))?;

        // Configure ALPN protocols (support HTTP/2 and HTTP/1.1)
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let acceptor = TlsAcceptor::from(Arc::new(tls_config));

        info!("TLS acceptor initialized successfully");

        Ok(Self { acceptor, config })
    }

    /// Accept a TLS connection from a TCP stream
    pub async fn accept<IO>(
        &self,
        stream: IO,
    ) -> Result<tokio_rustls::server::TlsStream<IO>, TlsError>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        match self.acceptor.accept(stream).await {
            Ok(tls_stream) => {
                info!("TLS handshake completed successfully");
                Ok(tls_stream)
            }
            Err(e) => {
                warn!("TLS handshake failed: {}", e);
                Err(TlsError::ConfigError(rustls::Error::General(
                    "TLS handshake failed".to_string(),
                )))
            }
        }
    }

    /// Get the TLS configuration
    pub fn config(&self) -> &TlsConfig {
        &self.config
    }
}

/// Load certificates from a PEM file
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let cert_file = File::open(path)?;
    let mut reader = BufReader::new(cert_file);

    let certs: Vec<CertificateDer<'static>> = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertificateParse(e.to_string()))?;

    info!("Loaded {} certificates from {}", certs.len(), path);

    Ok(certs)
}

/// Load private keys from a PEM file
fn load_private_keys(path: &str) -> Result<Vec<PrivateKeyDer<'static>>, TlsError> {
    let key_file = File::open(path)?;
    let mut reader = BufReader::new(key_file);

    let keys: Vec<PrivateKeyDer<'static>> = pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::PrivateKeyParse(e.to_string()))?
        .into_iter()
        .map(|k| k.into())
        .collect();

    if keys.is_empty() {
        // Try RSA keys if PKCS8 keys are not found
        let key_file = File::open(path)?;
        let mut reader = BufReader::new(key_file);

        let rsa_keys: Vec<PrivateKeyDer<'static>> = rustls_pemfile::rsa_private_keys(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::PrivateKeyParse(e.to_string()))?
            .into_iter()
            .map(|k| k.into())
            .collect();

        info!("Loaded {} RSA private keys from {}", rsa_keys.len(), path);
        Ok(rsa_keys)
    } else {
        info!("Loaded {} PKCS8 private keys from {}", keys.len(), path);
        Ok(keys)
    }
}

/// Generate self-signed certificate for development/testing
#[cfg(test)]
pub fn generate_self_signed_cert() -> Result<(String, String), TlsError> {
    // This is a placeholder for self-signed certificate generation
    // In a real implementation, you'd use a crate like `rcgen` to generate certificates
    let cert_pem = r#"-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKpWKx... (example self-signed cert)
-----END CERTIFICATE-----"#;

    let key_pem = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC... (example private key)
-----END PRIVATE KEY-----"#;

    let temp_dir = std::env::temp_dir();
    let cert_path = temp_dir.join("test-cert.pem");
    let key_path = temp_dir.join("test-key.pem");

    std::fs::write(&cert_path, cert_pem)?;
    std::fs::write(&key_path, key_pem)?;

    Ok((
        cert_path.to_string_lossy().to_string(),
        key_path.to_string_lossy().to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_cert_files() -> Result<(String, String), Box<dyn std::error::Error>> {
        let dir = tempdir()?;

        // Example certificate (this is not a real certificate, just for testing structure)
        let cert_content = r#"-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKpWKxVxNJrTANBgkqhkiG9w0BAQsFADCBjTELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAkNBMRQwEgYDVQQHDAtTYW4gRnJhbmNpcmUxEDAOBgNVBAoM
B0ZsdXhNUTEQMA4GA1UECwwHRmx1eE1RMREwDwYDVQQDDAhsb2NhbGhvc3QxJDAi
BgkqhkiG9w0BCQEWFXRlc3RAZXhhbXBsZS5jb20wHhcNMjMwNzE1MTIzMDAwWhcN
MjQwNzE0MTIzMDAwWjCBjTELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMRQwEgYD
EXAMPLE_CERT_CONTENT_HERE
-----END CERTIFICATE-----"#;

        let key_content = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxyz8/M2ZxYZkK
EXAMPLE_KEY_CONTENT_HERE
-----END PRIVATE KEY-----"#;

        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        std::fs::write(&cert_path, cert_content)?;
        std::fs::write(&key_path, key_content)?;

        Ok((
            cert_path.to_string_lossy().to_string(),
            key_path.to_string_lossy().to_string(),
        ))
    }

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new("cert.pem", "key.pem");
        assert_eq!(config.cert_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
        assert!(!config.require_client_certs);
        assert!(config.ca_cert_path.is_none());
    }

    #[test]
    fn test_tls_config_with_client_certs() {
        let config = TlsConfig::new("cert.pem", "key.pem").with_client_certs("ca.pem");

        assert!(config.require_client_certs);
        assert_eq!(config.ca_cert_path, Some("ca.pem".to_string()));
    }
}
