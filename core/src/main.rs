use clap::Parser;
use fluxmq::{tls::TlsConfig, BrokerConfig, BrokerId, BrokerServer, Result};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "fluxmq")]
#[command(about = "A Kafka-compatible message broker written in Rust")]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(short, long, default_value = "9092")]
    port: u16,

    #[arg(short, long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "0")]
    broker_id: BrokerId,

    #[arg(long)]
    enable_replication: bool,

    #[arg(long)]
    enable_consumer_groups: bool,

    #[arg(long)]
    recovery_mode: bool,

    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long, default_value = "8080")]
    metrics_port: Option<u16>,

    /// Enable TLS encryption
    #[arg(long)]
    enable_tls: bool,

    /// TLS certificate file path (PEM format)
    #[arg(long)]
    tls_cert: Option<String>,

    /// TLS private key file path (PEM format)  
    #[arg(long)]
    tls_key: Option<String>,

    /// TLS port (defaults to port + 1000)
    #[arg(long)]
    tls_port: Option<u16>,

    /// Enable ACL authorization
    #[arg(long)]
    enable_acl: bool,

    /// ACL configuration file path (JSON format)
    #[arg(long)]
    acl_config: Option<String>,

    /// Allow everyone access when no ACL matches (default: true for development)
    #[arg(long)]
    acl_allow_everyone: bool,

    /// Super users (comma-separated list)
    #[arg(long)]
    super_users: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(parse_log_level(&args.log_level))
        .init();

    info!("Starting FluxMQ broker on {}:{}", args.host, args.port);
    info!("Broker ID: {}", args.broker_id);
    info!("Replication enabled: {}", args.enable_replication);
    info!("Consumer groups enabled: {}", args.enable_consumer_groups);
    info!("Recovery mode: {}", args.recovery_mode);
    info!("Data directory: {}", args.data_dir);
    info!("TLS enabled: {}", args.enable_tls);

    if args.enable_tls {
        if let (Some(ref cert_path), Some(ref key_path)) = (&args.tls_cert, &args.tls_key) {
            info!("TLS certificate: {}", cert_path);
            info!("TLS key: {}", key_path);
            if let Some(tls_port) = args.tls_port {
                info!("TLS port: {}", tls_port);
            } else {
                info!("TLS port: {} (auto)", args.port + 1000);
            }
        } else {
            error!("TLS enabled but certificate and/or key path not provided");
            error!("Use --tls-cert and --tls-key to specify certificate files");
            return Err(fluxmq::FluxmqError::Config(
                "Missing TLS configuration".to_string(),
            ));
        }
    }

    info!("ACL enabled: {}", args.enable_acl);

    if args.enable_acl {
        info!("ACL allow everyone: {}", args.acl_allow_everyone);
        if let Some(ref acl_config) = args.acl_config {
            info!("ACL config file: {}", acl_config);
        }
        if let Some(ref super_users) = args.super_users {
            info!("Super users: {}", super_users);
        }
    }

    if let Some(metrics_port) = args.metrics_port {
        info!("Metrics HTTP server port: {}", metrics_port);
    } else {
        info!("Metrics HTTP server: disabled");
    }

    // Create TLS configuration if enabled
    let tls_config = if args.enable_tls {
        if let (Some(cert_path), Some(key_path)) = (args.tls_cert, args.tls_key) {
            Some(TlsConfig::new(cert_path, key_path))
        } else {
            None
        }
    } else {
        None
    };

    // Parse super users
    let super_users = if let Some(super_users_str) = args.super_users {
        super_users_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    } else {
        vec!["admin".to_string()] // Default admin user
    };

    // Create broker server with Kafka-only support
    let config = BrokerConfig {
        host: args.host,
        port: args.port,
        broker_id: args.broker_id,
        data_dir: args.data_dir,
        enable_replication: args.enable_replication,
        enable_consumer_groups: args.enable_consumer_groups,
        recovery_mode: args.recovery_mode,
        metrics_port: args.metrics_port,
        enable_tls: args.enable_tls,
        tls_port: args.tls_port,
        tls_config,
        enable_acl: args.enable_acl,
        acl_config_file: args.acl_config,
        allow_everyone: args.acl_allow_everyone,
        super_users,
        ..Default::default()
    };

    let server = BrokerServer::new_async(config).await?;

    // Spawn the server task
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("Server error: {}", e);
        }
    });

    // Set up signal handlers
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down gracefully...");
        }
        _ = server_handle => {
            info!("Server task completed");
        }
    }

    info!("FluxMQ shut down successfully");
    Ok(())
}

fn parse_log_level(level: &str) -> tracing::Level {
    match level.to_lowercase().as_str() {
        "trace" => tracing::Level::TRACE,
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => {
            warn!("Invalid log level '{}', defaulting to 'info'", level);
            tracing::Level::INFO
        }
    }
}
