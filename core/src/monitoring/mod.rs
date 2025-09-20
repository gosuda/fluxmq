//! # FluxMQ Advanced Monitoring Module
//!
//! This module provides comprehensive monitoring capabilities for FluxMQ including:
//! - Advanced performance dashboard with real-time charts
//! - Historical data tracking and analysis
//! - Performance alerts and threshold monitoring
//! - System resource utilization tracking
//! - io_uring integration status monitoring
//! - Consumer group performance analytics
//! - Storage layer metrics and optimization tracking

pub mod advanced_dashboard;
pub mod alert_manager;
pub mod performance_analyzer;
pub mod resource_monitor;

pub use advanced_dashboard::{AdvancedDashboard, AlertSeverity, DashboardConfig, PerformanceAlert};
pub use alert_manager::AlertManager;
pub use performance_analyzer::PerformanceAnalyzer;
pub use resource_monitor::SystemResourceMonitor;
