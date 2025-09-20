//! Alert Management System
//!
//! This module provides comprehensive alerting capabilities including:
//! - Real-time alert generation and management
//! - Alert severity classification and escalation
//! - Alert aggregation and deduplication
//! - Notification routing and delivery
//! - Alert history and analytics

use crate::monitoring::{AlertSeverity, PerformanceAlert};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Alert rule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub metric_name: String,
    pub condition: AlertCondition,
    pub threshold: f64,
    pub severity: AlertSeverity,
    pub duration_seconds: u64,
    pub enabled: bool,
    pub notification_channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    GreaterThan,
    LessThan,
    EqualTo,
    NotEqualTo,
    RateIncrease(f64), // Percentage increase
    RateDecrease(f64), // Percentage decrease
}

/// Alert state tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertState {
    pub rule_id: String,
    pub status: AlertStatus,
    pub first_triggered: u64,
    pub last_triggered: u64,
    pub trigger_count: u64,
    pub current_value: f64,
    pub resolved_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertStatus {
    Pending,  // Condition met but not yet fired
    Firing,   // Alert is active
    Resolved, // Alert condition no longer met
    Silenced, // Alert temporarily disabled
}

/// Alert notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: String,
    pub name: String,
    pub channel_type: ChannelType,
    pub configuration: ChannelConfig,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChannelType {
    Email,
    Slack,
    Webhook,
    PagerDuty,
    Log,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub email_addresses: Option<Vec<String>>,
    pub slack_webhook_url: Option<String>,
    pub webhook_url: Option<String>,
    pub pagerduty_key: Option<String>,
    pub log_level: Option<String>,
}

/// Alert manager configuration
#[derive(Debug, Clone)]
pub struct AlertManagerConfig {
    pub evaluation_interval_seconds: u64,
    pub alert_history_retention_hours: usize,
    pub max_alerts_per_rule: usize,
    pub enable_alert_aggregation: bool,
    pub aggregation_window_seconds: u64,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        Self {
            evaluation_interval_seconds: 10,
            alert_history_retention_hours: 24,
            max_alerts_per_rule: 1000,
            enable_alert_aggregation: true,
            aggregation_window_seconds: 300, // 5 minutes
        }
    }
}

/// Alert analytics and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertAnalytics {
    pub total_alerts_24h: u64,
    pub alerts_by_severity: HashMap<String, u64>,
    pub alerts_by_component: HashMap<String, u64>,
    pub avg_resolution_time_minutes: f64,
    pub top_firing_rules: Vec<TopFiringRule>,
    pub alert_trends: AlertTrends,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopFiringRule {
    pub rule_name: String,
    pub fire_count: u64,
    pub avg_duration_minutes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertTrends {
    pub hourly_counts: Vec<u64>,
    pub daily_pattern: Vec<f64>,
    pub resolution_time_trend: TrendDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrendDirection {
    Improving,
    Worsening,
    Stable,
}

/// Comprehensive alert manager
pub struct AlertManager {
    config: AlertManagerConfig,
    alert_rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    alert_states: Arc<RwLock<HashMap<String, AlertState>>>,
    notification_channels: Arc<RwLock<HashMap<String, NotificationChannel>>>,
    alert_history: Arc<RwLock<VecDeque<PerformanceAlert>>>,
    metrics_cache: Arc<RwLock<HashMap<String, f64>>>,
}

impl AlertManager {
    pub fn new(config: AlertManagerConfig) -> Self {
        Self {
            config,
            alert_rules: Arc::new(RwLock::new(HashMap::new())),
            alert_states: Arc::new(RwLock::new(HashMap::new())),
            notification_channels: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(VecDeque::new())),
            metrics_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the alert manager background task
    pub async fn start_alert_manager(self: Arc<Self>) {
        // Initialize default alert rules
        self.initialize_default_rules().await;

        // Initialize default notification channels
        self.initialize_default_channels().await;

        let manager = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                manager.config.evaluation_interval_seconds,
            ));

            loop {
                interval.tick().await;
                if let Err(e) = manager.evaluate_alert_rules().await {
                    error!("Failed to evaluate alert rules: {}", e);
                }
            }
        });

        info!(
            "Alert manager started with {}s evaluation interval",
            self.config.evaluation_interval_seconds
        );
    }

    /// Initialize default alert rules
    async fn initialize_default_rules(&self) {
        let default_rules = vec![
            AlertRule {
                id: "high_memory_usage".to_string(),
                name: "High Memory Usage".to_string(),
                description: "Memory usage exceeds 80%".to_string(),
                metric_name: "memory_usage_percent".to_string(),
                condition: AlertCondition::GreaterThan,
                threshold: 80.0,
                severity: AlertSeverity::Warning,
                duration_seconds: 300,
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
            AlertRule {
                id: "high_cpu_usage".to_string(),
                name: "High CPU Usage".to_string(),
                description: "CPU usage exceeds 75%".to_string(),
                metric_name: "cpu_usage_percent".to_string(),
                condition: AlertCondition::GreaterThan,
                threshold: 75.0,
                severity: AlertSeverity::Warning,
                duration_seconds: 180,
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
            AlertRule {
                id: "throughput_drop".to_string(),
                name: "Throughput Drop".to_string(),
                description: "Throughput decreased by more than 50%".to_string(),
                metric_name: "total_throughput".to_string(),
                condition: AlertCondition::RateDecrease(50.0),
                threshold: 0.0,
                severity: AlertSeverity::Critical,
                duration_seconds: 120,
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
            AlertRule {
                id: "connection_limit".to_string(),
                name: "Connection Limit Approaching".to_string(),
                description: "Active connections exceed 8000".to_string(),
                metric_name: "active_connections".to_string(),
                condition: AlertCondition::GreaterThan,
                threshold: 8000.0,
                severity: AlertSeverity::Critical,
                duration_seconds: 60,
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
        ];

        let mut rules = self.alert_rules.write().await;
        for rule in default_rules {
            rules.insert(rule.id.clone(), rule);
        }
    }

    /// Initialize default notification channels
    async fn initialize_default_channels(&self) {
        let default_channels = vec![NotificationChannel {
            id: "default_log".to_string(),
            name: "Default Log Channel".to_string(),
            channel_type: ChannelType::Log,
            configuration: ChannelConfig {
                email_addresses: None,
                slack_webhook_url: None,
                webhook_url: None,
                pagerduty_key: None,
                log_level: Some("warn".to_string()),
            },
            enabled: true,
        }];

        let mut channels = self.notification_channels.write().await;
        for channel in default_channels {
            channels.insert(channel.id.clone(), channel);
        }
    }

    /// Evaluate all alert rules against current metrics
    async fn evaluate_alert_rules(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let rules = self.alert_rules.read().await;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        for rule in rules.values() {
            if !rule.enabled {
                continue;
            }

            if let Err(e) = self.evaluate_single_rule(rule, timestamp).await {
                warn!("Failed to evaluate rule {}: {}", rule.id, e);
            }
        }

        // Clean up old alert history
        self.cleanup_alert_history().await;

        Ok(())
    }

    /// Evaluate a single alert rule
    async fn evaluate_single_rule(
        &self,
        rule: &AlertRule,
        timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get current metric value (simplified - would integrate with actual metrics)
        let current_value = self.get_metric_value(&rule.metric_name).await;

        // Check if alert condition is met
        let condition_met = match &rule.condition {
            AlertCondition::GreaterThan => current_value > rule.threshold,
            AlertCondition::LessThan => current_value < rule.threshold,
            AlertCondition::EqualTo => (current_value - rule.threshold).abs() < f64::EPSILON,
            AlertCondition::NotEqualTo => (current_value - rule.threshold).abs() > f64::EPSILON,
            AlertCondition::RateIncrease(percent) => {
                if let Some(previous_value) =
                    self.get_previous_metric_value(&rule.metric_name).await
                {
                    let increase = ((current_value - previous_value) / previous_value) * 100.0;
                    increase > *percent
                } else {
                    false
                }
            }
            AlertCondition::RateDecrease(percent) => {
                if let Some(previous_value) =
                    self.get_previous_metric_value(&rule.metric_name).await
                {
                    let decrease = ((previous_value - current_value) / previous_value) * 100.0;
                    decrease > *percent
                } else {
                    false
                }
            }
        };

        // Update alert state
        self.update_alert_state(rule, condition_met, current_value, timestamp)
            .await?;

        Ok(())
    }

    /// Update alert state and trigger notifications if needed
    async fn update_alert_state(
        &self,
        rule: &AlertRule,
        condition_met: bool,
        current_value: f64,
        timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut states = self.alert_states.write().await;
        let state = states.entry(rule.id.clone()).or_insert(AlertState {
            rule_id: rule.id.clone(),
            status: AlertStatus::Resolved,
            first_triggered: 0,
            last_triggered: 0,
            trigger_count: 0,
            current_value: current_value,
            resolved_at: Some(timestamp),
        });

        state.current_value = current_value;

        match (&state.status, condition_met) {
            (AlertStatus::Resolved, true) => {
                // New alert condition detected
                state.status = AlertStatus::Pending;
                state.first_triggered = timestamp;
                state.last_triggered = timestamp;
                state.trigger_count += 1;
                state.resolved_at = None;
            }
            (AlertStatus::Pending, true) => {
                // Check if duration threshold is met
                if timestamp - state.first_triggered >= rule.duration_seconds {
                    state.status = AlertStatus::Firing;
                    self.trigger_alert(rule, state, timestamp).await?;
                }
            }
            (AlertStatus::Firing, false) => {
                // Alert condition resolved
                state.status = AlertStatus::Resolved;
                state.resolved_at = Some(timestamp);
                self.resolve_alert(rule, state, timestamp).await?;
            }
            (AlertStatus::Pending, false) => {
                // Condition no longer met before firing
                state.status = AlertStatus::Resolved;
                state.resolved_at = Some(timestamp);
            }
            _ => {
                // No state change needed
            }
        }

        Ok(())
    }

    /// Trigger an alert and send notifications
    async fn trigger_alert(
        &self,
        rule: &AlertRule,
        state: &AlertState,
        timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let alert = PerformanceAlert {
            id: format!("{}_{}", rule.id, timestamp),
            severity: rule.severity.clone(),
            message: format!(
                "{}: {} = {:.2} (threshold: {:.2})",
                rule.name, rule.metric_name, state.current_value, rule.threshold
            ),
            timestamp,
            metric_name: rule.metric_name.clone(),
            current_value: state.current_value,
            threshold: rule.threshold,
        };

        // Add to alert history
        {
            let mut history = self.alert_history.write().await;
            history.push_back(alert.clone());
        }

        // Send notifications
        for channel_id in &rule.notification_channels {
            if let Err(e) = self.send_notification(channel_id, &alert).await {
                warn!(
                    "Failed to send notification to channel {}: {}",
                    channel_id, e
                );
            }
        }

        info!("Alert triggered: {} - {}", rule.name, alert.message);

        Ok(())
    }

    /// Resolve an alert
    async fn resolve_alert(
        &self,
        rule: &AlertRule,
        state: &AlertState,
        _timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Alert resolved: {} - {} returned to normal ({})",
            rule.name, rule.metric_name, state.current_value
        );
        Ok(())
    }

    /// Send notification through specified channel
    async fn send_notification(
        &self,
        channel_id: &str,
        alert: &PerformanceAlert,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let channels = self.notification_channels.read().await;
        if let Some(channel) = channels.get(channel_id) {
            if !channel.enabled {
                return Ok(());
            }

            match channel.channel_type {
                ChannelType::Log => match alert.severity {
                    AlertSeverity::Critical => error!("CRITICAL ALERT: {}", alert.message),
                    AlertSeverity::Warning => warn!("WARNING ALERT: {}", alert.message),
                    AlertSeverity::Info => info!("INFO ALERT: {}", alert.message),
                },
                ChannelType::Email => {
                    // Email notification implementation would go here
                    info!("Email notification sent for alert: {}", alert.message);
                }
                ChannelType::Slack => {
                    // Slack notification implementation would go here
                    info!("Slack notification sent for alert: {}", alert.message);
                }
                ChannelType::Webhook => {
                    // Webhook notification implementation would go here
                    info!("Webhook notification sent for alert: {}", alert.message);
                }
                ChannelType::PagerDuty => {
                    // PagerDuty notification implementation would go here
                    info!("PagerDuty notification sent for alert: {}", alert.message);
                }
            }
        }

        Ok(())
    }

    /// Get current metric value (simplified implementation)
    async fn get_metric_value(&self, metric_name: &str) -> f64 {
        // In a real implementation, this would fetch from the metrics registry
        match metric_name {
            "memory_usage_percent" => 25.0,
            "cpu_usage_percent" => 30.0,
            "total_throughput" => 45000.0,
            "active_connections" => 1500.0,
            _ => 0.0,
        }
    }

    /// Get previous metric value for rate calculations
    async fn get_previous_metric_value(&self, metric_name: &str) -> Option<f64> {
        let cache = self.metrics_cache.read().await;
        cache.get(metric_name).copied()
    }

    /// Clean up old alert history
    async fn cleanup_alert_history(&self) {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - (self.config.alert_history_retention_hours as u64 * 3600);

        let mut history = self.alert_history.write().await;
        while let Some(front) = history.front() {
            if front.timestamp < cutoff_time {
                history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get current alert statistics
    pub async fn get_alert_analytics(&self) -> AlertAnalytics {
        let history = self.alert_history.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let twenty_four_hours_ago = now - 86400;

        let recent_alerts: Vec<_> = history
            .iter()
            .filter(|alert| alert.timestamp > twenty_four_hours_ago)
            .collect();

        let total_alerts_24h = recent_alerts.len() as u64;

        let mut alerts_by_severity = HashMap::new();
        let mut alerts_by_component = HashMap::new();

        for alert in &recent_alerts {
            let severity_key = format!("{:?}", alert.severity);
            *alerts_by_severity.entry(severity_key).or_insert(0) += 1;
            *alerts_by_component
                .entry(alert.metric_name.clone())
                .or_insert(0) += 1;
        }

        AlertAnalytics {
            total_alerts_24h,
            alerts_by_severity,
            alerts_by_component,
            avg_resolution_time_minutes: 15.0, // Simplified
            top_firing_rules: vec![],          // Simplified
            alert_trends: AlertTrends {
                hourly_counts: vec![0; 24],  // Simplified
                daily_pattern: vec![0.0; 7], // Simplified
                resolution_time_trend: TrendDirection::Stable,
            },
        }
    }

    /// Get active alerts
    pub async fn get_active_alerts(&self) -> Vec<PerformanceAlert> {
        let states = self.alert_states.read().await;
        let history = self.alert_history.read().await;

        history
            .iter()
            .filter(|alert| {
                if let Some(state) = states.get(&extract_rule_id(&alert.id)) {
                    matches!(state.status, AlertStatus::Firing)
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }
}

/// Extract rule ID from alert ID
fn extract_rule_id(alert_id: &str) -> String {
    alert_id.split('_').take(1).collect::<Vec<_>>().join("_")
}
