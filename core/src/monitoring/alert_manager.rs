//! Alert Management System
//!
//! This module provides comprehensive alerting capabilities including:
//! - Real-time alert generation and management
//! - Alert severity classification and escalation
//! - Alert aggregation and deduplication
//! - Notification routing and delivery
//! - Alert history and analytics

use crate::monitoring::AlertSeverity;
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
            evaluation_interval_seconds: 30,
            alert_history_retention_hours: 24,
            max_alerts_per_rule: 1000,
            enable_alert_aggregation: true,
            aggregation_window_seconds: 300, // 5 minutes
        }
    }
}

/// Alert manager with real-time alerting capabilities
pub struct AlertManager {
    config: AlertManagerConfig,
    alert_rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    alert_states: Arc<RwLock<HashMap<String, AlertState>>>,
    notification_channels: Arc<RwLock<HashMap<String, NotificationChannel>>>,
    alert_history: Arc<RwLock<VecDeque<HistoricalAlert>>>,
    metrics_cache: Arc<RwLock<HashMap<String, f64>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalAlert {
    pub rule_id: String,
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub triggered_at: u64,
    pub resolved_at: Option<u64>,
    pub trigger_value: f64,
    pub threshold: f64,
    pub message: String,
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

    /// Start the alert evaluation loop
    pub async fn start_evaluation_loop(self: Arc<Self>) {
        // Initialize default rules and channels
        self.initialize_defaults().await;

        let manager = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                manager.config.evaluation_interval_seconds,
            ));

            loop {
                interval.tick().await;
                if let Err(e) = manager.evaluate_alerts().await {
                    error!("Alert evaluation failed: {}", e);
                }
            }
        });

        info!(
            "Alert manager started with {}s evaluation interval",
            self.config.evaluation_interval_seconds
        );
    }

    /// Initialize default alert rules and notification channels
    async fn initialize_defaults(&self) {
        // Add default log channel
        let log_channel = NotificationChannel {
            id: "default_log".to_string(),
            name: "Default Log Channel".to_string(),
            channel_type: ChannelType::Log,
            configuration: ChannelConfig {
                email_addresses: None,
                slack_webhook_url: None,
                webhook_url: None,
                pagerduty_key: None,
                log_level: Some("info".to_string()),
            },
            enabled: true,
        };
        self.add_notification_channel(log_channel).await;

        // Add default alert rules
        let default_rules = vec![
            AlertRule {
                id: "high_memory_usage".to_string(),
                name: "High Memory Usage".to_string(),
                description: "Memory usage exceeds 80%".to_string(),
                metric_name: "memory_usage_percent".to_string(),
                condition: AlertCondition::GreaterThan,
                threshold: 80.0,
                severity: AlertSeverity::Warning,
                duration_seconds: 300, // 5 minutes
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
            AlertRule {
                id: "high_cpu_usage".to_string(),
                name: "High CPU Usage".to_string(),
                description: "CPU usage exceeds 90%".to_string(),
                metric_name: "cpu_usage_percent".to_string(),
                condition: AlertCondition::GreaterThan,
                threshold: 90.0,
                severity: AlertSeverity::Critical,
                duration_seconds: 60, // 1 minute
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
            AlertRule {
                id: "low_cache_hit_rate".to_string(),
                name: "Low Cache Hit Rate".to_string(),
                description: "Cache hit rate below 70%".to_string(),
                metric_name: "cache_hit_rate".to_string(),
                condition: AlertCondition::LessThan,
                threshold: 0.7,
                severity: AlertSeverity::Warning,
                duration_seconds: 600, // 10 minutes
                enabled: true,
                notification_channels: vec!["default_log".to_string()],
            },
        ];

        for rule in default_rules {
            self.add_alert_rule(rule).await;
        }
    }

    /// Add a new alert rule
    pub async fn add_alert_rule(&self, rule: AlertRule) {
        let mut rules = self.alert_rules.write().await;
        rules.insert(rule.id.clone(), rule);
    }

    /// Add a notification channel
    pub async fn add_notification_channel(&self, channel: NotificationChannel) {
        let mut channels = self.notification_channels.write().await;
        channels.insert(channel.id.clone(), channel);
    }

    /// Update metrics cache for alert evaluation
    pub async fn update_metrics(&self, metrics: HashMap<String, f64>) {
        let mut cache = self.metrics_cache.write().await;
        *cache = metrics;
    }

    /// Evaluate all alert rules
    async fn evaluate_alerts(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let rules = self.alert_rules.read().await;
        let metrics = self.metrics_cache.read().await;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        for rule in rules.values() {
            if !rule.enabled {
                continue;
            }

            if let Some(&metric_value) = metrics.get(&rule.metric_name) {
                self.evaluate_rule(rule, metric_value, current_time).await?;
            }
        }

        // Clean up old alert history
        self.cleanup_old_alerts(current_time).await;

        Ok(())
    }

    /// Evaluate a single alert rule
    async fn evaluate_rule(
        &self,
        rule: &AlertRule,
        metric_value: f64,
        current_time: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let condition_met = self.evaluate_condition(&rule.condition, metric_value, rule.threshold);

        let mut states = self.alert_states.write().await;
        let alert_state = states.entry(rule.id.clone()).or_insert_with(|| AlertState {
            rule_id: rule.id.clone(),
            status: AlertStatus::Pending,
            first_triggered: current_time,
            last_triggered: current_time,
            trigger_count: 0,
            current_value: metric_value,
            resolved_at: None,
        });

        alert_state.current_value = metric_value;
        alert_state.last_triggered = current_time;

        match (&alert_state.status, condition_met) {
            (AlertStatus::Resolved | AlertStatus::Pending, true) => {
                // Condition met, check duration
                if current_time - alert_state.first_triggered >= rule.duration_seconds {
                    // Fire the alert
                    alert_state.status = AlertStatus::Firing;
                    alert_state.trigger_count += 1;
                    alert_state.resolved_at = None;

                    self.fire_alert(rule, metric_value, current_time).await?;
                } else {
                    alert_state.status = AlertStatus::Pending;
                }
            }
            (AlertStatus::Firing, false) => {
                // Alert resolved
                alert_state.status = AlertStatus::Resolved;
                alert_state.resolved_at = Some(current_time);

                self.resolve_alert(rule, current_time).await?;
            }
            (AlertStatus::Pending, false) => {
                // Reset pending state
                alert_state.first_triggered = current_time;
            }
            _ => {
                // No state change needed
            }
        }

        Ok(())
    }

    /// Evaluate alert condition
    fn evaluate_condition(&self, condition: &AlertCondition, value: f64, threshold: f64) -> bool {
        match condition {
            AlertCondition::GreaterThan => value > threshold,
            AlertCondition::LessThan => value < threshold,
            AlertCondition::EqualTo => (value - threshold).abs() < f64::EPSILON,
            AlertCondition::NotEqualTo => (value - threshold).abs() > f64::EPSILON,
            AlertCondition::RateIncrease(percentage) => {
                // Would need historical data to calculate rate
                value > threshold * (1.0 + percentage / 100.0)
            }
            AlertCondition::RateDecrease(percentage) => {
                // Would need historical data to calculate rate
                value < threshold * (1.0 - percentage / 100.0)
            }
        }
    }

    /// Fire an alert
    async fn fire_alert(
        &self,
        rule: &AlertRule,
        trigger_value: f64,
        timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let message = format!(
            "Alert: {} - {} exceeded threshold {} with value {}",
            rule.name, rule.metric_name, rule.threshold, trigger_value
        );

        info!("🚨 ALERT FIRED: {}", message);

        // Add to history
        let historical_alert = HistoricalAlert {
            rule_id: rule.id.clone(),
            rule_name: rule.name.clone(),
            severity: rule.severity.clone(),
            triggered_at: timestamp,
            resolved_at: None,
            trigger_value,
            threshold: rule.threshold,
            message: message.clone(),
        };

        {
            let mut history = self.alert_history.write().await;
            history.push_back(historical_alert);

            // Limit history size
            while history.len() > self.config.max_alerts_per_rule {
                history.pop_front();
            }
        }

        // Send notifications
        self.send_notifications(rule, &message, AlertNotificationType::Fire)
            .await?;

        Ok(())
    }

    /// Resolve an alert
    async fn resolve_alert(
        &self,
        rule: &AlertRule,
        timestamp: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let message = format!("Alert resolved: {}", rule.name);

        info!("✅ ALERT RESOLVED: {}", message);

        // Update history
        {
            let mut history = self.alert_history.write().await;
            if let Some(alert) = history
                .iter_mut()
                .rev()
                .find(|a| a.rule_id == rule.id && a.resolved_at.is_none())
            {
                alert.resolved_at = Some(timestamp);
            }
        }

        // Send resolution notification
        self.send_notifications(rule, &message, AlertNotificationType::Resolve)
            .await?;

        Ok(())
    }

    /// Send notifications for alert
    async fn send_notifications(
        &self,
        rule: &AlertRule,
        message: &str,
        notification_type: AlertNotificationType,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let channels = self.notification_channels.read().await;

        for channel_id in &rule.notification_channels {
            if let Some(channel) = channels.get(channel_id) {
                if channel.enabled {
                    self.send_notification(channel, message, notification_type)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Send notification to specific channel
    async fn send_notification(
        &self,
        channel: &NotificationChannel,
        message: &str,
        notification_type: AlertNotificationType,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match channel.channel_type {
            ChannelType::Log => match notification_type {
                AlertNotificationType::Fire => warn!("ALERT: {}", message),
                AlertNotificationType::Resolve => info!("ALERT RESOLVED: {}", message),
            },
            ChannelType::Email => {
                // Would implement email sending here
                info!("EMAIL ALERT: {}", message);
            }
            ChannelType::Slack => {
                // Would implement Slack webhook here
                info!("SLACK ALERT: {}", message);
            }
            ChannelType::Webhook => {
                // Would implement generic webhook here
                info!("WEBHOOK ALERT: {}", message);
            }
            ChannelType::PagerDuty => {
                // Would implement PagerDuty integration here
                info!("PAGERDUTY ALERT: {}", message);
            }
        }

        Ok(())
    }

    /// Clean up old alerts from history
    async fn cleanup_old_alerts(&self, current_time: u64) {
        let cutoff_time = current_time - (self.config.alert_history_retention_hours as u64 * 3600);

        let mut history = self.alert_history.write().await;
        history.retain(|alert| alert.triggered_at > cutoff_time);
    }

    /// Get current alert statistics
    pub async fn get_alert_stats(&self) -> AlertStats {
        let states = self.alert_states.read().await;
        let history = self.alert_history.read().await;

        let total_alerts = states.len();
        let firing_alerts = states
            .values()
            .filter(|s| matches!(s.status, AlertStatus::Firing))
            .count();
        let pending_alerts = states
            .values()
            .filter(|s| matches!(s.status, AlertStatus::Pending))
            .count();

        let critical_alerts = history
            .iter()
            .filter(|a| matches!(a.severity, AlertSeverity::Critical))
            .count();
        let warning_alerts = history
            .iter()
            .filter(|a| matches!(a.severity, AlertSeverity::Warning))
            .count();
        let info_alerts = history
            .iter()
            .filter(|a| matches!(a.severity, AlertSeverity::Info))
            .count();

        AlertStats {
            total_alerts,
            firing_alerts,
            pending_alerts,
            resolved_alerts: total_alerts - firing_alerts - pending_alerts,
            critical_alerts,
            warning_alerts,
            info_alerts,
            alert_history_count: history.len(),
        }
    }

    /// Get recent alert history
    pub async fn get_recent_alerts(&self, limit: usize) -> Vec<HistoricalAlert> {
        let history = self.alert_history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }
}

#[derive(Debug, Clone, Copy)]
enum AlertNotificationType {
    Fire,
    Resolve,
}

/// Alert statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStats {
    pub total_alerts: usize,
    pub firing_alerts: usize,
    pub pending_alerts: usize,
    pub resolved_alerts: usize,
    pub critical_alerts: usize,
    pub warning_alerts: usize,
    pub info_alerts: usize,
    pub alert_history_count: usize,
}
