//! Access Control List (ACL) system for FluxMQ
//!
//! This module provides fine-grained access control for Kafka resources including
//! topics, consumer groups, and cluster operations. It supports user-based and
//! role-based access control with configurable permissions.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{debug, info, warn};

/// ACL-related errors
#[derive(Error, Debug)]
pub enum AclError {
    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Invalid principal: {0}")]
    InvalidPrincipal(String),

    #[error("Invalid resource pattern: {0}")]
    InvalidResourcePattern(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

/// Types of Kafka resources that can be controlled by ACLs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// Kafka topics
    Topic,
    /// Consumer groups
    Group,
    /// Cluster-wide operations
    Cluster,
    /// Transactional ID for exactly-once semantics
    TransactionalId,
    /// Delegation tokens
    DelegationToken,
}

/// Pattern types for resource matching
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PatternType {
    /// Exact match
    Literal,
    /// Prefix match (e.g., "logs-*")
    Prefixed,
    /// Any resource of the given type
    Any,
}

/// Operations that can be performed on resources
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Operation {
    /// Read data from topics, fetch offsets, describe topics/groups
    Read,
    /// Write data to topics, commit offsets
    Write,
    /// Create topics, create/delete consumer groups
    Create,
    /// Delete topics
    Delete,
    /// Modify topic configurations, group memberships
    Alter,
    /// Describe topic/group metadata
    Describe,
    /// List topics, groups
    DescribeConfigs,
    /// Modify configurations
    AlterConfigs,
    /// Cluster admin operations
    ClusterAction,
    /// Idempotent write operations
    IdempotentWrite,
    /// All operations (superuser)
    All,
}

/// Permission type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Grant permission
    Allow,
    /// Explicitly deny permission
    Deny,
}

/// Principal representing a user or service account
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    /// Principal type (User, ServiceAccount)
    pub principal_type: String,
    /// Principal name
    pub name: String,
}

impl Principal {
    pub fn user(name: &str) -> Self {
        Self {
            principal_type: "User".to_string(),
            name: name.to_string(),
        }
    }

    pub fn service_account(name: &str) -> Self {
        Self {
            principal_type: "ServiceAccount".to_string(),
            name: name.to_string(),
        }
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

/// Resource pattern for ACL matching
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourcePattern {
    /// Type of resource
    pub resource_type: ResourceType,
    /// Resource name or pattern
    pub name: String,
    /// Pattern matching type
    pub pattern_type: PatternType,
}

impl ResourcePattern {
    pub fn topic_literal(name: &str) -> Self {
        Self {
            resource_type: ResourceType::Topic,
            name: name.to_string(),
            pattern_type: PatternType::Literal,
        }
    }

    pub fn topic_prefixed(prefix: &str) -> Self {
        Self {
            resource_type: ResourceType::Topic,
            name: prefix.to_string(),
            pattern_type: PatternType::Prefixed,
        }
    }

    pub fn group_literal(name: &str) -> Self {
        Self {
            resource_type: ResourceType::Group,
            name: name.to_string(),
            pattern_type: PatternType::Literal,
        }
    }

    pub fn cluster() -> Self {
        Self {
            resource_type: ResourceType::Cluster,
            name: "kafka-cluster".to_string(),
            pattern_type: PatternType::Literal,
        }
    }

    /// Check if this pattern matches a given resource
    pub fn matches(&self, resource_type: &ResourceType, resource_name: &str) -> bool {
        if &self.resource_type != resource_type {
            return false;
        }

        match self.pattern_type {
            PatternType::Literal => self.name == resource_name,
            PatternType::Prefixed => resource_name.starts_with(&self.name),
            PatternType::Any => true,
        }
    }
}

/// Access Control Entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    /// Principal this ACL applies to
    pub principal: Principal,
    /// Resource pattern
    pub resource_pattern: ResourcePattern,
    /// Operation being controlled
    pub operation: Operation,
    /// Permission type (Allow/Deny)
    pub permission: Permission,
    /// Host restriction (optional, "*" means any host)
    pub host: String,
}

impl AclEntry {
    pub fn new(
        principal: Principal,
        resource_pattern: ResourcePattern,
        operation: Operation,
        permission: Permission,
    ) -> Self {
        Self {
            principal,
            resource_pattern,
            operation,
            permission,
            host: "*".to_string(), // Allow from any host by default
        }
    }

    pub fn with_host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }
}

/// ACL Authorization result
#[derive(Debug, PartialEq)]
pub enum AuthorizationResult {
    /// Access is explicitly allowed
    Allowed,
    /// Access is explicitly denied
    Denied,
    /// No matching ACL found (default behavior depends on configuration)
    NoMatch,
}

/// ACL Manager for handling authorization decisions
pub struct AclManager {
    /// All ACL entries
    acls: Vec<AclEntry>,
    /// Whether to allow operations when no ACL matches (default: false for security)
    allow_everyone: bool,
    /// Super users who bypass all ACL checks
    super_users: HashSet<Principal>,
    /// Index for fast lookups
    principal_index: HashMap<Principal, Vec<usize>>,
}

impl AclManager {
    /// Create a new ACL manager
    pub fn new(allow_everyone: bool) -> Self {
        Self {
            acls: Vec::new(),
            allow_everyone,
            super_users: HashSet::new(),
            principal_index: HashMap::new(),
        }
    }

    /// Add a super user who bypasses all ACL checks
    pub fn add_super_user(&mut self, principal: Principal) {
        info!("Adding super user: {}", principal);
        self.super_users.insert(principal);
    }

    /// Add an ACL entry
    pub fn add_acl(&mut self, acl: AclEntry) {
        debug!("Adding ACL: {:?}", acl);

        let index = self.acls.len();
        self.principal_index
            .entry(acl.principal.clone())
            .or_insert_with(Vec::new)
            .push(index);

        self.acls.push(acl);
    }

    /// Remove ACL entries matching the criteria
    pub fn remove_acls(
        &mut self,
        principal: &Principal,
        resource_pattern: &ResourcePattern,
        operation: &Operation,
    ) -> usize {
        let initial_len = self.acls.len();

        self.acls.retain(|acl| {
            !(acl.principal == *principal
                && acl.resource_pattern == *resource_pattern
                && acl.operation == *operation)
        });

        // Rebuild index
        self.rebuild_index();

        let removed = initial_len - self.acls.len();
        if removed > 0 {
            info!("Removed {} ACL entries for {}", removed, principal);
        }
        removed
    }

    /// Check if a principal is authorized to perform an operation on a resource
    pub fn authorize(
        &self,
        principal: &Principal,
        resource_type: &ResourceType,
        resource_name: &str,
        operation: &Operation,
        host: Option<&str>,
    ) -> AuthorizationResult {
        // Super users bypass all checks
        if self.super_users.contains(principal) {
            debug!(
                "Allowing super user {} for {:?}:{} operation {:?}",
                principal, resource_type, resource_name, operation
            );
            return AuthorizationResult::Allowed;
        }

        // Get ACLs for this principal
        let acl_indices = match self.principal_index.get(principal) {
            Some(indices) => indices,
            None => {
                return if self.allow_everyone {
                    debug!(
                        "No ACLs found for {}, allowing due to allow_everyone=true",
                        principal
                    );
                    AuthorizationResult::Allowed
                } else {
                    debug!(
                        "No ACLs found for {}, denying due to allow_everyone=false",
                        principal
                    );
                    AuthorizationResult::NoMatch
                };
            }
        };

        let mut has_allow = false;
        let host_str = host.unwrap_or("*");

        // Check all matching ACLs
        for &index in acl_indices {
            let acl = &self.acls[index];

            // Check if this ACL matches the request
            if !acl.resource_pattern.matches(resource_type, resource_name) {
                continue;
            }

            // Check if operation matches (All operation matches everything)
            if acl.operation != *operation && acl.operation != Operation::All {
                continue;
            }

            // Check host restriction
            if acl.host != "*" && acl.host != host_str {
                continue;
            }

            // Check permission
            match acl.permission {
                Permission::Deny => {
                    warn!(
                        "Denying {} access to {:?}:{} operation {:?} due to explicit DENY ACL",
                        principal, resource_type, resource_name, operation
                    );
                    return AuthorizationResult::Denied;
                }
                Permission::Allow => {
                    has_allow = true;
                }
            }
        }

        if has_allow {
            debug!(
                "Allowing {} access to {:?}:{} operation {:?}",
                principal, resource_type, resource_name, operation
            );
            AuthorizationResult::Allowed
        } else if self.allow_everyone {
            debug!(
                "No specific ACL for {}, allowing due to allow_everyone=true",
                principal
            );
            AuthorizationResult::Allowed
        } else {
            debug!("No ALLOW ACL found for {}, denying access", principal);
            AuthorizationResult::NoMatch
        }
    }

    /// List all ACL entries for a principal
    pub fn list_acls_for_principal(&self, principal: &Principal) -> Vec<&AclEntry> {
        match self.principal_index.get(principal) {
            Some(indices) => indices.iter().map(|&i| &self.acls[i]).collect(),
            None => Vec::new(),
        }
    }

    /// List all ACL entries
    pub fn list_all_acls(&self) -> &[AclEntry] {
        &self.acls
    }

    /// Rebuild the principal index after modifications
    fn rebuild_index(&mut self) {
        self.principal_index.clear();

        for (index, acl) in self.acls.iter().enumerate() {
            self.principal_index
                .entry(acl.principal.clone())
                .or_insert_with(Vec::new)
                .push(index);
        }
    }

    /// Save ACLs to a JSON file
    pub fn save_to_file(&self, path: &str) -> Result<(), AclError> {
        let json = serde_json::to_string_pretty(&self.acls)?;
        std::fs::write(path, json)?;
        info!("Saved {} ACL entries to {}", self.acls.len(), path);
        Ok(())
    }

    /// Load ACLs from a JSON file
    pub fn load_from_file(&mut self, path: &str) -> Result<(), AclError> {
        let content = std::fs::read_to_string(path)?;
        let loaded_acls: Vec<AclEntry> = serde_json::from_str(&content)?;

        for acl in loaded_acls {
            self.add_acl(acl);
        }

        info!("Loaded {} ACL entries from {}", self.acls.len(), path);
        Ok(())
    }
}

/// Default ACL policies for common scenarios
pub struct AclPolicyBuilder;

impl AclPolicyBuilder {
    /// Create ACLs for a regular user with topic access
    pub fn user_topic_access(username: &str, topic: &str) -> Vec<AclEntry> {
        let user = Principal::user(username);
        let topic_pattern = ResourcePattern::topic_literal(topic);

        vec![
            AclEntry::new(
                user.clone(),
                topic_pattern.clone(),
                Operation::Read,
                Permission::Allow,
            ),
            AclEntry::new(
                user.clone(),
                topic_pattern.clone(),
                Operation::Write,
                Permission::Allow,
            ),
            AclEntry::new(user, topic_pattern, Operation::Describe, Permission::Allow),
        ]
    }

    /// Create ACLs for a service account with admin privileges
    pub fn service_admin(service_name: &str) -> Vec<AclEntry> {
        let service = Principal::service_account(service_name);
        let cluster_pattern = ResourcePattern::cluster();

        vec![
            AclEntry::new(
                service.clone(),
                cluster_pattern,
                Operation::All,
                Permission::Allow,
            ),
            AclEntry::new(
                service.clone(),
                ResourcePattern {
                    resource_type: ResourceType::Topic,
                    name: "*".to_string(),
                    pattern_type: PatternType::Any,
                },
                Operation::All,
                Permission::Allow,
            ),
            AclEntry::new(
                service,
                ResourcePattern {
                    resource_type: ResourceType::Group,
                    name: "*".to_string(),
                    pattern_type: PatternType::Any,
                },
                Operation::All,
                Permission::Allow,
            ),
        ]
    }

    /// Create ACLs for read-only monitoring user
    pub fn monitoring_user(username: &str) -> Vec<AclEntry> {
        let user = Principal::user(username);

        vec![
            AclEntry::new(
                user.clone(),
                ResourcePattern::cluster(),
                Operation::Describe,
                Permission::Allow,
            ),
            AclEntry::new(
                user.clone(),
                ResourcePattern {
                    resource_type: ResourceType::Topic,
                    name: "*".to_string(),
                    pattern_type: PatternType::Any,
                },
                Operation::Describe,
                Permission::Allow,
            ),
            AclEntry::new(
                user,
                ResourcePattern {
                    resource_type: ResourceType::Group,
                    name: "*".to_string(),
                    pattern_type: PatternType::Any,
                },
                Operation::Describe,
                Permission::Allow,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_pattern_matching() {
        let literal_pattern = ResourcePattern::topic_literal("test-topic");
        assert!(literal_pattern.matches(&ResourceType::Topic, "test-topic"));
        assert!(!literal_pattern.matches(&ResourceType::Topic, "other-topic"));

        let prefixed_pattern = ResourcePattern::topic_prefixed("logs-");
        assert!(prefixed_pattern.matches(&ResourceType::Topic, "logs-app1"));
        assert!(prefixed_pattern.matches(&ResourceType::Topic, "logs-app2"));
        assert!(!prefixed_pattern.matches(&ResourceType::Topic, "metrics-app1"));
    }

    #[test]
    fn test_acl_authorization() {
        let mut acl_manager = AclManager::new(false);
        let user = Principal::user("testuser");

        // Add allow ACL for specific topic
        acl_manager.add_acl(AclEntry::new(
            user.clone(),
            ResourcePattern::topic_literal("allowed-topic"),
            Operation::Read,
            Permission::Allow,
        ));

        // Test allowed access
        assert_eq!(
            acl_manager.authorize(
                &user,
                &ResourceType::Topic,
                "allowed-topic",
                &Operation::Read,
                None
            ),
            AuthorizationResult::Allowed
        );

        // Test denied access (no ACL)
        assert_eq!(
            acl_manager.authorize(
                &user,
                &ResourceType::Topic,
                "other-topic",
                &Operation::Read,
                None
            ),
            AuthorizationResult::NoMatch
        );
    }

    #[test]
    fn test_super_user() {
        let mut acl_manager = AclManager::new(false);
        let admin = Principal::user("admin");
        acl_manager.add_super_user(admin.clone());

        // Super user should have access to everything
        assert_eq!(
            acl_manager.authorize(
                &admin,
                &ResourceType::Topic,
                "any-topic",
                &Operation::All,
                None
            ),
            AuthorizationResult::Allowed
        );
    }

    #[test]
    fn test_deny_override() {
        let mut acl_manager = AclManager::new(false);
        let user = Principal::user("testuser");

        // Add both allow and deny ACLs
        acl_manager.add_acl(AclEntry::new(
            user.clone(),
            ResourcePattern::topic_literal("test-topic"),
            Operation::Read,
            Permission::Allow,
        ));

        acl_manager.add_acl(AclEntry::new(
            user.clone(),
            ResourcePattern::topic_literal("test-topic"),
            Operation::Read,
            Permission::Deny,
        ));

        // Deny should override allow
        assert_eq!(
            acl_manager.authorize(
                &user,
                &ResourceType::Topic,
                "test-topic",
                &Operation::Read,
                None
            ),
            AuthorizationResult::Denied
        );
    }
}
