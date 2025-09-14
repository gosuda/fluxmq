# Contributing to FluxMQ

Thank you for your interest in contributing to FluxMQ! We welcome contributions from everyone, whether you're fixing a bug, adding a feature, improving documentation, or helping with testing.

## 🚀 Getting Started

### Prerequisites

- **Rust 1.70+**: Install from [rustup.rs](https://rustup.rs/)
- **Git**: For version control
- **Basic knowledge of**: Rust, async programming, and message queuing concepts

### Development Setup

1. **Fork the repository**
   ```bash
   # Fork on GitHub, then clone your fork
   git clone https://github.com/gosuda/fluxmq.git
   cd fluxmq
   ```

2. **Set up the development environment**
   ```bash
   # Install development tools
   cargo install cargo-watch cargo-audit cargo-clippy rustfmt
   
   # Build the project
   cargo build
   
   # Run tests to ensure everything works
   cargo test
   ```

3. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-number
   ```

## 🎯 How to Contribute

### 🐛 Reporting Bugs

Before creating a bug report, please check the [existing issues](https://github.com/gosuda/fluxmq/issues) to avoid duplicates.

**When reporting a bug, please include:**
- FluxMQ version
- Rust version (`rustc --version`)
- Operating system and version
- Steps to reproduce the issue
- Expected vs actual behavior
- Any relevant logs or error messages

**Use this template:**
```markdown
**Bug Description:**
A clear description of what the bug is.

**To Reproduce:**
1. Start FluxMQ with `...`
2. Send a message `...`
3. Observe error `...`

**Expected Behavior:**
What you expected to happen.

**Environment:**
- FluxMQ version: 
- Rust version: 
- OS: 

**Logs:**
```
[Include relevant logs here]
```
```

### 💡 Suggesting Features

We love new ideas! Before suggesting a feature:

1. Check if it's already been suggested in [issues](https://github.com/gosuda/fluxmq/issues)
2. Consider if it fits FluxMQ's goals (Kafka compatibility, performance, reliability)
3. Think about the implementation complexity and maintenance burden

**Feature request template:**
```markdown
**Feature Description:**
Clear description of the feature you'd like to see.

**Use Case:**
Why would this feature be useful? What problem does it solve?

**Proposed Implementation:**
How do you think this should work?

**Alternatives:**
Have you considered any alternatives?

**Additional Context:**
Any other relevant information.
```

### 🔧 Code Contributions

#### Types of Contributions We Welcome

- **Bug fixes**: Always appreciated!
- **Performance improvements**: Benchmarks and optimizations
- **New features**: Please discuss in an issue first
- **Documentation**: Code comments, README updates, examples
- **Tests**: Unit tests, integration tests, performance tests
- **Refactoring**: Code quality improvements

#### Code Style and Standards

**Rust Conventions:**
```bash
# Format your code
cargo fmt

# Check for common mistakes
cargo clippy

# Ensure all tests pass
cargo test

# Check for security vulnerabilities
cargo audit
```

**Code Quality Requirements:**
- **All tests must pass**: 86+ tests should remain green
- **No clippy warnings**: Code should be clippy-clean
- **Proper error handling**: Use `Result<T, E>` appropriately
- **Documentation**: Public APIs must be documented
- **Performance**: Avoid regressions, add benchmarks for critical paths

**Specific Guidelines:**
```rust
// ✅ Good: Proper error handling
pub fn create_topic(&self, name: &str) -> Result<(), FluxmqError> {
    if name.is_empty() {
        return Err(FluxmqError::InvalidTopicName);
    }
    // ... implementation
}

// ❌ Bad: Using unwrap() in production code
pub fn create_topic(&self, name: &str) {
    assert!(!name.is_empty()); // Don't use in production
    // ... implementation
}

// ✅ Good: Documented public API
/// Creates a new topic with the specified name and partition count.
/// 
/// # Arguments
/// * `name` - The topic name (must be non-empty)
/// * `partitions` - Number of partitions (must be > 0)
/// 
/// # Errors
/// Returns `FluxmqError::InvalidTopicName` if name is empty.
pub fn create_topic(&self, name: &str, partitions: u32) -> Result<(), FluxmqError>
```

#### Testing Guidelines

**Test Coverage Requirements:**
- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test component interactions
- **Performance tests**: Ensure no regressions
- **Error handling tests**: Test failure scenarios

**Writing Good Tests:**
```rust
#[tokio::test]
async fn test_produce_message_success() {
    // Arrange
    let handler = MessageHandler::new().unwrap();
    let request = ProduceRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        messages: vec![create_test_message("key", "value")],
        // ... other fields
    };
    
    // Act
    let response = handler.handle_produce(request).await.unwrap();
    
    // Assert
    assert_eq!(response.error_code, 0);
    assert_eq!(response.topic, "test-topic");
    assert!(response.base_offset >= 0);
}
```

**Test Organization:**
```bash
# Run all tests
cargo test

# Run specific test modules
cargo test storage::tests
cargo test consumer::tests
cargo test integration_tests

# Run performance benchmarks
cargo test --release -- --ignored benchmark
```

#### Performance Considerations

FluxMQ is a high-performance system. When contributing:

**Memory Management:**
- Use `bytes::Bytes` for zero-copy message handling
- Avoid unnecessary allocations in hot paths
- Use `Arc<T>` for shared data structures
- Prefer stack allocation over heap when possible

**Async Programming:**
- Use async/await properly with Tokio
- Don't block the async runtime with CPU-intensive work
- Use channels for inter-task communication
- Prefer async locks over sync locks

**Performance Testing:**
```bash
# Run benchmarks before and after your changes
cargo test --release -- --ignored benchmark

# Profile your changes
cargo build --release
# Use profiling tools like `perf` or `flamegraph`
```

### 📝 Documentation Contributions

**Types of Documentation:**
- **Code comments**: Explain complex logic
- **API documentation**: Document public interfaces
- **Examples**: Practical usage examples
- **README updates**: Keep installation/usage current
- **Architecture docs**: Explain system design

**Documentation Standards:**
```rust
/// Handles a produce request by storing messages in the appropriate partition.
/// 
/// This method performs the following steps:
/// 1. Validates the topic and partition
/// 2. Assigns partition if auto-assignment is requested (partition = u32::MAX)
/// 3. Stores messages in the storage layer
/// 4. Updates replication if enabled
/// 
/// # Arguments
/// * `request` - The produce request containing topic, partition, and messages
/// 
/// # Returns
/// * `Ok(Response::Produce)` - Success response with assigned partition and base offset
/// * `Err(FluxmqError)` - Error if validation fails or storage operation fails
/// 
/// # Examples
/// ```rust
/// let request = ProduceRequest {
///     topic: "my-topic".to_string(),
///     partition: u32::MAX, // Auto-assign
///     messages: vec![message],
///     // ...
/// };
/// let response = handler.handle_produce(request).await?;
/// ```
pub async fn handle_produce(&self, request: ProduceRequest) -> Result<Response, FluxmqError>
```

## 🔄 Development Workflow

### 1. **Before You Start**
```bash
# Sync with upstream
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/amazing-feature
```

### 2. **During Development**
```bash
# Run tests frequently
cargo test

# Check code style
cargo fmt --check
cargo clippy

# Watch for changes during development
cargo watch -x check -x test
```

### 3. **Before Submitting**
```bash
# Ensure all tests pass
cargo test

# Check formatting and linting
cargo fmt
cargo clippy

# Security audit
cargo audit

# Update documentation if needed
# Add examples for new features
# Update CHANGELOG.md if applicable
```

### 4. **Submitting Pull Request**

**PR Title Format:**
- `feat: add consumer group sticky assignment`
- `fix: resolve partition assignment race condition`
- `docs: update installation instructions`
- `perf: optimize message serialization`
- `test: add integration tests for replication`

**PR Description Template:**
```markdown
## Changes
- Brief description of what this PR does

## Motivation
- Why is this change needed?
- What problem does it solve?

## Testing
- [ ] All existing tests pass
- [ ] New tests added for new functionality
- [ ] Manual testing performed

## Performance Impact
- [ ] No performance regression
- [ ] Benchmarks included (if applicable)

## Breaking Changes
- [ ] No breaking changes
- [ ] Breaking changes documented

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] CHANGELOG.md updated (if needed)
```

## 🏗️ Project Structure

Understanding the codebase structure will help you contribute effectively:

```
src/
├── main.rs                 # CLI entry point
├── lib.rs                  # Library exports
├── broker/
│   ├── handler.rs          # Core request handling logic
│   └── server.rs           # TCP server implementation
├── storage/
│   ├── mod.rs             # Storage abstractions
│   ├── log.rs             # Append-only log implementation
│   ├── segment.rs         # Log segment management
│   └── index.rs           # Offset indexing
├── protocol/
│   ├── messages.rs        # Protocol message definitions
│   ├── codec.rs           # Server-side encoding/decoding
│   └── client_codec.rs    # Client-side encoding/decoding
├── replication/
│   ├── mod.rs             # Replication coordinator
│   ├── leader.rs          # Leader state management
│   └── follower.rs        # Follower synchronization
├── consumer/
│   ├── mod.rs             # Consumer group types
│   └── coordinator.rs     # Group coordination logic
└── topic_manager.rs       # Topic and partition management
```

**Key Areas for Contributions:**

1. **Performance Critical**: `storage/`, `protocol/codec.rs`
2. **Feature Rich**: `consumer/`, `replication/`
3. **User Facing**: `main.rs`, examples/
4. **Testing**: All `tests.rs` files

## 🧪 Testing Strategy

### Test Categories

**Unit Tests (`#[test]`):**
- Test individual functions and methods
- Mock external dependencies
- Fast execution (< 1ms per test)

**Integration Tests (`tests/`):**
- Test component interactions
- Use real storage and network
- Moderate execution time

**Performance Tests (`#[ignore]`):**
- Benchmark critical paths
- Ensure no performance regressions
- Run with `cargo test --release -- --ignored benchmark`

### Writing Effective Tests

```rust
// Good test example
#[tokio::test]
async fn test_consumer_group_rebalancing() {
    // Arrange: Set up test environment
    let coordinator = setup_test_coordinator().await;
    let consumer1 = create_test_consumer("consumer-1");
    let consumer2 = create_test_consumer("consumer-2");
    
    // Act: Perform the operation being tested
    coordinator.join_group(consumer1).await.unwrap();
    coordinator.join_group(consumer2).await.unwrap();
    
    // Assert: Verify expected outcomes
    let group = coordinator.get_group("test-group").await.unwrap();
    assert_eq!(group.members.len(), 2);
    assert_eq!(group.state, ConsumerGroupState::Stable);
}
```

## 🚀 Release Process

For maintainers and regular contributors:

### Version Numbering
We follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **Major**: Breaking changes
- **Minor**: New features (backwards compatible)
- **Patch**: Bug fixes (backwards compatible)

### Release Checklist
- [ ] All tests pass on CI
- [ ] Performance benchmarks stable
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in `Cargo.toml`
- [ ] Git tag created
- [ ] GitHub release created
- [ ] Crates.io published (if applicable)

## 🏷️ Issue Labels

Understanding our issue labels helps you find ways to contribute:

- **`good first issue`**: Easy for newcomers
- **`help wanted`**: We'd love community help
- **`bug`**: Something isn't working
- **`feature`**: New feature request
- **`performance`**: Performance-related issue
- **`documentation`**: Documentation needs
- **`test`**: Testing improvements needed

## 💬 Communication

### Getting Help

- **GitHub Discussions**: General questions and ideas
- **GitHub Issues**: Specific bugs or feature requests
- **Email**: hsng95@gmail.com for private matters

### Code Review Process

1. **Automated checks**: CI must pass
2. **Maintainer review**: At least one maintainer approval
3. **Community feedback**: Other contributors may provide input
4. **Merge**: Squash and merge preferred

### Response Times

- **Issues**: We aim to respond within 48 hours
- **PRs**: Initial review within 72 hours
- **Discussions**: Community-driven, no specific timeline

## 🎉 Recognition

Contributors are recognized in several ways:

- **README.md**: Major contributors listed
- **CHANGELOG.md**: Feature contributions noted
- **GitHub**: Contributor status and badges
- **Releases**: Thank you notes in release announcements

## 📋 Contributor License Agreement

By contributing to FluxMQ, you agree that:

1. You have the right to contribute the code
2. Your contributions will be licensed under the same license as the project (MIT)
3. You understand that your contributions may be used commercially

## 🤔 Questions?

Don't hesitate to ask questions! We're here to help:

- **New to Rust?** Check out [The Rust Book](https://doc.rust-lang.org/book/)
- **New to async programming?** See [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- **New to message queues?** Read about [Apache Kafka](https://kafka.apache.org/documentation/)

Thank you for contributing to FluxMQ! 🚀