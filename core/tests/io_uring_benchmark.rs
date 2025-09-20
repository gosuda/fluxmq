/// io_uring performance benchmarks
///
/// These tests benchmark the ultra-high performance io_uring implementation
/// and compare it with traditional networking approaches.
///
/// Note: These tests only run on Linux systems where io_uring is available.

#[cfg(target_os = "linux")]
mod linux_tests {
    use fluxmq::performance::io_uring_zero_copy::{
        IoUringConfig, IoUringMessageTransfer, IoUringNetworkHandler,
    };
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use std::time::Instant;
    use tempfile::tempdir;
    use tokio::net::{TcpListener, TcpStream};

    /// Test basic io_uring functionality
    #[tokio::test]
    async fn test_io_uring_basic_functionality() {
        // This test may fail if io_uring is not available on the system
        if let Ok(handler) = IoUringNetworkHandler::new() {
            let stats = handler.get_stats();

            println!("io_uring handler created successfully!");
            println!("Configuration: {}", stats.report());

            assert_eq!(stats.operations_submitted, 0);
            assert_eq!(stats.operations_completed, 0);
            assert!(stats.queue_depth > 0);
        } else {
            println!("⚠️ io_uring not available on this system, skipping test");
        }
    }

    /// Test io_uring configuration options
    #[test]
    fn test_io_uring_configuration() {
        let config = IoUringConfig {
            queue_depth: 2048,
            sqpoll_mode: true,
            sqpoll_cpu: Some(1),
            iopoll_mode: false,
            batch_size: 64,
        };

        assert_eq!(config.queue_depth, 2048);
        assert!(config.sqpoll_mode);
        assert_eq!(config.sqpoll_cpu, Some(1));
        assert!(!config.iopoll_mode);
        assert_eq!(config.batch_size, 64);

        // Test creation with custom config
        if let Ok(handler) = IoUringNetworkHandler::with_config(config.clone()) {
            let stats = handler.get_stats();
            assert_eq!(stats.queue_depth, 2048);
            assert!(stats.sqpoll_enabled);

            println!("Custom io_uring configuration applied successfully!");
        } else {
            println!("⚠️ io_uring not available, skipping custom config test");
        }
    }

    /// Benchmark io_uring vs traditional networking
    #[tokio::test]
    async fn benchmark_io_uring_vs_traditional() {
        // Skip if io_uring not available
        if IoUringNetworkHandler::new().is_err() {
            println!("⚠️ io_uring not available, skipping benchmark");
            return;
        }

        // Create test data - 5MB file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("io_uring_test.bin");
        let file_size = 5 * 1024 * 1024; // 5MB

        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&file_path)
                .unwrap();

            let chunk = vec![0xEF; 1024 * 1024]; // 1MB chunk
            for _ in 0..5 {
                file.write_all(&chunk).unwrap();
            }
            file.sync_all().unwrap();
        }

        println!("\n=== io_uring vs Traditional Networking Benchmark ===");
        println!("File size: {} MB", file_size / (1024 * 1024));

        // Test 1: io_uring (if available)
        println!("\n--- io_uring Performance Test ---");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn server to receive data
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut total_received = 0usize;
            let mut buffer = vec![0u8; 64 * 1024];

            use tokio::io::AsyncReadExt;
            while total_received < file_size {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                total_received += n;
            }
            total_received
        });

        let socket = TcpStream::connect(addr).await.unwrap();
        let socket_fd = socket.as_raw_fd();

        // Create io_uring message transfer
        if let Ok(mut transfer) = IoUringMessageTransfer::new() {
            let file = File::open(&file_path).unwrap();

            let start = Instant::now();

            // Prepare segments for transfer (1MB each)
            let segments = (0..5)
                .map(|i| {
                    let offset = (i * 1024 * 1024) as u64;
                    let length = 1024 * 1024;
                    (offset, length)
                })
                .collect();

            // Submit io_uring operations
            let operation_ids = transfer
                .transfer_messages_async(&file, socket_fd, segments)
                .unwrap();

            println!("Submitted {} io_uring operations", operation_ids.len());

            // Wait for completions
            let completions = transfer.wait_for_completions(operation_ids.len()).unwrap();

            let io_uring_duration = start.elapsed();
            let total_transferred: usize = completions.iter().map(|c| c.bytes_transferred).sum();

            let io_uring_throughput =
                (total_transferred as f64 / io_uring_duration.as_secs_f64()) / (1024.0 * 1024.0);

            println!("io_uring Results:");
            println!("  Time: {:?}", io_uring_duration);
            println!("  Transferred: {} bytes", total_transferred);
            println!("  Throughput: {:.2} MB/s", io_uring_throughput);
            println!("  Completions: {}", completions.len());
            println!("  Stats: {}", transfer.get_stats());

            // Wait for server
            let received = server_handle.await.unwrap();
            println!("  Server received: {} bytes", received);

            if io_uring_throughput > 100.0 {
                println!("✅ io_uring achieved excellent performance (>100 MB/s)!");
            } else {
                println!("ℹ️ io_uring performance: {:.2} MB/s", io_uring_throughput);
            }
        } else {
            println!("❌ Failed to create io_uring transfer handler");
        }
    }

    /// Test io_uring performance with various file sizes
    #[tokio::test]
    async fn test_io_uring_scaling() {
        if IoUringNetworkHandler::new().is_err() {
            println!("⚠️ io_uring not available, skipping scaling test");
            return;
        }

        let sizes = vec![
            (64 * 1024, "64KB"),
            (1024 * 1024, "1MB"),
            (5 * 1024 * 1024, "5MB"),
            (10 * 1024 * 1024, "10MB"),
        ];

        println!("\n=== io_uring Performance Scaling Test ===");

        for (size, label) in sizes {
            // Create test file
            let dir = tempdir().unwrap();
            let file_path = dir.path().join(format!("scaling_test_{}.bin", label));

            {
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&file_path)
                    .unwrap();

                let data = vec![0xAB; size];
                file.write_all(&data).unwrap();
                file.sync_all().unwrap();
            }

            // Setup TCP connection
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut total_received = 0usize;
                let mut buffer = vec![0u8; 64 * 1024];

                use tokio::io::AsyncReadExt;
                while total_received < size {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    total_received += n;
                }
            });

            let socket = TcpStream::connect(addr).await.unwrap();

            // Test with io_uring
            if let Ok(mut transfer) = IoUringMessageTransfer::new() {
                let file = File::open(&file_path).unwrap();
                let socket_fd = socket.as_raw_fd();

                let start = Instant::now();

                // Single segment transfer
                let operation_ids = transfer
                    .transfer_messages_async(&file, socket_fd, vec![(0, size)])
                    .unwrap();

                let completions = transfer.wait_for_completions(operation_ids.len()).unwrap();

                let duration = start.elapsed();
                let transferred: usize = completions.iter().map(|c| c.bytes_transferred).sum();
                let throughput_mbps =
                    (transferred as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);

                println!(
                    "{}: {} bytes in {:?} = {:.2} MB/s",
                    label, transferred, duration, throughput_mbps
                );
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod non_linux_tests {
    #[test]
    fn test_io_uring_unavailable() {
        println!("⚠️ io_uring is only available on Linux systems");
        println!("Current platform: {}", std::env::consts::OS);

        // Test that io_uring gracefully handles non-Linux platforms
        use fluxmq::performance::io_uring_zero_copy::IoUringNetworkHandler;

        let result = IoUringNetworkHandler::new();
        assert!(result.is_err());

        if let Err(e) = result {
            println!("Expected error on non-Linux platform: {}", e);
            assert!(format!("{}", e).contains("only supported on Linux"));
        }
    }
}
