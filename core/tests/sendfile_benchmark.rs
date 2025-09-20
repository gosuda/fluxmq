use fluxmq::performance::sendfile_zero_copy::SendfileTransfer;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::time::Instant;
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

/// Benchmark comparing traditional copy vs sendfile performance
#[tokio::test]
async fn benchmark_sendfile_vs_traditional() {
    // Create test data - 10MB file
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_data.bin");
    let file_size = 10 * 1024 * 1024; // 10MB

    // Generate test data
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let chunk = vec![0xAB; 1024 * 1024]; // 1MB chunk
        for _ in 0..10 {
            file.write_all(&chunk).unwrap();
        }
        file.sync_all().unwrap();
    }

    // Start TCP server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server task
    let server_handle = tokio::spawn(async move {
        let mut total_received = 0usize;
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
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

    // Connect to server
    let socket = TcpStream::connect(addr).await.unwrap();

    // Test 1: Sendfile (zero-copy)
    println!("\n=== Sendfile Benchmark (Zero-Copy) ===");
    let file = File::open(&file_path).unwrap();
    let mut transfer = SendfileTransfer::new();

    let start = Instant::now();
    let mut offset = 0u64;
    let mut total_sent = 0usize;

    while total_sent < file_size {
        let chunk_size = (file_size - total_sent).min(1024 * 1024); // 1MB chunks
        let bytes_sent = transfer
            .sendfile_to_socket(&file, &socket, offset, chunk_size)
            .await
            .unwrap();

        offset += bytes_sent as u64;
        total_sent += bytes_sent;

        if bytes_sent == 0 {
            break;
        }
    }

    let sendfile_duration = start.elapsed();
    let sendfile_throughput =
        (file_size as f64 / sendfile_duration.as_secs_f64()) / (1024.0 * 1024.0);

    println!("Sendfile Results:");
    println!("  Time: {:?}", sendfile_duration);
    println!("  Throughput: {:.2} MB/s", sendfile_throughput);
    println!("  Stats: {}", transfer.get_stats().report());

    // Wait for server to finish
    let received = server_handle.await.unwrap();
    println!("  Server received: {} bytes", received);

    // Test 2: Traditional copy (for comparison)
    println!("\n=== Traditional Copy Benchmark ===");

    // Start new server for traditional test
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let mut total_received = 0usize;
        let (mut socket, _) = listener.accept().await.unwrap();

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

    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Traditional read + write
    use std::io::{Read, Seek, SeekFrom};
    use tokio::io::AsyncWriteExt;

    let start = Instant::now();
    let mut file = File::open(&file_path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
    let mut total_sent = 0usize;

    while total_sent < file_size {
        let bytes_read = file.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            break;
        }

        socket.write_all(&buffer[..bytes_read]).await.unwrap();
        total_sent += bytes_read;
    }

    let traditional_duration = start.elapsed();
    let traditional_throughput =
        (file_size as f64 / traditional_duration.as_secs_f64()) / (1024.0 * 1024.0);

    println!("Traditional Copy Results:");
    println!("  Time: {:?}", traditional_duration);
    println!("  Throughput: {:.2} MB/s", traditional_throughput);

    let received = server_handle.await.unwrap();
    println!("  Server received: {} bytes", received);

    // Performance comparison
    println!("\n=== Performance Comparison ===");
    let speedup = sendfile_throughput / traditional_throughput;
    println!("Sendfile is {:.2}x faster than traditional copy", speedup);

    if speedup > 1.0 {
        println!("✅ Sendfile provides better performance!");
    } else {
        println!("⚠️ Traditional copy was faster (might be due to test conditions)");
    }
}

/// Test sendfile with various file sizes
#[tokio::test]
async fn test_sendfile_various_sizes() {
    let sizes = vec![
        (1024, "1KB"),
        (64 * 1024, "64KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    println!("\n=== Sendfile Performance Across File Sizes ===");

    for (size, label) in sizes {
        // Create test file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join(format!("test_{}.bin", label));

        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&file_path)
                .unwrap();

            let data = vec![0xCD; size];
            file.write_all(&data).unwrap();
            file.sync_all().unwrap();
        }

        // Setup TCP connection
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0u8; size];
            use tokio::io::AsyncReadExt;
            socket.read_exact(&mut buffer).await.unwrap();
        });

        let socket = TcpStream::connect(addr).await.unwrap();

        // Benchmark sendfile
        let file = File::open(&file_path).unwrap();
        let mut transfer = SendfileTransfer::new();

        let start = Instant::now();
        let bytes_sent = transfer
            .sendfile_to_socket(&file, &socket, 0, size)
            .await
            .unwrap();

        let duration = start.elapsed();
        let throughput_mbps = (size as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);

        println!(
            "{}: {} bytes in {:?} = {:.2} MB/s",
            label, bytes_sent, duration, throughput_mbps
        );
    }
}
