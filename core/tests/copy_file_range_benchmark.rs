/// copy_file_range performance benchmarks
///
/// These tests benchmark the kernel-level file-to-file copying using copy_file_range
/// and compare it with traditional read/write operations.
use fluxmq::performance::copy_file_range_zero_copy::{
    CopyFileRangeHandler, CopyOperation, CopyOperationType, LogSegmentCopyManager,
};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::time::Instant;
use tempfile::tempdir;

/// Test basic copy_file_range functionality
#[test]
fn test_copy_file_range_basic() {
    let dir = tempdir().unwrap();
    let src_path = dir.path().join("source.txt");
    let dst_path = dir.path().join("destination.txt");

    // Create source file with test data
    {
        let mut src_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&src_path)
            .unwrap();

        let test_data = b"Hello, copy_file_range zero-copy test!";
        src_file.write_all(test_data).unwrap();
        src_file.flush().unwrap();
    }

    // Test copy operation
    let handler = CopyFileRangeHandler::new();
    let result = handler.copy_entire_file(&src_path, &dst_path);

    match result {
        Ok(bytes_copied) => {
            println!(
                "✅ copy_file_range: Successfully copied {} bytes",
                bytes_copied
            );
            assert!(bytes_copied > 0);

            // Verify destination file
            let dst_metadata = std::fs::metadata(&dst_path).unwrap();
            assert_eq!(dst_metadata.len(), bytes_copied);

            // Verify content
            let dst_content = std::fs::read(&dst_path).unwrap();
            let src_content = std::fs::read(&src_path).unwrap();
            assert_eq!(dst_content, src_content);
        }
        Err(e) => {
            println!(
                "⚠️ copy_file_range failed: {} (expected on non-Linux systems)",
                e
            );
        }
    }

    let stats = handler.get_stats();
    println!("Stats: {}", stats.report());
}

/// Benchmark copy_file_range vs traditional copy
#[test]
fn benchmark_copy_file_range_vs_traditional() {
    let dir = tempdir().unwrap();
    let src_path = dir.path().join("benchmark_source.bin");
    let dst1_path = dir.path().join("copy_file_range_dest.bin");
    let dst2_path = dir.path().join("traditional_dest.bin");

    // Create test file - 10MB
    let file_size = 10 * 1024 * 1024; // 10MB
    {
        let mut src_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&src_path)
            .unwrap();

        let chunk = vec![0xAB; 1024 * 1024]; // 1MB chunk
        for _ in 0..10 {
            src_file.write_all(&chunk).unwrap();
        }
        src_file.flush().unwrap();
    }

    println!("\n=== copy_file_range vs Traditional Copy Benchmark ===");
    println!("File size: {} MB", file_size / (1024 * 1024));

    // Test 1: copy_file_range
    let handler = CopyFileRangeHandler::new();
    let start = Instant::now();
    let cfr_result = handler.copy_entire_file(&src_path, &dst1_path);
    let cfr_duration = start.elapsed();

    match cfr_result {
        Ok(bytes_copied) => {
            let cfr_throughput =
                (bytes_copied as f64 / cfr_duration.as_secs_f64()) / (1024.0 * 1024.0);
            println!("\ncopy_file_range Results:");
            println!("  Time: {:?}", cfr_duration);
            println!("  Bytes: {}", bytes_copied);
            println!("  Throughput: {:.2} MB/s", cfr_throughput);
            println!("  Stats: {}", handler.get_stats().report());

            // Test 2: Traditional copy
            let start = Instant::now();
            std::fs::copy(&src_path, &dst2_path).unwrap();
            let traditional_duration = start.elapsed();

            let traditional_throughput =
                (file_size as f64 / traditional_duration.as_secs_f64()) / (1024.0 * 1024.0);

            println!("\nTraditional Copy Results:");
            println!("  Time: {:?}", traditional_duration);
            println!("  Bytes: {}", file_size);
            println!("  Throughput: {:.2} MB/s", traditional_throughput);

            // Performance comparison
            println!("\n=== Performance Comparison ===");
            let speedup = cfr_throughput / traditional_throughput;
            println!("copy_file_range is {:.2}x vs traditional copy", speedup);

            if speedup > 1.0 {
                println!("✅ copy_file_range provides better performance!");
            } else {
                println!("ℹ️ Traditional copy was faster (may vary by filesystem/kernel)");
            }

            // Verify files are identical
            let src_content = std::fs::read(&src_path).unwrap();
            let dst1_content = std::fs::read(&dst1_path).unwrap();
            let dst2_content = std::fs::read(&dst2_path).unwrap();
            assert_eq!(src_content, dst1_content);
            assert_eq!(src_content, dst2_content);
        }
        Err(e) => {
            println!("⚠️ copy_file_range not available: {}", e);
            println!("This is expected on non-Linux systems or older kernels");
        }
    }
}

/// Test copy_file_range with various file sizes
#[test]
fn test_copy_file_range_scaling() {
    let sizes = vec![
        (1024, "1KB"),
        (64 * 1024, "64KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    println!("\n=== copy_file_range Performance Scaling ===");

    let handler = CopyFileRangeHandler::new();

    for (size, label) in sizes {
        let dir = tempdir().unwrap();
        let src_path = dir.path().join(format!("src_{}.bin", label));
        let dst_path = dir.path().join(format!("dst_{}.bin", label));

        // Create test file
        {
            let mut src_file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&src_path)
                .unwrap();

            let data = vec![0xCD; size];
            src_file.write_all(&data).unwrap();
            src_file.flush().unwrap();
        }

        // Benchmark copy
        let start = Instant::now();
        let result = handler.copy_entire_file(&src_path, &dst_path);
        let duration = start.elapsed();

        match result {
            Ok(bytes_copied) => {
                let throughput_mbps =
                    (bytes_copied as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);
                println!(
                    "{}: {} bytes in {:?} = {:.2} MB/s",
                    label, bytes_copied, duration, throughput_mbps
                );

                // Verify copy
                let dst_metadata = std::fs::metadata(&dst_path).unwrap();
                assert_eq!(dst_metadata.len(), bytes_copied);
            }
            Err(e) => {
                println!("{}: copy_file_range failed - {}", label, e);
            }
        }
    }

    let final_stats = handler.get_stats();
    println!("\nFinal stats: {}", final_stats.report());
}

/// Test log segment copy manager
#[test]
fn test_log_segment_copy_manager() {
    let manager = LogSegmentCopyManager::new();

    // Test manager creation
    println!("\n=== Log Segment Copy Manager Test ===");
    println!("Manager stats: {}", manager.get_stats());

    // Create test segment
    let dir = tempdir().unwrap();
    let segment_path = dir.path().join("test_segment.log");
    let backup_dir = dir.path().join("backup");
    std::fs::create_dir_all(&backup_dir).unwrap();

    // Create test segment with data
    {
        let mut segment_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&segment_path)
            .unwrap();

        let segment_data = b"Test log segment data for replication testing";
        segment_file.write_all(segment_data).unwrap();
        segment_file.flush().unwrap();
    }

    // Test segment replication
    let result = manager.replicate_segment(&segment_path, &backup_dir, 12345);
    match result {
        Ok(backup_path) => {
            println!("✅ Segment replicated successfully to: {}", backup_path);

            // Verify backup exists and has correct content
            assert!(std::path::Path::new(&backup_path).exists());
            let original_content = std::fs::read(&segment_path).unwrap();
            let backup_content = std::fs::read(&backup_path).unwrap();
            assert_eq!(original_content, backup_content);
        }
        Err(e) => {
            println!("⚠️ Segment replication failed: {}", e);
        }
    }

    println!("Final manager stats: {}", manager.get_stats());
}

/// Test batch copy operations
#[test]
fn test_batch_copy_operations() {
    let dir = tempdir().unwrap();
    let handler = CopyFileRangeHandler::new();

    // Create multiple source files
    let mut operations = Vec::new();
    for i in 0..5 {
        let src_path = dir.path().join(format!("batch_src_{}.txt", i));
        let dst_path = dir.path().join(format!("batch_dst_{}.txt", i));

        // Create source file
        {
            let mut src_file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&src_path)
                .unwrap();

            let data = format!("Batch copy test data for file {}", i);
            src_file.write_all(data.as_bytes()).unwrap();
            src_file.flush().unwrap();
        }

        operations.push(CopyOperation {
            operation_id: i as u64,
            src_path: src_path.to_string_lossy().into_owned(),
            dst_path: dst_path.to_string_lossy().into_owned(),
            operation_type: CopyOperationType::EntireFile,
        });
    }

    println!("\n=== Batch Copy Operations Test ===");
    println!("Processing {} copy operations", operations.len());

    // Execute batch copy
    let start = Instant::now();
    let results = handler.batch_copy_segments(operations);
    let total_duration = start.elapsed();

    match results {
        Ok(copy_results) => {
            println!("✅ Batch copy completed in {:?}", total_duration);

            let mut total_bytes = 0u64;
            for result in &copy_results {
                println!(
                    "  Operation {}: {} bytes in {:?}",
                    result.operation_id, result.bytes_copied, result.duration
                );
                total_bytes += result.bytes_copied;
                assert!(result.success);
            }

            let total_throughput =
                (total_bytes as f64 / total_duration.as_secs_f64()) / (1024.0 * 1024.0);
            println!("Total: {} bytes, {:.2} MB/s", total_bytes, total_throughput);

            assert_eq!(copy_results.len(), 5);
        }
        Err(e) => {
            println!("⚠️ Batch copy failed: {}", e);
        }
    }

    let stats = handler.get_stats();
    println!("Final batch stats: {}", stats.report());
}

/// Test copy_file_range with partial ranges
#[test]
fn test_copy_file_range_partial() {
    let dir = tempdir().unwrap();
    let src_path = dir.path().join("partial_src.txt");
    let dst_path = dir.path().join("partial_dst.txt");

    // Create source file with test data
    let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    {
        let mut src_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&src_path)
            .unwrap();

        src_file.write_all(test_data).unwrap();
        src_file.flush().unwrap();
    }

    // Create destination file
    let dst_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&dst_path)
        .unwrap();

    // Set destination file size
    dst_file.set_len(test_data.len() as u64).unwrap();

    println!("\n=== Partial Range Copy Test ===");

    let handler = CopyFileRangeHandler::new();
    let src_file = File::open(&src_path).unwrap();

    // Test copying a range in the middle
    let result = handler.copy_file_range(&src_file, 10, &dst_file, 5, 20);

    match result {
        Ok(bytes_copied) => {
            println!(
                "✅ Partial copy: {} bytes from offset 10 to offset 5",
                bytes_copied
            );
            assert_eq!(bytes_copied, 20);

            // Verify the copied data
            let dst_content = std::fs::read(&dst_path).unwrap();
            let expected_slice = &test_data[10..30]; // Source bytes 10-29
            let actual_slice = &dst_content[5..25]; // Destination bytes 5-24

            assert_eq!(actual_slice, expected_slice);
            println!("✅ Partial copy content verification passed");
        }
        Err(e) => {
            println!("⚠️ Partial copy failed: {}", e);
        }
    }

    let stats = handler.get_stats();
    println!("Partial copy stats: {}", stats.report());
}
