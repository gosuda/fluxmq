#!/usr/bin/env python3
"""
Simple FluxMQ connection test - minimal dependencies
"""
import socket
import struct
import time
import threading

def send_api_versions_request():
    """Send raw ApiVersions request and check response"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9092))

        # ApiVersions request (API key 18)
        # Format: [length][api_key][api_version][correlation_id][client_id]
        api_key = 18
        api_version = 4
        correlation_id = 123
        client_id = b"test-client"

        # Build request
        request_data = struct.pack('>hhih', api_key, api_version, correlation_id, len(client_id)) + client_id

        # Add length prefix
        message = struct.pack('>i', len(request_data)) + request_data

        print(f"Sending ApiVersions request: {len(message)} bytes")
        sock.send(message)

        # Read response
        response_len = struct.unpack('>i', sock.recv(4))[0]
        response_data = sock.recv(response_len)

        print(f"✅ Received response: {response_len} bytes")
        print(f"First 12 bytes: {response_data[:12].hex()}")

        sock.close()
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def benchmark_raw_throughput():
    """Test raw socket throughput"""
    connections = []
    results = []

    def worker(worker_id):
        try:
            count = 0
            start = time.time()

            for i in range(100):  # 100 connections per worker
                success = send_api_versions_request()
                if success:
                    count += 1
                time.sleep(0.01)  # 10ms between requests

            end = time.time()
            rate = count / (end - start) if end > start else 0
            results.append(rate)
            print(f"Worker {worker_id}: {count} requests, {rate:.1f} req/sec")

        except Exception as e:
            print(f"Worker {worker_id} failed: {e}")
            results.append(0)

    print("🚀 Starting raw socket benchmark...")
    threads = []

    start_time = time.time()

    for i in range(4):  # 4 workers
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    total_rate = sum(results)

    print(f"\n📊 Raw Socket Benchmark Results:")
    print(f"Total rate: {total_rate:.1f} req/sec")
    print(f"Average per worker: {total_rate/4:.1f} req/sec")
    print(f"Test duration: {end_time - start_time:.1f}s")

    return total_rate

if __name__ == "__main__":
    print("🔥 FluxMQ Simple Connection Test")
    print("-" * 40)

    # Test single connection
    print("1. Testing single ApiVersions request...")
    success = send_api_versions_request()

    if success:
        print("\n2. Running throughput benchmark...")
        throughput = benchmark_raw_throughput()

        if throughput > 100:
            print("✅ Connection test passed!")
        else:
            print("⚠️ Low throughput detected")
    else:
        print("❌ Basic connection test failed")