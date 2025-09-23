#!/usr/bin/env python3
"""
Test Metadata v9 flexible version behavior with current FluxMQ setup
"""
import socket
import struct
import time

def test_metadata_v9():
    print("🔧 Testing Metadata v9 flexible version behavior...")

    try:
        # Connect to FluxMQ
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)  # Set timeout
        sock.connect(('localhost', 9092))

        # Step 1: ApiVersions request to see what versions are advertised
        print("\n1. Sending ApiVersions request...")
        api_versions_request = bytearray()
        api_versions_request.extend(struct.pack('>I', 23))  # Length
        api_versions_request.extend(struct.pack('>H', 18))  # API Key: ApiVersions
        api_versions_request.extend(struct.pack('>H', 3))   # API Version: 3 (flexible)
        api_versions_request.extend(struct.pack('>I', 123)) # Correlation ID
        api_versions_request.extend(struct.pack('>H', 12))  # Client ID length
        api_versions_request.extend(b'test-client')         # Client ID
        api_versions_request.extend(b'\x00')                # Tagged fields (empty)

        sock.send(api_versions_request)

        # Read response
        response_length = struct.unpack('>I', sock.recv(4))[0]
        response_data = sock.recv(response_length)

        correlation_id = struct.unpack('>I', response_data[0:4])[0]
        error_code = struct.unpack('>H', response_data[4:6])[0]

        print(f"   ✅ ApiVersions response: correlation_id={correlation_id}, error_code={error_code}")

        # Parse API versions to find Metadata support
        offset = 6
        num_apis = struct.unpack('>I', response_data[offset:offset+4])[0]
        offset += 4

        metadata_max_version = None
        for i in range(num_apis):
            api_key = struct.unpack('>H', response_data[offset:offset+2])[0]
            min_version = struct.unpack('>H', response_data[offset+2:offset+4])[0]
            max_version = struct.unpack('>H', response_data[offset+4:offset+6])[0]
            offset += 6

            if api_key == 3:  # Metadata API
                metadata_max_version = max_version
                print(f"   📊 Metadata API: versions {min_version}-{max_version}")
                break

        if metadata_max_version is None:
            print("   ❌ Metadata API not found in ApiVersions response!")
            return

        # Step 2: Test Metadata request with different versions
        for version in [8, 9]:
            if version > metadata_max_version:
                print(f"\n2. Skipping Metadata v{version} (max advertised: v{metadata_max_version})")
                continue

            print(f"\n2. Testing Metadata v{version} request...")

            metadata_request = bytearray()
            if version >= 9:
                # v9+ flexible version
                print(f"   🔧 Using flexible version format for v{version}")
                metadata_request.extend(struct.pack('>I', 21))  # Length
                metadata_request.extend(struct.pack('>H', 3))   # API Key: Metadata
                metadata_request.extend(struct.pack('>H', version)) # API Version
                metadata_request.extend(struct.pack('>I', 456)) # Correlation ID
                metadata_request.extend(struct.pack('>H', 12))  # Client ID length
                metadata_request.extend(b'test-client')         # Client ID
                metadata_request.extend(b'\x01')                # Topics array (compact, empty = 1)
                metadata_request.extend(b'\x01')                # Allow auto topic creation (true)
                metadata_request.extend(b'\x00')                # Tagged fields (empty)
            else:
                # v8 non-flexible version
                print(f"   🔧 Using non-flexible version format for v{version}")
                metadata_request.extend(struct.pack('>I', 23))  # Length
                metadata_request.extend(struct.pack('>H', 3))   # API Key: Metadata
                metadata_request.extend(struct.pack('>H', version)) # API Version
                metadata_request.extend(struct.pack('>I', 456)) # Correlation ID
                metadata_request.extend(struct.pack('>H', 12))  # Client ID length
                metadata_request.extend(b'test-client')         # Client ID
                metadata_request.extend(struct.pack('>I', 0))   # Topics array (standard, empty)
                metadata_request.extend(struct.pack('?', True)) # Allow auto topic creation

            sock.send(metadata_request)

            # Read response
            try:
                response_length = struct.unpack('>I', sock.recv(4))[0]
                response_data = sock.recv(response_length)

                correlation_id = struct.unpack('>I', response_data[0:4])[0]
                print(f"   ✅ Metadata v{version} response received: correlation_id={correlation_id}, length={response_length}")

                # Check if it's flexible format by checking bytes
                if version >= 9:
                    print(f"   🔍 Checking if response uses flexible format...")
                    # Try to detect compact array encoding in response
                    offset = 4  # After correlation_id

                    # Skip throttle_time_ms if present (v3+)
                    if version >= 3:
                        throttle_time = struct.unpack('>I', response_data[offset:offset+4])[0]
                        offset += 4
                        print(f"   📊 throttle_time_ms: {throttle_time}")

                    # Check brokers array encoding
                    if offset < len(response_data):
                        first_byte = response_data[offset]
                        if first_byte < 128:  # Not a varint (standard int32)
                            print(f"   ⚠️  Response seems to use standard format (first_byte={first_byte}), not flexible!")
                        else:
                            print(f"   ✅ Response uses flexible format (varint detected: {first_byte})")

            except Exception as e:
                print(f"   ❌ Error receiving Metadata v{version} response: {e}")

        sock.close()
        print(f"\n✅ Test completed successfully!")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_metadata_v9()