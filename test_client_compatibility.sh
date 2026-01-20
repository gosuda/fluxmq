#!/bin/bash

# FluxMQ Client Compatibility Test Script
# Tests various Kafka client versions against FluxMQ

set -e

FLUXMQ_PORT=9092
TEST_TOPIC="compatibility-test"
RESULTS_DIR="./compatibility_test_results"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================"
echo "FluxMQ Client Compatibility Test Suite"
echo "================================================"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Function to check if FluxMQ is running
check_fluxmq() {
    if lsof -Pi :$FLUXMQ_PORT -sTCP:LISTEN -t >/dev/null ; then
        echo -e "${GREEN}✓${NC} FluxMQ is running on port $FLUXMQ_PORT"
        return 0
    else
        echo -e "${RED}✗${NC} FluxMQ is not running on port $FLUXMQ_PORT"
        echo "Please start FluxMQ first:"
        echo "  env RUSTFLAGS=\"-C target-cpu=native\" ./target/release/fluxmq --port $FLUXMQ_PORT"
        return 1
    fi
}

# Function to test rdkafka
test_rdkafka() {
    echo ""
    echo "================================================"
    echo "Testing rdkafka (librdkafka)"
    echo "================================================"

    # Check rdkafka version
    RDKAFKA_VERSION=$(./target/release/simple_producer --version 2>&1 || echo "unknown")
    echo "rdkafka version: $RDKAFKA_VERSION"

    # Run test
    echo "Running producer test with 50 messages..."
    if ./cdc-test/target/release/simple_producer 50 > "$RESULTS_DIR/rdkafka_test.log" 2>&1; then
        SUCCESS=$(grep "Success:" "$RESULTS_DIR/rdkafka_test.log" | awk '{print $2}')
        ERRORS=$(grep "Errors:" "$RESULTS_DIR/rdkafka_test.log" | awk '{print $2}')
        THROUGHPUT=$(grep "Throughput:" "$RESULTS_DIR/rdkafka_test.log" | awk '{print $2" "$3}')

        if [ "$ERRORS" = "0" ]; then
            echo -e "${GREEN}✓${NC} rdkafka test PASSED"
            echo "  - Success: $SUCCESS messages"
            echo "  - Throughput: $THROUGHPUT"
        else
            echo -e "${RED}✗${NC} rdkafka test FAILED"
            echo "  - Errors: $ERRORS"
        fi
    else
        echo -e "${RED}✗${NC} rdkafka test FAILED (execution error)"
    fi
}

# Function to test Java client
test_java_client() {
    local VERSION=$1

    echo ""
    echo "================================================"
    echo "Testing Java Kafka Client $VERSION"
    echo "================================================"

    # Note: This requires Maven project with specified Kafka version
    echo "To test Java client version $VERSION:"
    echo "1. Update pom.xml kafka.version to $VERSION"
    echo "2. Run: mvn clean install"
    echo "3. Run: mvn exec:java -Dexec.mainClass=\"com.fluxmq.tests.MinimalProducerTest\""
    echo ""
    echo -e "${YELLOW}⚠${NC} Manual test required - automated testing not yet implemented"
}

# Function to check API versions response
check_api_versions() {
    echo ""
    echo "================================================"
    echo "Checking ApiVersions Response"
    echo "================================================"

    # Start FluxMQ if not running
    if ! check_fluxmq; then
        echo "Starting FluxMQ temporarily..."
        env RUSTFLAGS="-C target-cpu=native" ./target/release/fluxmq \
            --port $FLUXMQ_PORT --log-level debug \
            > "$RESULTS_DIR/fluxmq_apiversion_check.log" 2>&1 &
        FLUXMQ_PID=$!
        sleep 3
    fi

    # Send simple ApiVersions request using rdkafka
    echo "Sending ApiVersions request..."
    ./cdc-test/target/release/simple_producer 1 > "$RESULTS_DIR/apiversion_test.log" 2>&1 || true

    # Extract ApiVersions info from FluxMQ logs
    if [ -f "$RESULTS_DIR/fluxmq_apiversion_check.log" ]; then
        echo ""
        echo "ApiVersions responses sent:"
        grep "ApiVersions fast encoding" "$RESULTS_DIR/fluxmq_apiversion_check.log" | head -5
        echo ""
        echo "Template sizes:"
        grep "template_size=" "$RESULTS_DIR/fluxmq_apiversion_check.log" | \
            awk -F'template_size=' '{print $2}' | \
            awk -F',' '{print "  - "$1" bytes"}' | \
            sort -u
    fi

    # Kill temporary FluxMQ if we started it
    if [ ! -z "$FLUXMQ_PID" ]; then
        kill $FLUXMQ_PID 2>/dev/null || true
    fi
}

# Main test execution
main() {
    echo "Starting compatibility tests..."
    echo ""

    # Check prerequisites
    if ! check_fluxmq; then
        exit 1
    fi

    # Test rdkafka
    test_rdkafka

    # Check API versions
    check_api_versions

    # Java client versions to test (informational)
    echo ""
    echo "================================================"
    echo "Recommended Java Client Versions to Test"
    echo "================================================"
    echo ""
    echo "Latest:"
    echo "  - Kafka 4.1.0 (tested ✓)"
    echo "  - Kafka 4.0.0"
    echo ""
    echo "Kafka 3.x:"
    echo "  - Kafka 3.8.1 (latest 3.x)"
    echo "  - Kafka 3.6.0"
    echo "  - Kafka 3.0.0"
    echo ""
    echo "Kafka 2.x:"
    echo "  - Kafka 2.8.0 (last 2.x)"
    echo "  - Kafka 2.4.0 (flexible versions introduced)"
    echo "  - Kafka 2.1.0 (minimum for 4.x compat)"
    echo ""

    # Summary
    echo ""
    echo "================================================"
    echo "Test Summary"
    echo "================================================"
    echo ""
    echo "Results saved to: $RESULTS_DIR/"
    echo ""
    echo "For detailed analysis:"
    echo "  - rdkafka results: cat $RESULTS_DIR/rdkafka_test.log"
    echo "  - FluxMQ logs: cat $RESULTS_DIR/*.log"
    echo ""
}

# Run main function
main "$@"
