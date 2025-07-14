#!/bin/bash

# Performance Benchmarking Script for MySQL Kafka Sync Tool
# This script generates test data and measures sync performance

set -e

# Configuration
MASTER_HOST="localhost"
MASTER_PORT="3306"
MASTER_USER="master_user"
MASTER_PASSWORD="master_password"
DATABASE="inventory"

SYNC_TOOL_HOST="localhost"
SYNC_TOOL_PORT="8080"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}MySQL Kafka Sync Tool Performance Benchmark${NC}"
echo "============================================="

# Function to check if service is ready
check_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=1

    echo -n "Checking $service_name readiness..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "http://$host:$port/api/actuator/health" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    echo -e " ${RED}✗${NC}"
    echo -e "${RED}Error: $service_name is not ready after $((max_attempts * 2)) seconds${NC}"
    exit 1
}

# Function to execute MySQL query
mysql_query() {
    local query=$1
    mysql -h "$MASTER_HOST" -P "$MASTER_PORT" -u "$MASTER_USER" -p"$MASTER_PASSWORD" "$DATABASE" -e "$query" 2>/dev/null
}

# Function to get sync metrics
get_sync_metrics() {
    curl -s "http://$SYNC_TOOL_HOST:$SYNC_TOOL_PORT/api/sync/metrics" | \
    python3 -m json.tool 2>/dev/null || echo "{}"
}

# Function to benchmark insert operations
benchmark_inserts() {
    local num_records=$1
    echo -e "\n${YELLOW}Benchmarking INSERT operations (${num_records} records)...${NC}"
    
    # Get initial metrics
    local initial_metrics=$(get_sync_metrics)
    local initial_success=$(echo "$initial_metrics" | grep -o '"sync-success-total":[0-9]*' | grep -o '[0-9]*' || echo "0")
    
    # Record start time
    local start_time=$(date +%s.%N)
    
    # Generate test data
    echo "Generating test data..."
    for ((i=1; i<=num_records; i++)); do
        mysql_query "INSERT INTO customers (first_name, last_name, email) VALUES ('User$i', 'Test$i', 'user$i@test.com');"
        
        # Show progress every 100 records
        if [ $((i % 100)) -eq 0 ]; then
            echo -n "."
        fi
    done
    echo ""
    
    # Wait for sync to complete
    echo "Waiting for sync to complete..."
    sleep 10
    
    # Calculate metrics
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local tps=$(echo "scale=2; $num_records / $duration" | bc)
    
    # Get final metrics
    local final_metrics=$(get_sync_metrics)
    local final_success=$(echo "$final_metrics" | grep -o '"sync-success-total":[0-9]*' | grep -o '[0-9]*' || echo "0")
    local synced_records=$((final_success - initial_success))
    
    echo -e "${GREEN}INSERT Benchmark Results:${NC}"
    echo "- Records inserted: $num_records"
    echo "- Records synced: $synced_records"
    echo "- Duration: ${duration}s"
    echo "- Throughput: ${tps} TPS"
    echo "- Sync efficiency: $(echo "scale=2; $synced_records * 100 / $num_records" | bc)%"
}

# Function to benchmark update operations
benchmark_updates() {
    local num_records=$1
    echo -e "\n${YELLOW}Benchmarking UPDATE operations (${num_records} records)...${NC}"
    
    # Get initial metrics
    local initial_metrics=$(get_sync_metrics)
    local initial_success=$(echo "$initial_metrics" | grep -o '"sync-success-total":[0-9]*' | grep -o '[0-9]*' || echo "0")
    
    # Record start time
    local start_time=$(date +%s.%N)
    
    # Update test data
    echo "Updating test data..."
    for ((i=1; i<=num_records; i++)); do
        mysql_query "UPDATE customers SET first_name='UpdatedUser$i' WHERE id=$i;"
        
        # Show progress every 100 records
        if [ $((i % 100)) -eq 0 ]; then
            echo -n "."
        fi
    done
    echo ""
    
    # Wait for sync to complete
    echo "Waiting for sync to complete..."
    sleep 10
    
    # Calculate metrics
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local tps=$(echo "scale=2; $num_records / $duration" | bc)
    
    # Get final metrics
    local final_metrics=$(get_sync_metrics)
    local final_success=$(echo "$final_metrics" | grep -o '"sync-success-total":[0-9]*' | grep -o '[0-9]*' || echo "0")
    local synced_records=$((final_success - initial_success))
    
    echo -e "${GREEN}UPDATE Benchmark Results:${NC}"
    echo "- Records updated: $num_records"
    echo "- Records synced: $synced_records"
    echo "- Duration: ${duration}s"
    echo "- Throughput: ${tps} TPS"
    echo "- Sync efficiency: $(echo "scale=2; $synced_records * 100 / $num_records" | bc)%"
}

# Function to test sync latency
test_sync_latency() {
    echo -e "\n${YELLOW}Testing sync latency...${NC}"
    
    local num_tests=10
    local total_latency=0
    
    for ((i=1; i<=num_tests; i++)); do
        # Insert record and measure time until sync
        local test_email="latency_test_$i@test.com"
        local start_time=$(date +%s.%N)
        
        mysql_query "INSERT INTO customers (first_name, last_name, email) VALUES ('LatencyTest$i', 'User$i', '$test_email');"
        
        # Wait for record to appear in metrics or timeout after 30 seconds
        local timeout=30
        local elapsed=0
        local found=false
        
        while [ $elapsed -lt $timeout ]; do
            sleep 0.5
            elapsed=$(echo "$elapsed + 0.5" | bc)
            
            # Check sync metrics for increases
            local current_metrics=$(get_sync_metrics)
            local current_success=$(echo "$current_metrics" | grep -o '"sync-success-total":[0-9]*' | grep -o '[0-9]*' || echo "0")
            
            if [ $current_success -gt 0 ]; then
                local end_time=$(date +%s.%N)
                local latency=$(echo "$end_time - $start_time" | bc)
                total_latency=$(echo "$total_latency + $latency" | bc)
                echo "Test $i: ${latency}s"
                found=true
                break
            fi
        done
        
        if [ "$found" = false ]; then
            echo "Test $i: TIMEOUT (>30s)"
        fi
        
        sleep 1
    done
    
    local avg_latency=$(echo "scale=3; $total_latency / $num_tests" | bc)
    echo -e "${GREEN}Average Sync Latency: ${avg_latency}s${NC}"
}

# Main execution
main() {
    echo "Starting performance benchmark..."
    
    # Check prerequisites
    if ! command -v mysql &> /dev/null; then
        echo -e "${RED}Error: mysql client is not installed${NC}"
        exit 1
    fi
    
    if ! command -v curl &> /dev/null; then
        echo -e "${RED}Error: curl is not installed${NC}"
        exit 1
    fi
    
    if ! command -v bc &> /dev/null; then
        echo -e "${RED}Error: bc calculator is not installed${NC}"
        exit 1
    fi
    
    # Check service readiness
    check_service "Sync Tool" "$SYNC_TOOL_HOST" "$SYNC_TOOL_PORT"
    
    # Clean up previous test data
    echo "Cleaning up previous test data..."
    mysql_query "DELETE FROM customers WHERE email LIKE '%@test.com';" || true
    
    # Run benchmarks
    benchmark_inserts 500
    benchmark_updates 500
    test_sync_latency
    
    # Final system status
    echo -e "\n${GREEN}Final System Status:${NC}"
    echo "==================="
    get_sync_metrics | python3 -m json.tool || echo "Unable to retrieve metrics"
    
    echo -e "\n${GREEN}Benchmark completed successfully!${NC}"
}

# Run main function
main "$@"