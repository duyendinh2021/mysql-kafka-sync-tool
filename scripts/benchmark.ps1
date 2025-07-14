# Performance Benchmarking Script for MySQL Kafka Sync Tool (Windows PowerShell)
# This script generates test data and measures sync performance on Windows systems

param(
    [string]$MasterHost = "localhost",
    [int]$MasterPort = 3306,
    [string]$MasterUser = "master_user", 
    [string]$MasterPassword = "master_password",
    [string]$Database = "inventory",
    [string]$SyncToolHost = "localhost",
    [int]$SyncToolPort = 8080
)

# Prerequisites check and setup
function Test-Prerequisites {
    Write-Host "Checking prerequisites..." -ForegroundColor Yellow
    
    # Check if MySQL .NET Connector is available or suggest installation
    try {
        Add-Type -Path "C:\Program Files (x86)\MySQL\Connector NET 8.0\Assemblies\v4.5.2\MySql.Data.dll" -ErrorAction SilentlyContinue
    } catch {
        Write-Warning "MySQL .NET Connector not found at default location."
        Write-Host "Please install MySQL .NET Connector from: https://dev.mysql.com/downloads/connector/net/" -ForegroundColor Yellow
        Write-Host "Or ensure mysql.exe is in your PATH" -ForegroundColor Yellow
    }
    
    # Check if mysql.exe is available as fallback
    $mysqlExe = Get-Command mysql.exe -ErrorAction SilentlyContinue
    if (-not $mysqlExe) {
        Write-Warning "mysql.exe not found in PATH. Please install MySQL client or add to PATH."
        Write-Host "Download from: https://dev.mysql.com/downloads/mysql/" -ForegroundColor Yellow
        return $false
    }
    
    return $true
}

# Function to execute MySQL query using mysql.exe
function Invoke-MySQLQuery {
    param([string]$Query)
    
    try {
        $result = & mysql.exe -h $MasterHost -P $MasterPort -u $MasterUser -p"$MasterPassword" $Database -e $Query 2>$null
        return $result
    } catch {
        Write-Error "Failed to execute MySQL query: $_"
        return $null
    }
}

# Function to check if service is ready
function Test-ServiceReadiness {
    param(
        [string]$ServiceName,
        [string]$Host,
        [int]$Port,
        [int]$MaxAttempts = 30
    )
    
    Write-Host "Checking $ServiceName readiness..." -NoNewline
    
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $response = Invoke-RestMethod -Uri "http://${Host}:${Port}/api/actuator/health" -Method Get -TimeoutSec 5
            if ($response) {
                Write-Host " ✓" -ForegroundColor Green
                return $true
            }
        } catch {
            Write-Host "." -NoNewline
            Start-Sleep -Seconds 2
        }
    }
    
    Write-Host " ✗" -ForegroundColor Red
    Write-Error "$ServiceName is not ready after $($MaxAttempts * 2) seconds"
    return $false
}

# Function to get sync metrics
function Get-SyncMetrics {
    try {
        $metrics = Invoke-RestMethod -Uri "http://${SyncToolHost}:${SyncToolPort}/api/sync/metrics" -Method Get -ErrorAction SilentlyContinue
        return $metrics
    } catch {
        return @{}
    }
}

# Function to benchmark insert operations  
function Test-InsertPerformance {
    param([int]$NumRecords)
    
    Write-Host "`nBenchmarking INSERT operations ($NumRecords records)..." -ForegroundColor Yellow
    
    # Get initial metrics
    $initialMetrics = Get-SyncMetrics
    $initialSuccess = 0
    if ($initialMetrics -and $initialMetrics.'sync-success-total') {
        $initialSuccess = $initialMetrics.'sync-success-total'
    }
    
    # Record start time
    $startTime = Get-Date
    
    # Generate test data
    Write-Host "Generating test data..."
    for ($i = 1; $i -le $NumRecords; $i++) {
        $query = "INSERT INTO customers (first_name, last_name, email) VALUES ('User$i', 'Test$i', 'user$i@test.com');"
        Invoke-MySQLQuery -Query $query
        
        # Show progress every 100 records
        if ($i % 100 -eq 0) {
            Write-Host "." -NoNewline
        }
    }
    Write-Host ""
    
    # Wait for sync to complete
    Write-Host "Waiting for sync to complete..."
    Start-Sleep -Seconds 10
    
    # Calculate metrics
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $tps = [math]::Round($NumRecords / $duration, 2)
    
    # Get final metrics
    $finalMetrics = Get-SyncMetrics
    $finalSuccess = 0
    if ($finalMetrics -and $finalMetrics.'sync-success-total') {
        $finalSuccess = $finalMetrics.'sync-success-total'
    }
    $syncedRecords = $finalSuccess - $initialSuccess
    
    Write-Host "INSERT Benchmark Results:" -ForegroundColor Green
    Write-Host "- Records inserted: $NumRecords"
    Write-Host "- Records synced: $syncedRecords"
    Write-Host "- Duration: ${duration}s"
    Write-Host "- Throughput: $tps TPS"
    
    if ($NumRecords -gt 0) {
        $efficiency = [math]::Round(($syncedRecords * 100 / $NumRecords), 2)
        Write-Host "- Sync efficiency: $efficiency%"
    }
}

# Function to benchmark update operations
function Test-UpdatePerformance {
    param([int]$NumRecords)
    
    Write-Host "`nBenchmarking UPDATE operations ($NumRecords records)..." -ForegroundColor Yellow
    
    # Get initial metrics
    $initialMetrics = Get-SyncMetrics
    $initialSuccess = 0
    if ($initialMetrics -and $initialMetrics.'sync-success-total') {
        $initialSuccess = $initialMetrics.'sync-success-total'
    }
    
    # Record start time
    $startTime = Get-Date
    
    # Update test data
    Write-Host "Updating test data..."
    for ($i = 1; $i -le $NumRecords; $i++) {
        $query = "UPDATE customers SET first_name='UpdatedUser$i' WHERE id=$i;"
        Invoke-MySQLQuery -Query $query
        
        # Show progress every 100 records
        if ($i % 100 -eq 0) {
            Write-Host "." -NoNewline
        }
    }
    Write-Host ""
    
    # Wait for sync to complete
    Write-Host "Waiting for sync to complete..."
    Start-Sleep -Seconds 10
    
    # Calculate metrics
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $tps = [math]::Round($NumRecords / $duration, 2)
    
    # Get final metrics
    $finalMetrics = Get-SyncMetrics
    $finalSuccess = 0
    if ($finalMetrics -and $finalMetrics.'sync-success-total') {
        $finalSuccess = $finalMetrics.'sync-success-total'
    }
    $syncedRecords = $finalSuccess - $initialSuccess
    
    Write-Host "UPDATE Benchmark Results:" -ForegroundColor Green
    Write-Host "- Records updated: $NumRecords"
    Write-Host "- Records synced: $syncedRecords"
    Write-Host "- Duration: ${duration}s"
    Write-Host "- Throughput: $tps TPS"
    
    if ($NumRecords -gt 0) {
        $efficiency = [math]::Round(($syncedRecords * 100 / $NumRecords), 2)
        Write-Host "- Sync efficiency: $efficiency%"
    }
}

# Function to test sync latency
function Test-SyncLatency {
    Write-Host "`nTesting sync latency..." -ForegroundColor Yellow
    
    $numTests = 10
    $totalLatency = 0
    $successfulTests = 0
    
    for ($i = 1; $i -le $numTests; $i++) {
        # Insert record and measure time until sync
        $testEmail = "latency_test_$i@test.com"
        $startTime = Get-Date
        
        $query = "INSERT INTO customers (first_name, last_name, email) VALUES ('LatencyTest$i', 'User$i', '$testEmail');"
        Invoke-MySQLQuery -Query $query
        
        # Wait for record to sync or timeout after 30 seconds
        $timeout = 30
        $found = $false
        $initialMetrics = Get-SyncMetrics
        $initialSuccess = 0
        if ($initialMetrics -and $initialMetrics.'sync-success-total') {
            $initialSuccess = $initialMetrics.'sync-success-total'
        }
        
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            Start-Sleep -Milliseconds 500
            $elapsed += 0.5
            
            # Check sync metrics for increases
            $currentMetrics = Get-SyncMetrics
            $currentSuccess = 0
            if ($currentMetrics -and $currentMetrics.'sync-success-total') {
                $currentSuccess = $currentMetrics.'sync-success-total'
            }
            
            if ($currentSuccess -gt $initialSuccess) {
                $endTime = Get-Date
                $latency = ($endTime - $startTime).TotalSeconds
                $totalLatency += $latency
                $successfulTests++
                Write-Host "Test $i: $([math]::Round($latency, 3))s"
                $found = $true
                break
            }
        }
        
        if (-not $found) {
            Write-Host "Test $i: TIMEOUT (>30s)" -ForegroundColor Red
        }
        
        Start-Sleep -Seconds 1
    }
    
    if ($successfulTests -gt 0) {
        $avgLatency = [math]::Round($totalLatency / $successfulTests, 3)
        Write-Host "Average Sync Latency: ${avgLatency}s" -ForegroundColor Green
    } else {
        Write-Host "No successful latency tests completed" -ForegroundColor Red
    }
}

# Main execution function
function Start-PerformanceBenchmark {
    Write-Host "MySQL Kafka Sync Tool Performance Benchmark (Windows)" -ForegroundColor Green
    Write-Host "======================================================="
    
    # Check prerequisites
    if (-not (Test-Prerequisites)) {
        Write-Error "Prerequisites check failed. Please install required components."
        exit 1
    }
    
    # Check service readiness
    if (-not (Test-ServiceReadiness -ServiceName "Sync Tool" -Host $SyncToolHost -Port $SyncToolPort)) {
        Write-Error "Sync Tool service is not ready"
        exit 1
    }
    
    # Clean up previous test data
    Write-Host "Cleaning up previous test data..."
    try {
        Invoke-MySQLQuery -Query "DELETE FROM customers WHERE email LIKE '%@test.com';"
    } catch {
        Write-Warning "Could not clean up previous test data: $_"
    }
    
    # Run benchmarks
    Test-InsertPerformance -NumRecords 500
    Test-UpdatePerformance -NumRecords 500
    Test-SyncLatency
    
    # Final system status
    Write-Host "`nFinal System Status:" -ForegroundColor Green
    Write-Host "==================="
    
    try {
        $finalMetrics = Get-SyncMetrics
        if ($finalMetrics) {
            $finalMetrics | ConvertTo-Json -Depth 3 | Write-Host
        } else {
            Write-Host "Unable to retrieve metrics"
        }
    } catch {
        Write-Host "Unable to retrieve metrics: $_"
    }
    
    Write-Host "`nBenchmark completed successfully!" -ForegroundColor Green
}

# Script execution starts here
Write-Host "Starting Windows PowerShell benchmark script..." -ForegroundColor Cyan

# Display configuration
Write-Host "`nConfiguration:" -ForegroundColor Yellow
Write-Host "- Master Host: $MasterHost:$MasterPort"
Write-Host "- Database: $Database"
Write-Host "- Sync Tool: $SyncToolHost:$SyncToolPort"
Write-Host "- User: $MasterUser"

# Run the benchmark
Start-PerformanceBenchmark