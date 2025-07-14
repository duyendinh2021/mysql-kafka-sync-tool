# Windows PowerShell Benchmark Script

This directory contains both Linux/Unix (`benchmark.sh`) and Windows (`benchmark.ps1`) benchmark scripts for the MySQL Kafka Sync Tool.

## Windows Prerequisites

Before running the PowerShell benchmark script, ensure you have the following installed:

### 1. MySQL Client for Windows
Download and install MySQL client from: https://dev.mysql.com/downloads/mysql/

**OR** install MySQL Workbench which includes the command-line client: https://dev.mysql.com/downloads/workbench/

### 2. MySQL .NET Connector (Optional but recommended)
Download from: https://dev.mysql.com/downloads/connector/net/

### 3. PowerShell 5.1+ or PowerShell Core 7+
Most Windows systems include PowerShell 5.1 by default. For PowerShell Core 7+, download from: https://github.com/PowerShell/PowerShell

## Usage

### Basic Usage
```powershell
# Run with default settings
.\benchmark.ps1

# Run with custom parameters
.\benchmark.ps1 -MasterHost "192.168.1.100" -MasterPort 3306 -MasterUser "admin" -MasterPassword "password123"
```

### Available Parameters
- `MasterHost`: MySQL master database host (default: "localhost")
- `MasterPort`: MySQL master database port (default: 3306)
- `MasterUser`: MySQL username (default: "master_user")
- `MasterPassword`: MySQL password (default: "master_password")
- `Database`: Database name (default: "inventory")
- `SyncToolHost`: Sync tool API host (default: "localhost")
- `SyncToolPort`: Sync tool API port (default: 8080)

### Example with Docker Desktop
If you're running the stack with Docker Desktop on Windows:

```powershell
# Make sure Docker Desktop is running and all services are up
docker-compose up -d

# Wait for services to be ready, then run benchmark
.\benchmark.ps1
```

### Example with Custom Configuration
```powershell
.\benchmark.ps1 -MasterHost "mysql-server.local" -MasterUser "dbadmin" -MasterPassword "mypassword" -Database "production_db"
```

## What the Script Does

The PowerShell script performs the same benchmarks as the bash version:

1. **Service Readiness Check**: Verifies the sync tool API is accessible
2. **INSERT Performance Test**: Inserts 500 test records and measures throughput
3. **UPDATE Performance Test**: Updates 500 records and measures throughput  
4. **Latency Test**: Measures end-to-end sync latency for individual operations
5. **Metrics Collection**: Retrieves and displays sync performance metrics

## Expected Output

```
MySQL Kafka Sync Tool Performance Benchmark (Windows)
=======================================================

Configuration:
- Master Host: localhost:3306
- Database: inventory
- Sync Tool: localhost:8080
- User: master_user

Checking prerequisites...
Checking Sync Tool readiness... ✓

Benchmarking INSERT operations (500 records)...
Generating test data...
.....
Waiting for sync to complete...
INSERT Benchmark Results:
- Records inserted: 500
- Records synced: 500
- Duration: 12.34s
- Throughput: 40.52 TPS
- Sync efficiency: 100%

Benchmarking UPDATE operations (500 records)...
...

Testing sync latency...
Test 1: 0.245s
Test 2: 0.198s
...
Average Sync Latency: 0.223s

Final System Status:
===================
{
  "sync-success-total": 1000,
  "sync-error-total": 0,
  "avg-latency-ms": 223
}

Benchmark completed successfully!
```

## Troubleshooting

### "mysql.exe not found"
- Ensure MySQL client is installed and added to your PATH
- Or provide full path: `$env:PATH += ";C:\Program Files\MySQL\MySQL Server 8.0\bin"`

### "Access denied" errors
- Verify MySQL credentials are correct
- Ensure the user has appropriate permissions on the database

### "Cannot connect to sync tool API"
- Verify the sync tool service is running
- Check if Docker containers are up: `docker-compose ps`
- Ensure firewall isn't blocking the connection

### PowerShell execution policy errors
```powershell
# Allow script execution (run as Administrator)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Performance Targets

The script validates the system meets these performance requirements:
- **Sustained Throughput**: 50,000 TPS
- **Peak Throughput**: 100,000 TPS  
- **End-to-End Latency (P95)**: < 3 seconds
- **End-to-End Latency (P99)**: < 5 seconds

## Comparison with Linux Version

| Feature | Linux (bash) | Windows (PowerShell) |
|---------|-------------|---------------------|
| MySQL Client | mysql CLI | mysql.exe or .NET Connector |
| HTTP Requests | curl | Invoke-RestMethod |
| JSON Parsing | python3 -m json.tool | ConvertTo-Json |
| Math Calculations | bc | PowerShell math |
| Colors | ANSI escape codes | PowerShell colors |
| Error Handling | bash error handling | PowerShell try/catch |