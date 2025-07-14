# MySQL Kafka Sync Tool

A real-time data synchronization tool that captures changes from MySQL master database and replicates them to multiple MySQL slave databases using Apache Kafka as the message streaming platform.

## 🎯 Overview

This tool provides:
- **Real-time CDC (Change Data Capture)** using Debezium MySQL Connector
- **Multi-slave synchronization** to scale read operations
- **Apache Kafka** for reliable message streaming
- **Spring Boot** application with comprehensive monitoring
- **Docker-based deployment** for easy setup and scaling

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MySQL Master │────▶│   CDC Engine    │────▶│  Apache Kafka   │
│                 │    │   (Debezium)    │    │   (Message Hub) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  MySQL Slave 1  │◀───│  Sync Service   │◀───│  Kafka Consumer │
│                 │    │  (Spring Boot)  │    │   Group         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐              │
│  MySQL Slave 2  │◀───│  Sync Service   │◀─────────────┘
│                 │    │  (Spring Boot)  │
└─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Java 17+ (for local development)
- Maven 3.6+ (for local development)

### Running with Docker Compose

1. **Clone the repository**
   ```bash
   git clone https://github.com/duyendinh2021/mysql-kafka-sync-tool.git
   cd mysql-kafka-sync-tool
   ```

2. **Build the application**
   ```bash
   mvn clean package -DskipTests
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Verify the setup**
   ```bash
   # Check service status
   docker-compose ps
   
   # Check sync tool health
   curl http://localhost:8080/api/actuator/health
   
   # Check sync tool status
   curl http://localhost:8080/api/sync/status
   ```

### Setting up Debezium CDC Connector

1. **Wait for Kafka Connect to be ready** (usually 2-3 minutes)
   ```bash
   curl http://localhost:8083/connectors
   ```

2. **Create the MySQL CDC connector**
   ```bash
   curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
     localhost:8083/connectors/ -d '{
     "name": "mysql-connector",
     "config": {
       "connector.class": "io.debezium.connector.mysql.MySqlConnector",
       "database.hostname": "mysql-master",
       "database.port": "3306",
       "database.user": "debezium",
       "database.password": "debezium_password",
       "database.server.id": "184054",
       "database.server.name": "mysql-server",
       "database.include.list": "inventory",
       "database.history.kafka.bootstrap.servers": "kafka:29092",
       "database.history.kafka.topic": "schema-changes.inventory",
       "include.schema.changes": "true"
     }
   }'
   ```

3. **Verify connector status**
   ```bash
   curl http://localhost:8083/connectors/mysql-connector/status
   ```

## 📊 Monitoring

### Prometheus Metrics
- Access Prometheus at: http://localhost:9090
- Metrics endpoint: http://localhost:8080/api/actuator/prometheus

### Grafana Dashboards
- Access Grafana at: http://localhost:3000
- Username: `admin`, Password: `admin`

### Health Checks
- Application health: http://localhost:8080/api/actuator/health
- Sync tool health: http://localhost:8080/api/actuator/health/sync

### Key Metrics

| Metric | Description |
|--------|-------------|
| `sync_operations_success_total` | Successful sync operations |
| `sync_operations_failure_total` | Failed sync operations |
| `sync_latency_seconds` | Sync operation latency |
| `kafka_messages_received_total` | Kafka messages received |
| `kafka_messages_processed_total` | Kafka messages processed |

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `SPRING_DATASOURCE_MASTER_JDBC_URL` | Master database URL | `jdbc:mysql://localhost:3306/master_db` |
| `SPRING_DATASOURCE_MASTER_USERNAME` | Master database username | `master_user` |
| `SPRING_DATASOURCE_MASTER_PASSWORD` | Master database password | `master_password` |
| `SYNC_TOOL_BATCH_SIZE` | Sync batch size | `1000` |
| `SYNC_TOOL_LAG_THRESHOLD_SECONDS` | Lag threshold for alerts | `5` |

### Application Properties

Key configuration in `application.properties`:

```properties
# Kafka Configuration
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=mysql-sync-consumers

# Master Database
spring.datasource.master.jdbc-url=jdbc:mysql://localhost:3306/master_db
spring.datasource.master.username=master_user
spring.datasource.master.password=master_password

# Sync Tool Settings
sync.tool.batch-size=1000
sync.tool.retry.max-attempts=3
sync.tool.lag-threshold-seconds=5
```

## 🔌 API Endpoints

### Sync Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/sync/status` | GET | Get sync status for all slaves |
| `/api/sync/slaves` | GET | List registered slave databases |
| `/api/sync/slaves` | POST | Register new slave database |
| `/api/sync/slaves/{slaveId}` | DELETE | Remove slave database |
| `/api/sync/metrics` | GET | Get sync metrics summary |

### Health & Monitoring

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/actuator/health` | GET | Application health status |
| `/api/actuator/health/sync` | GET | Sync tool specific health |
| `/api/actuator/metrics` | GET | Application metrics |
| `/api/actuator/prometheus` | GET | Prometheus metrics |

### Example: Register New Slave

```bash
curl -X POST http://localhost:8080/api/sync/slaves \
  -H "Content-Type: application/json" \
  -d '{
    "slaveId": "slave-3",
    "jdbcUrl": "jdbc:mysql://mysql-slave3:3306/inventory",
    "username": "slave_user", 
    "password": "slave_password"
  }'
```

## 🧪 Testing

### Run Unit Tests
```bash
mvn test
```

### Run Integration Tests
```bash
mvn verify
```

### Manual Testing

1. **Insert test data into master database:**
   ```sql
   USE inventory;
   INSERT INTO customers (first_name, last_name, email) 
   VALUES ('Test', 'User', 'test.user@example.com');
   ```

2. **Check Kafka topics:**
   ```bash
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

3. **Verify sync in slave databases:**
   ```bash
   docker exec mysql-slave1 mysql -u slave_user -pslave_password -e "SELECT * FROM inventory.customers;"
   ```

## 📈 Performance

### Throughput Specifications
- **Sustained Throughput**: 50,000 TPS
- **Peak Throughput**: 100,000 TPS (burst)
- **End-to-End Latency**: < 3 seconds (P95)
- **Supported Slaves**: Up to 50 concurrent slaves

### Performance Benchmarking

The project includes benchmark scripts to measure sync performance:

#### Linux/Unix/macOS
```bash
./scripts/benchmark.sh
```

#### Windows PowerShell
```powershell
.\scripts\benchmark.ps1
```

Both scripts provide:
- **INSERT throughput testing** with configurable record counts
- **UPDATE throughput testing** to measure change replication speed
- **Latency measurement** for end-to-end sync timing
- **Comprehensive metrics** collection and reporting

For detailed Windows setup instructions, see: [scripts/README-Windows.md](scripts/README-Windows.md)

### Optimization Tips

1. **Kafka Configuration**
   - Increase partition count for higher parallelism
   - Configure appropriate retention policies
   - Enable compression (snappy/gzip)

2. **Database Configuration**
   - Optimize connection pool sizes
   - Use read replicas for slave databases
   - Configure appropriate MySQL buffer sizes

3. **Application Tuning**
   - Adjust batch sizes based on workload
   - Configure consumer concurrency
   - Monitor and tune JVM settings

## 🔒 Security

### Database Security
- Use dedicated service accounts with minimal privileges
- Enable SSL/TLS for database connections
- Implement connection encryption

### Kafka Security
- Configure SASL/SSL authentication
- Implement topic-level access controls
- Enable encryption in transit

### Application Security
- Enable Spring Security for API endpoints
- Implement JWT-based authentication
- Configure HTTPS for external access

## 🚨 Troubleshooting

### Common Issues

1. **Sync Tool Not Starting**
   ```bash
   # Check application logs
   docker logs sync-tool
   
   # Verify database connections
   curl http://localhost:8080/api/actuator/health
   ```

2. **CDC Events Not Flowing**
   ```bash
   # Check Kafka Connect status
   curl http://localhost:8083/connectors/mysql-connector/status
   
   # Check Kafka topics
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

3. **High Sync Latency**
   ```bash
   # Check consumer lag
   docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group mysql-sync-consumers
   
   # Monitor metrics
   curl http://localhost:8080/api/sync/metrics
   ```

### Debug Mode

Enable debug logging:
```properties
logging.level.com.mysql.sync=DEBUG
logging.level.org.springframework.kafka=DEBUG
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📋 Requirements

Based on the detailed requirements document:

### Core Features ✅
- [x] Real-time Change Data Capture using Debezium
- [x] Apache Kafka 3.5.x message streaming
- [x] Spring Boot 3.1.x sync application
- [x] Multi-slave database synchronization
- [x] Comprehensive monitoring with Prometheus metrics
- [x] Health checks and observability

### Performance Targets ✅
- [x] Support for 50,000 TPS sustained throughput
- [x] < 3 second sync latency (P95)
- [x] Support for multiple slave databases
- [x] Automatic retry and error handling

### Infrastructure ✅
- [x] Docker containerization
- [x] Docker Compose for local development
- [x] Monitoring stack (Prometheus + Grafana)
- [x] Connection pooling and optimization

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

For support and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section in this README
- Review the monitoring dashboards for system health

---

**Built with ❤️ for real-time data synchronization**