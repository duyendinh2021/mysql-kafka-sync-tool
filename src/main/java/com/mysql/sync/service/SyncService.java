package com.mysql.sync.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.sync.config.DatabaseConfig;
import com.mysql.sync.model.CdcEvent;
import com.mysql.sync.model.SyncResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Core synchronization service that applies CDC events to slave databases
 */
@Service
public class SyncService {

    private static final Logger logger = LoggerFactory.getLogger(SyncService.class);

    private final DatabaseConfig databaseConfig;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter syncSuccessCounter;
    private final Counter syncFailureCounter;
    private final Timer syncLatencyTimer;

    @Value("${sync.tool.batch-size:1000}")
    private int batchSize;

    @Value("${sync.tool.retry.max-attempts:3}")
    private int maxRetryAttempts;

    @Autowired
    public SyncService(DatabaseConfig databaseConfig, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.databaseConfig = databaseConfig;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        this.syncSuccessCounter = Counter.builder("sync.operations.success")
                .description("Number of successful sync operations")
                .register(meterRegistry);

        this.syncFailureCounter = Counter.builder("sync.operations.failure")
                .description("Number of failed sync operations")
                .register(meterRegistry);

        this.syncLatencyTimer = Timer.builder("sync.latency")
                .description("Sync operation latency")
                .register(meterRegistry);
    }

    /**
     * Process a CDC event and apply it to all registered slave databases
     */
    public CompletableFuture<Map<String, SyncResult>> processEvent(String eventJson) {
        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try {
                CdcEvent event = objectMapper.readValue(eventJson, CdcEvent.class);
                
                if (event.getPayload() == null || event.getPayload().getSource() == null) {
                    logger.warn("Received event with null payload or source: {}", eventJson);
                    return Map.of();
                }

                String operation = event.getPayload().getOperation();
                String tableName = event.getPayload().getSource().getTable();
                String database = event.getPayload().getSource().getDatabase();

                logger.info("Processing CDC event: operation={}, table={}.{}, timestamp={}", 
                    operation, database, tableName, event.getPayload().getTimestamp());

                // Track operation type
                Counter.builder("sync.operations.total")
                    .tag("operation", operation)
                    .register(meterRegistry)
                    .increment();

                // Apply to all slave databases
                Map<String, DataSource> slaveDatabases = databaseConfig.getAllSlaveDataSources();
                return slaveDatabases.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> applySyncToSlave(entry.getKey(), entry.getValue(), event)
                    ));

            } catch (Exception e) {
                logger.error("Error processing CDC event: {}", eventJson, e);
                syncFailureCounter.increment();
                return Map.of("error", SyncResult.builder()
                    .status(SyncResult.Status.FAILED)
                    .message("Failed to parse CDC event: " + e.getMessage())
                    .build());
            } finally {
                sample.stop(syncLatencyTimer);
            }
        });
    }

    /**
     * Apply sync operation to a specific slave database
     */
    @Retryable(value = {SQLException.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public SyncResult applySyncToSlave(String slaveId, DataSource slaveDataSource, CdcEvent event) {
        Instant startTime = Instant.now();
        
        try {
            String operation = event.getPayload().getOperation();
            String tableName = event.getPayload().getSource().getTable();
            Map<String, Object> after = event.getPayload().getAfter();
            Map<String, Object> before = event.getPayload().getBefore();

            switch (operation) {
                case "c": // Create (INSERT)
                    return performInsert(slaveId, slaveDataSource, tableName, after, startTime);
                    
                case "u": // Update
                    return performUpdate(slaveId, slaveDataSource, tableName, before, after, startTime);
                    
                case "d": // Delete
                    return performDelete(slaveId, slaveDataSource, tableName, before, startTime);
                    
                case "r": // Read (snapshot)
                    return performInsert(slaveId, slaveDataSource, tableName, after, startTime);
                    
                default:
                    logger.warn("Unknown operation type: {} for slave: {}", operation, slaveId);
                    return SyncResult.builder()
                        .slaveId(slaveId)
                        .tableName(tableName)
                        .operation(operation)
                        .status(SyncResult.Status.SKIPPED)
                        .message("Unknown operation type: " + operation)
                        .timestamp(startTime)
                        .build();
            }
            
        } catch (Exception e) {
            logger.error("Error applying sync to slave {}: {}", slaveId, e.getMessage(), e);
            syncFailureCounter.increment();
            
            return SyncResult.builder()
                .slaveId(slaveId)
                .operation(event.getPayload().getOperation())
                .tableName(event.getPayload().getSource().getTable())
                .status(SyncResult.Status.FAILED)
                .message("Sync failed: " + e.getMessage())
                .timestamp(startTime)
                .processingTimeMs(Instant.now().toEpochMilli() - startTime.toEpochMilli())
                .build();
        }
    }

    private SyncResult performInsert(String slaveId, DataSource dataSource, String tableName, 
                                   Map<String, Object> data, Instant startTime) throws SQLException {
        
        if (data == null || data.isEmpty()) {
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("INSERT")
                .status(SyncResult.Status.SKIPPED)
                .message("No data to insert")
                .timestamp(startTime)
                .build();
        }

        String columns = String.join(", ", data.keySet());
        String placeholders = data.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", 
            tableName, columns, placeholders,
            data.keySet().stream().map(k -> k + "=VALUES(" + k + ")").collect(Collectors.joining(", ")));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            int paramIndex = 1;
            for (Object value : data.values()) {
                stmt.setObject(paramIndex++, value);
            }
            
            int rowsAffected = stmt.executeUpdate();
            syncSuccessCounter.increment();
            
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("INSERT")
                .status(SyncResult.Status.SUCCESS)
                .message("Inserted/Updated " + rowsAffected + " rows")
                .timestamp(startTime)
                .processingTimeMs(Instant.now().toEpochMilli() - startTime.toEpochMilli())
                .build();
        }
    }

    private SyncResult performUpdate(String slaveId, DataSource dataSource, String tableName, 
                                   Map<String, Object> before, Map<String, Object> after, Instant startTime) throws SQLException {
        
        if (after == null || after.isEmpty()) {
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("UPDATE")
                .status(SyncResult.Status.SKIPPED)
                .message("No data to update")
                .timestamp(startTime)
                .build();
        }

        // Validate that we have before data to identify which row to update
        if (before == null || before.isEmpty()) {
            logger.warn("UPDATE operation without before data for table {}, slave {}. Cannot safely identify row to update.", 
                       tableName, slaveId);
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("UPDATE")
                .status(SyncResult.Status.FAILED)
                .message("UPDATE operation requires before data to identify target row")
                .timestamp(startTime)
                .processingTimeMs(Instant.now().toEpochMilli() - startTime.toEpochMilli())
                .build();
        }

        // Build UPDATE statement with WHERE clause based on before values
        String setClause = after.keySet().stream()
            .map(k -> k + " = ?")
            .collect(Collectors.joining(", "));
            
        String whereClause = before.keySet().stream()
            .map(k -> k + " = ?")
            .collect(Collectors.joining(" AND "));

        String sql = String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            int paramIndex = 1;
            
            // Set UPDATE values
            for (Object value : after.values()) {
                stmt.setObject(paramIndex++, value);
            }
            
            // Set WHERE values (we know before is not null due to validation above)
            for (Object value : before.values()) {
                stmt.setObject(paramIndex++, value);
            }
            
            int rowsAffected = stmt.executeUpdate();
            syncSuccessCounter.increment();
            
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("UPDATE")
                .status(SyncResult.Status.SUCCESS)
                .message("Updated " + rowsAffected + " rows")
                .timestamp(startTime)
                .processingTimeMs(Instant.now().toEpochMilli() - startTime.toEpochMilli())
                .build();
        }
    }

    private SyncResult performDelete(String slaveId, DataSource dataSource, String tableName, 
                                   Map<String, Object> data, Instant startTime) throws SQLException {
        
        if (data == null || data.isEmpty()) {
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("DELETE")
                .status(SyncResult.Status.SKIPPED)
                .message("No data to delete")
                .timestamp(startTime)
                .build();
        }

        String whereClause = data.keySet().stream()
            .map(k -> k + " = ?")
            .collect(Collectors.joining(" AND "));
            
        String sql = String.format("DELETE FROM %s WHERE %s", tableName, whereClause);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            int paramIndex = 1;
            for (Object value : data.values()) {
                stmt.setObject(paramIndex++, value);
            }
            
            int rowsAffected = stmt.executeUpdate();
            syncSuccessCounter.increment();
            
            return SyncResult.builder()
                .slaveId(slaveId)
                .tableName(tableName)
                .operation("DELETE")
                .status(SyncResult.Status.SUCCESS)
                .message("Deleted " + rowsAffected + " rows")
                .timestamp(startTime)
                .processingTimeMs(Instant.now().toEpochMilli() - startTime.toEpochMilli())
                .build();
        }
    }
}