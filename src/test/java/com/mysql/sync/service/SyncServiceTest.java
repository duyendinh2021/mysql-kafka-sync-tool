package com.mysql.sync.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.sync.config.DatabaseConfig;
import com.mysql.sync.model.CdcEvent;
import com.mysql.sync.model.SyncResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SyncServiceTest {

    @Mock
    private DatabaseConfig databaseConfig;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    private SyncService syncService;
    private ObjectMapper objectMapper;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        meterRegistry = new SimpleMeterRegistry();
        syncService = new SyncService(databaseConfig, objectMapper, meterRegistry);
    }

    @Test
    void testUpdateWithoutBeforeData_ShouldFailSafely() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> after = Map.of("id", 1, "name", "updated_name");
        Map<String, Object> before = null; // This is the case we're testing
        Instant startTime = Instant.now();

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("u");
        payload.setAfter(after);
        payload.setBefore(before);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.FAILED, result.getStatus());
        assertEquals("UPDATE operation requires before data to identify target row", result.getMessage());
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("UPDATE", result.getOperation());
        
        // Verify no database connection was made
        verify(dataSource, never()).getConnection();
    }

    @Test
    void testUpdateWithEmptyBeforeData_ShouldFailSafely() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> after = Map.of("id", 1, "name", "updated_name");
        Map<String, Object> before = new HashMap<>(); // Empty map - also dangerous
        Instant startTime = Instant.now();

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("u");
        payload.setAfter(after);
        payload.setBefore(before);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.FAILED, result.getStatus());
        assertEquals("UPDATE operation requires before data to identify target row", result.getMessage());
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("UPDATE", result.getOperation());
        
        // Verify no database connection was made
        verify(dataSource, never()).getConnection();
    }

    @Test
    void testUpdateWithValidBeforeData_ShouldSucceed() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> after = Map.of("id", 1, "name", "updated_name", "status", "active");
        Map<String, Object> before = Map.of("id", 1, "name", "old_name", "status", "inactive");

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("u");
        payload.setAfter(after);
        payload.setBefore(before);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Mock database interactions
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.SUCCESS, result.getStatus());
        assertEquals("Updated 1 rows", result.getMessage());
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("UPDATE", result.getOperation());

        // Verify the SQL was properly constructed (verify it contains UPDATE and WHERE but not exact order)
        verify(connection).prepareStatement(argThat(sql -> 
            sql.startsWith("UPDATE test_table SET") && 
            sql.contains("WHERE") &&
            sql.contains("id = ?") &&
            sql.contains("name = ?") &&
            sql.contains("status = ?")
        ));
        
        // Verify parameters were set correctly (3 for SET + 3 for WHERE = 6 total)
        verify(preparedStatement, times(6)).setObject(anyInt(), any());
        verify(preparedStatement).executeUpdate();
    }

    @Test
    void testInsertOperation_ShouldSucceed() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> after = Map.of("id", 1, "name", "new_name", "status", "active");

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("c");
        payload.setAfter(after);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Mock database interactions
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.SUCCESS, result.getStatus());
        assertEquals("Inserted/Updated 1 rows", result.getMessage());
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("INSERT", result.getOperation());

        // Verify the SQL includes ON DUPLICATE KEY UPDATE
        verify(connection).prepareStatement(contains("INSERT INTO"));
        verify(connection).prepareStatement(contains("ON DUPLICATE KEY UPDATE"));
        verify(preparedStatement, times(3)).setObject(anyInt(), any());
        verify(preparedStatement).executeUpdate();
    }

    @Test
    void testDeleteOperation_ShouldSucceed() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> before = Map.of("id", 1, "name", "to_delete");

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("d");
        payload.setBefore(before);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Mock database interactions
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.SUCCESS, result.getStatus());
        assertEquals("Deleted 1 rows", result.getMessage());
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("DELETE", result.getOperation());

        // Verify the SQL was properly constructed
        verify(connection).prepareStatement(argThat(sql -> 
            sql.startsWith("DELETE FROM test_table WHERE") &&
            sql.contains("id = ?") &&
            sql.contains("name = ?")
        ));
        verify(preparedStatement, times(2)).setObject(anyInt(), any());
        verify(preparedStatement).executeUpdate();
    }

    @Test
    void testDeleteWithoutBeforeData_ShouldSkip() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("d");
        payload.setBefore(null); // No before data for delete
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.SKIPPED, result.getStatus());
        assertEquals("No data to delete", result.getMessage());
        
        // Verify no database connection was made
        verify(dataSource, never()).getConnection();
    }

    @Test
    void testUnknownOperation_ShouldSkip() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("x"); // Unknown operation
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.SKIPPED, result.getStatus());
        assertEquals("Unknown operation type: x", result.getMessage());
        assertEquals("x", result.getOperation());
    }

    @Test
    void testSqlException_ShouldReturnFailedResult() throws SQLException {
        // Arrange
        String slaveId = "slave1";
        String tableName = "test_table";
        Map<String, Object> after = Map.of("id", 1, "name", "new_name");

        CdcEvent.Payload payload = new CdcEvent.Payload();
        payload.setOperation("c");
        payload.setAfter(after);
        
        CdcEvent.Source source = new CdcEvent.Source();
        source.setTable(tableName);
        source.setDatabase("test_db");
        payload.setSource(source);

        CdcEvent event = new CdcEvent();
        event.setPayload(payload);

        // Mock database to throw exception
        when(dataSource.getConnection()).thenThrow(new SQLException("Database connection failed"));

        // Act
        SyncResult result = syncService.applySyncToSlave(slaveId, dataSource, event);

        // Assert
        assertEquals(SyncResult.Status.FAILED, result.getStatus());
        assertTrue(result.getMessage().contains("Sync failed: Database connection failed"));
        assertEquals(slaveId, result.getSlaveId());
        assertEquals(tableName, result.getTableName());
        assertEquals("c", result.getOperation());
    }
}