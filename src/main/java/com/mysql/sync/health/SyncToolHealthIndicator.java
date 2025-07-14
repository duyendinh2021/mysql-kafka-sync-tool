package com.mysql.sync.health;

import com.mysql.sync.config.DatabaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom health check for sync tool components
 */
@RestController
public class SyncToolHealthIndicator {

    private final DatabaseConfig databaseConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public SyncToolHealthIndicator(DatabaseConfig databaseConfig, KafkaTemplate<String, String> kafkaTemplate) {
        this.databaseConfig = databaseConfig;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/actuator/health/sync")
    public Map<String, Object> health() {
        try {
            Map<String, Object> details = new HashMap<>();
            
            // Check master database connection
            boolean masterHealthy = checkMasterDatabase();
            details.put("master-database", masterHealthy ? "UP" : "DOWN");
            
            // Check slave databases
            Map<String, Boolean> slaveHealth = checkSlaveDatabaes();
            details.put("slave-databases", slaveHealth);
            
            // Check Kafka connectivity
            boolean kafkaHealthy = checkKafkaConnection();
            details.put("kafka", kafkaHealthy ? "UP" : "DOWN");
            
            // Overall health
            boolean overallHealthy = masterHealthy && kafkaHealthy && 
                slaveHealth.values().stream().allMatch(Boolean::booleanValue);
            
            details.put("status", overallHealthy ? "UP" : "DOWN");
            return details;
            
        } catch (Exception e) {
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("status", "DOWN");
            errorDetails.put("error", e.getMessage());
            return errorDetails;
        }
    }

    private boolean checkMasterDatabase() {
        try (Connection connection = databaseConfig.masterDataSource().getConnection()) {
            return connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    private Map<String, Boolean> checkSlaveDatabaes() {
        Map<String, Boolean> slaveHealth = new HashMap<>();
        Map<String, DataSource> slaveDatabases = databaseConfig.getAllSlaveDataSources();
        
        for (Map.Entry<String, DataSource> entry : slaveDatabases.entrySet()) {
            try (Connection connection = entry.getValue().getConnection()) {
                slaveHealth.put(entry.getKey(), connection.isValid(5));
            } catch (SQLException e) {
                slaveHealth.put(entry.getKey(), false);
            }
        }
        
        return slaveHealth;
    }

    private boolean checkKafkaConnection() {
        try {
            // Try to get metadata which tests the connection
            kafkaTemplate.metrics();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}