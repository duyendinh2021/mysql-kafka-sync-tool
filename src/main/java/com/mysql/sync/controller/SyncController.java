package com.mysql.sync.controller;

import com.mysql.sync.config.DatabaseConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for sync tool monitoring and management
 */
@RestController
@RequestMapping("/sync")
public class SyncController {

    private final DatabaseConfig databaseConfig;
    private final MeterRegistry meterRegistry;

    @Autowired
    public SyncController(DatabaseConfig databaseConfig, MeterRegistry meterRegistry) {
        this.databaseConfig = databaseConfig;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Get sync status for all slaves
     */
    @GetMapping("/status")
    public Map<String, Object> getSyncStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("slaves", databaseConfig.getAllSlaveDataSources().keySet());
        status.put("slave-count", databaseConfig.getAllSlaveDataSources().size());
        
        // Add metrics
        status.put("metrics", getMetricsSummary());
        
        return status;
    }

    /**
     * Get list of registered slave databases
     */
    @GetMapping("/slaves")
    public Map<String, Object> getSlaves() {
        Map<String, Object> slaves = new HashMap<>();
        Map<String, DataSource> slaveDataSources = databaseConfig.getAllSlaveDataSources();
        
        slaveDataSources.forEach((slaveId, dataSource) -> {
            Map<String, Object> slaveInfo = new HashMap<>();
            slaveInfo.put("id", slaveId);
            slaveInfo.put("status", "active");
            slaves.put(slaveId, slaveInfo);
        });
        
        return slaves;
    }

    /**
     * Register a new slave database
     */
    @PostMapping("/slaves")
    public Map<String, Object> registerSlave(@RequestBody Map<String, String> slaveConfig) {
        String slaveId = slaveConfig.get("slaveId");
        String jdbcUrl = slaveConfig.get("jdbcUrl");
        String username = slaveConfig.get("username");
        String password = slaveConfig.get("password");

        try {
            DataSource dataSource = databaseConfig.createSlaveDataSource(slaveId, jdbcUrl, username, password);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Slave database registered successfully");
            response.put("slaveId", slaveId);
            
            return response;
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Failed to register slave: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * Remove a slave database
     */
    @DeleteMapping("/slaves/{slaveId}")
    public Map<String, Object> removeSlave(@PathVariable String slaveId) {
        try {
            databaseConfig.removeSlaveDataSource(slaveId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Slave database removed successfully");
            response.put("slaveId", slaveId);
            
            return response;
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Failed to remove slave: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * Get sync metrics summary
     */
    @GetMapping("/metrics")
    public Map<String, Object> getMetricsSummary() {
        Map<String, Object> metrics = new HashMap<>();
        
        try {
            metrics.put("sync-success-total", 
                meterRegistry.find("sync.operations.success").counter().count());
            metrics.put("sync-failure-total", 
                meterRegistry.find("sync.operations.failure").counter().count());
            metrics.put("kafka-messages-received", 
                meterRegistry.find("kafka.messages.received").counter().count());
            metrics.put("kafka-messages-processed", 
                meterRegistry.find("kafka.messages.processed").counter().count());
            metrics.put("kafka-messages-failed", 
                meterRegistry.find("kafka.messages.failed").counter().count());
        } catch (Exception e) {
            metrics.put("error", "Unable to retrieve metrics: " + e.getMessage());
        }
        
        return metrics;
    }

    /**
     * Get application information
     */
    @GetMapping("/info")
    public Map<String, Object> getInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("application", "MySQL Kafka Sync Tool");
        info.put("version", "1.0.0");
        info.put("description", "Real-time MySQL to Kafka synchronization tool");
        
        Map<String, Object> features = new HashMap<>();
        features.put("cdc-enabled", true);
        features.put("multi-slave-support", true);
        features.put("metrics-enabled", true);
        features.put("health-checks", true);
        
        info.put("features", features);
        
        return info;
    }
}