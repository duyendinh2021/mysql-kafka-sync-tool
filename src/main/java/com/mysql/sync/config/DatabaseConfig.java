package com.mysql.sync.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Database configuration for MySQL master and slave connections
 * Manages connection pools for multiple slave databases
 */
@Configuration
public class DatabaseConfig {

    @Value("${spring.datasource.master.jdbc-url}")
    private String masterJdbcUrl;

    @Value("${spring.datasource.master.username}")
    private String masterUsername;

    @Value("${spring.datasource.master.password}")
    private String masterPassword;

    @Value("${spring.datasource.slave.default.jdbc-url}")
    private String defaultSlaveJdbcUrl;

    @Value("${spring.datasource.slave.default.username}")
    private String defaultSlaveUsername;

    @Value("${spring.datasource.slave.default.password}")
    private String defaultSlavePassword;

    private final Map<String, DataSource> slaveDataSources = new ConcurrentHashMap<>();

    @Bean
    @Primary
    public DataSource masterDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(masterJdbcUrl);
        config.setUsername(masterUsername);
        config.setPassword(masterPassword);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        
        // Connection pool settings
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(20000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(1200000);
        config.setLeakDetectionThreshold(60000);
        
        // Performance settings
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        
        config.setPoolName("MasterConnectionPool");
        
        return new HikariDataSource(config);
    }

    @Bean
    public DataSource defaultSlaveDataSource() {
        return createSlaveDataSource("default", defaultSlaveJdbcUrl, defaultSlaveUsername, defaultSlavePassword);
    }

    /**
     * Create a new slave database connection
     * 
     * @param slaveId Unique identifier for the slave
     * @param jdbcUrl JDBC URL for the slave database
     * @param username Database username
     * @param password Database password
     * @return Configured DataSource for the slave
     */
    public DataSource createSlaveDataSource(String slaveId, String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        
        // Connection pool settings
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(20000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(1200000);
        config.setLeakDetectionThreshold(60000);
        
        // Performance settings
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        
        config.setPoolName("SlaveConnectionPool-" + slaveId);
        
        DataSource dataSource = new HikariDataSource(config);
        slaveDataSources.put(slaveId, dataSource);
        
        return dataSource;
    }

    /**
     * Get a slave data source by ID
     */
    public DataSource getSlaveDataSource(String slaveId) {
        return slaveDataSources.get(slaveId);
    }

    /**
     * Get all registered slave data sources
     */
    public Map<String, DataSource> getAllSlaveDataSources() {
        return new HashMap<>(slaveDataSources);
    }

    /**
     * Remove a slave data source
     */
    public void removeSlaveDataSource(String slaveId) {
        DataSource dataSource = slaveDataSources.remove(slaveId);
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }
}