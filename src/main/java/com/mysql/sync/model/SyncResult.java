package com.mysql.sync.model;

import java.time.Instant;

/**
 * Represents a sync operation result
 */
public class SyncResult {
    
    public enum Status {
        SUCCESS,
        FAILED,
        SKIPPED,
        RETRY
    }

    private final String slaveId;
    private final String tableName;
    private final String operation;
    private final Status status;
    private final String message;
    private final Instant timestamp;
    private final long processingTimeMs;
    private final String recordId;

    private SyncResult(Builder builder) {
        this.slaveId = builder.slaveId;
        this.tableName = builder.tableName;
        this.operation = builder.operation;
        this.status = builder.status;
        this.message = builder.message;
        this.timestamp = builder.timestamp;
        this.processingTimeMs = builder.processingTimeMs;
        this.recordId = builder.recordId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String slaveId;
        private String tableName;
        private String operation;
        private Status status;
        private String message;
        private Instant timestamp = Instant.now();
        private long processingTimeMs;
        private String recordId;

        public Builder slaveId(String slaveId) {
            this.slaveId = slaveId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public Builder status(Status status) {
            this.status = status;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder processingTimeMs(long processingTimeMs) {
            this.processingTimeMs = processingTimeMs;
            return this;
        }

        public Builder recordId(String recordId) {
            this.recordId = recordId;
            return this;
        }

        public SyncResult build() {
            return new SyncResult(this);
        }
    }

    // Getters
    public String getSlaveId() { return slaveId; }
    public String getTableName() { return tableName; }
    public String getOperation() { return operation; }
    public Status getStatus() { return status; }
    public String getMessage() { return message; }
    public Instant getTimestamp() { return timestamp; }
    public long getProcessingTimeMs() { return processingTimeMs; }
    public String getRecordId() { return recordId; }

    @Override
    public String toString() {
        return String.format("SyncResult{slaveId='%s', table='%s', op='%s', status=%s, time=%dms, recordId='%s', message='%s'}",
                slaveId, tableName, operation, status, processingTimeMs, recordId, message);
    }
}