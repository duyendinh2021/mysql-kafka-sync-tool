package com.mysql.sync.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a Change Data Capture event from Debezium
 * Contains information about database changes (INSERT, UPDATE, DELETE)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcEvent {

    @JsonProperty("payload")
    private Payload payload;

    @JsonProperty("schema")
    private Schema schema;

    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {
        @JsonProperty("before")
        private Map<String, Object> before;

        @JsonProperty("after")
        private Map<String, Object> after;

        @JsonProperty("source")
        private Source source;

        @JsonProperty("op")
        private String operation;

        @JsonProperty("ts_ms")
        private Long timestampMs;

        @JsonProperty("transaction")
        private Transaction transaction;

        public Map<String, Object> getBefore() {
            return before;
        }

        public void setBefore(Map<String, Object> before) {
            this.before = before;
        }

        public Map<String, Object> getAfter() {
            return after;
        }

        public void setAfter(Map<String, Object> after) {
            this.after = after;
        }

        public Source getSource() {
            return source;
        }

        public void setSource(Source source) {
            this.source = source;
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public Long getTimestampMs() {
            return timestampMs;
        }

        public void setTimestampMs(Long timestampMs) {
            this.timestampMs = timestampMs;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }

        public Instant getTimestamp() {
            return timestampMs != null ? Instant.ofEpochMilli(timestampMs) : null;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source {
        @JsonProperty("version")
        private String version;

        @JsonProperty("connector")
        private String connector;

        @JsonProperty("name")
        private String name;

        @JsonProperty("ts_ms")
        private Long timestampMs;

        @JsonProperty("snapshot")
        private String snapshot;

        @JsonProperty("db")
        private String database;

        @JsonProperty("table")
        private String table;

        @JsonProperty("server_id")
        private Long serverId;

        @JsonProperty("gtid")
        private String gtid;

        @JsonProperty("file")
        private String file;

        @JsonProperty("pos")
        private Long position;

        @JsonProperty("row")
        private Integer row;

        @JsonProperty("thread")
        private Long thread;

        @JsonProperty("query")
        private String query;

        // Getters and setters
        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }

        public String getConnector() { return connector; }
        public void setConnector(String connector) { this.connector = connector; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public Long getTimestampMs() { return timestampMs; }
        public void setTimestampMs(Long timestampMs) { this.timestampMs = timestampMs; }

        public String getSnapshot() { return snapshot; }
        public void setSnapshot(String snapshot) { this.snapshot = snapshot; }

        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }

        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }

        public Long getServerId() { return serverId; }
        public void setServerId(Long serverId) { this.serverId = serverId; }

        public String getGtid() { return gtid; }
        public void setGtid(String gtid) { this.gtid = gtid; }

        public String getFile() { return file; }
        public void setFile(String file) { this.file = file; }

        public Long getPosition() { return position; }
        public void setPosition(Long position) { this.position = position; }

        public Integer getRow() { return row; }
        public void setRow(Integer row) { this.row = row; }

        public Long getThread() { return thread; }
        public void setThread(Long thread) { this.thread = thread; }

        public String getQuery() { return query; }
        public void setQuery(String query) { this.query = query; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Transaction {
        @JsonProperty("id")
        private String id;

        @JsonProperty("total_order")
        private Long totalOrder;

        @JsonProperty("data_collection_order")
        private Long dataCollectionOrder;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public Long getTotalOrder() { return totalOrder; }
        public void setTotalOrder(Long totalOrder) { this.totalOrder = totalOrder; }

        public Long getDataCollectionOrder() { return dataCollectionOrder; }
        public void setDataCollectionOrder(Long dataCollectionOrder) { this.dataCollectionOrder = dataCollectionOrder; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Schema {
        @JsonProperty("type")
        private String type;

        @JsonProperty("fields")
        private Object fields;

        @JsonProperty("optional")
        private Boolean optional;

        @JsonProperty("name")
        private String name;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Object getFields() { return fields; }
        public void setFields(Object fields) { this.fields = fields; }

        public Boolean getOptional() { return optional; }
        public void setOptional(Boolean optional) { this.optional = optional; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}