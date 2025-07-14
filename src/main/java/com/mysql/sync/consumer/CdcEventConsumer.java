package com.mysql.sync.consumer;

import com.mysql.sync.service.SyncService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for handling CDC events from Debezium
 * Listens to MySQL change events and triggers synchronization to slave databases
 */
@Component
public class CdcEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CdcEventConsumer.class);

    private final SyncService syncService;
    private final Counter messagesReceivedCounter;
    private final Counter messagesProcessedCounter;
    private final Counter messagesFailedCounter;

    @Autowired
    public CdcEventConsumer(SyncService syncService, MeterRegistry meterRegistry) {
        this.syncService = syncService;
        
        // Initialize metrics
        this.messagesReceivedCounter = Counter.builder("kafka.messages.received")
                .description("Number of Kafka messages received")
                .register(meterRegistry);
                
        this.messagesProcessedCounter = Counter.builder("kafka.messages.processed")
                .description("Number of Kafka messages successfully processed")
                .register(meterRegistry);
                
        this.messagesFailedCounter = Counter.builder("kafka.messages.failed")
                .description("Number of Kafka messages that failed processing")
                .register(meterRegistry);
    }

    /**
     * Listen to all CDC topics with the configured prefix pattern
     * Topics format: {prefix}.{database}.{table}
     */
    @KafkaListener(
        topicPattern = "mysql-server\\..*",
        groupId = "mysql-sync-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleCdcEvent(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment acknowledgment) {
        
        messagesReceivedCounter.increment();
        
        try {
            logger.debug("Received CDC event from topic: {}, partition: {}, offset: {}", topic, partition, offset);
            
            // Extract table and database from topic name
            String[] topicParts = topic.split("\\.");
            if (topicParts.length >= 3) {
                String database = topicParts[1];
                String table = topicParts[2];
                
                logger.info("Processing CDC event for {}.{}", database, table);
            }
            
            // Process the event asynchronously
            syncService.processEvent(message)
                .thenAccept(results -> {
                    logger.info("Sync completed for topic: {} with {} results", topic, results.size());
                    results.forEach((slaveId, result) -> {
                        if (result.getStatus() == com.mysql.sync.model.SyncResult.Status.SUCCESS) {
                            logger.debug("Sync success for slave {}: {}", slaveId, result.getMessage());
                        } else {
                            logger.warn("Sync issue for slave {}: {} - {}", slaveId, result.getStatus(), result.getMessage());
                        }
                    });
                    
                    // Acknowledge the message after successful processing
                    acknowledgment.acknowledge();
                    messagesProcessedCounter.increment();
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to process CDC event from topic: {}", topic, throwable);
                    messagesFailedCounter.increment();
                    
                    // For now, acknowledge failed messages to avoid infinite retry
                    // In production, consider implementing dead letter queue
                    acknowledgment.acknowledge();
                    return null;
                });
                
        } catch (Exception e) {
            logger.error("Error handling CDC event from topic: {}", topic, e);
            messagesFailedCounter.increment();
            
            // Acknowledge to prevent infinite retry
            acknowledgment.acknowledge();
        }
    }

    /**
     * Listen to specific inventory table changes
     */
    @KafkaListener(
        topics = "mysql-server.inventory.customers",
        groupId = "mysql-sync-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleCustomerChanges(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        logger.info("Received customer change event from topic: {}", topic);
        handleCdcEvent(message, topic, 0, 0L, acknowledgment);
    }

    /**
     * Listen to specific orders table changes
     */
    @KafkaListener(
        topics = "mysql-server.inventory.orders",
        groupId = "mysql-sync-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderChanges(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        logger.info("Received order change event from topic: {}", topic);
        handleCdcEvent(message, topic, 0, 0L, acknowledgment);
    }

    /**
     * Listen to inventory table changes  
     */
    @KafkaListener(
        topics = "mysql-server.inventory.products",
        groupId = "mysql-sync-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleProductChanges(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        logger.info("Received product change event from topic: {}", topic);
        handleCdcEvent(message, topic, 0, 0L, acknowledgment);
    }
}