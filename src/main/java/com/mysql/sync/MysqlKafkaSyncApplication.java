package com.mysql.sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for MySQL Kafka Sync Tool
 * 
 * This application provides real-time synchronization between MySQL master and slave databases
 * using Apache Kafka as the message streaming platform.
 */
@SpringBootApplication
@EnableKafka
@EnableAsync
@EnableScheduling
public class MysqlKafkaSyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(MysqlKafkaSyncApplication.class, args);
    }
}