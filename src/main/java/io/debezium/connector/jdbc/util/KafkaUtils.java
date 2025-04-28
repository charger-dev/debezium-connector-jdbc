/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.SinkRecordDescriptor;

/**
 * Utility methods for working with Kafka.
 *
 * @author Kevin Cui
 */
public class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * Checks if the current batch will consume all remaining records in a topic.
     *
     * @param record The SinkRecord to check
     * @param bootstrapServers The Kafka bootstrap servers
     * @param batchSize The batch size
     * @return true if this batch will consume all remaining records, false otherwise
     */
    public static boolean willConsumeAllRemainingRecords(SinkRecordDescriptor record, String bootstrapServers, int batchSize) {
        if (record == null) {
            return false;
        }

        String topic = record.getTopicName();
        int partition = record.getPartition();
        long currentOffset = record.getOffset();

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "offset-checker-group");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
            long endOffset = endOffsets.get(topicPartition);

            // Check if current offset is the last record or close to the end
            // We consider it the last batch if the current offset plus the batch size is greater than or equal to the end offset
            boolean willConsume = (currentOffset + batchSize >= endOffset - 1);

            if (willConsume) {
                LOGGER.info("Current batch will consume all remaining records for topic {} partition {}. Current offset: {}, End offset: {}",
                        topic, partition, currentOffset, endOffset);
            }
            else {
                LOGGER.debug("Current batch will NOT consume all remaining records for topic {} partition {}. Current offset: {}, End offset: {}",
                        topic, partition, currentOffset, endOffset);
            }

            return willConsume;
        }
        catch (Exception e) {
            LOGGER.warn("Failed to check if batch will consume all remaining records: {}", e.getMessage());
            return true;
        }
    }

    private KafkaUtils() {
        // Prevent instantiation
    }
}
