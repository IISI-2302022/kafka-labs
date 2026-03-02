package com.iisigroup.df.labs.consumer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;

@Slf4j
public class ConsumerGroupTest {

    // consumer group partition 分配機制: Range + CooperativeStickyAssignor
    @Test
    public void defaultPartitionAssignmentStrategyConsumer0() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }


    // consumer group partition 分配機制: Range + CooperativeStickyAssignor
    @Test
    public void defaultPartitionAssignmentStrategyConsumer1() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    // consumer group partition 分配機制: Range + CooperativeStickyAssignor
    @Test
    public void defaultPartitionAssignmentStrategyConsumer2() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    // consumer group partition 分配機制: RoundRobinAssignor
    @Test
    public void rBPartitionAssignmentStrategyConsumer0() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_rb");

        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    // consumer group partition 分配機制: RoundRobinAssignor
    @Test
    public void rBPartitionAssignmentStrategyConsumer1() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_rb");

        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    // consumer group partition 分配機制: RoundRobinAssignor
    @Test
    public void rBPartitionAssignmentStrategyConsumer2() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_rb");

        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

}
