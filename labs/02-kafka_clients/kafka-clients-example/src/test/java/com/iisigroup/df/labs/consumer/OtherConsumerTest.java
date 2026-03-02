package com.iisigroup.df.labs.consumer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;


@Slf4j
public class OtherConsumerTest {


    @Test
    public void consumePartition() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_partition");
        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topicPartitions = new ArrayList<TopicPartition>();
            topicPartitions.add(new TopicPartition(TEST_TOPIC, 0));
            topicPartitions.add(new TopicPartition(TEST_TOPIC, 1));
            topicPartitions.add(new TopicPartition(TEST_TOPIC, 6));
            kafkaConsumer.assign(topicPartitions);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    @Test
    public void consumeFromSpecificOffset() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_offset");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {

            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);

            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }
            for (TopicPartition tp : assignment) {
                kafkaConsumer.seek(tp, 20);
            }

            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }


    @Test
    public void consumeFromSpecificOffsetUseConsumerGroupRebalance() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_offset_rebalance");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {

            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("Partitions revoked: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Partitions assigned: {}", partitions);
                    for (TopicPartition tp : partitions) {
                        kafkaConsumer.seek(tp, 20);
                    }
                }
            });

            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    @Test
    public void consumeFromSpecificDateTime() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_datetime");


        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);

            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }

            val value = System.currentTimeMillis() - 86400000;
            val timestampToSearch = new HashMap<TopicPartition, Long>();
            for (TopicPartition topicPartition : assignment) {
                timestampToSearch.put(topicPartition, value);
            }

            val offsets = kafkaConsumer.offsetsForTimes(timestampToSearch);
            for (TopicPartition topicPartition : assignment) {
                val offsetAndTimestamp = offsets.get(topicPartition);
                if (offsetAndTimestamp != null) {
                    kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
                }
            }

            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }


}