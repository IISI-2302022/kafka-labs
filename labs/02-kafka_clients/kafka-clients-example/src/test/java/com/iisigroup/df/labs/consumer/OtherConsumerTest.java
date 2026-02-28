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


    // 針對 topic 之某 partition 消費
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
            // 事前已知道要拉取哪些 topic + partition , 就使用 assign 來指定
            kafkaConsumer.assign(topicPartitions);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    // 從指定 offset 開始消費
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

            // 先抓到 consumer 被分派到哪些 topic + partition
            // 缺點: rebalance 後不會再做 seek 了
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }
            // 針對這些被分配到的 topic + partition 指定 offset 來消費
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
                    // 在 partition 被收回前可以做一些事，例如提交 offset
                    log.info("Partitions revoked: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Partitions assigned: {}", partitions);
                    for (TopicPartition tp : partitions) {
                        // 缺點: 每次 rebalance 都會從 offset 20 開始消費 , 可能會重複消費資料
                        // 只是範例而已 , 實務上需要額外判斷 offset 是否已處理過
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

    // 從指定時間後之 offset 開始消費
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

            // 一天前
            val value = System.currentTimeMillis() - 86400000;
            val timestampToSearch = new HashMap<TopicPartition, Long>();
            for (TopicPartition topicPartition : assignment) {
                timestampToSearch.put(topicPartition, value);
            }

            // 抓出所有被分派之 partition 在指定時間後的 offset
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

    // 搭配 producer 交易成功資料或非交易資料進行讀取
    @Test
    public void consumeReadCommitForTransaction() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_read_committed");

        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

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