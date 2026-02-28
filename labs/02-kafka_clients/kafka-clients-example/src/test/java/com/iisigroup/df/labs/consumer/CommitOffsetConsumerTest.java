package com.iisigroup.df.labs.consumer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;


@Slf4j
public class CommitOffsetConsumerTest {

    // 自動 consumer offset commit
    @Test
    public void declarativeAutoCommit() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_auto_commit");

        // 開啟自動 commit
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 背景執行續每秒 commit 一次
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

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

    // 手動 consumer offset commit : 同步 commit
    @Test
    public void manuallySyncCommit() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_sync");

        // 開啟手動 commit
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
                // 程式中需要調用 commit , commit 成功才會往下個迴圈繼續
                kafkaConsumer.commitSync();
            }
        }
    }

    // 手動 consumer offset commit : 非同步 commit
    @Test
    public void manuallyAsyncCommit() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_async");

        // 開啟手動 commit
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
                // 程式中需要調用 commit , 這個 commit 是非同步的 , 對此執行續來說,方法調用算是馬上結束
                kafkaConsumer.commitAsync();
            }
        }
    }


}