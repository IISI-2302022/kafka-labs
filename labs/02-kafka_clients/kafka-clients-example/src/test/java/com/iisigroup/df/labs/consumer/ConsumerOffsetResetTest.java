package com.iisigroup.df.labs.consumer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;

@Slf4j
public class ConsumerOffsetResetTest {

    @Test
    public void latestConsume() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition，每個 Partition 只會被群組內的一個 Consumer 消費
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "latestConsume");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            // 訂閱 Topic（支援訂閱多個 Topic）
            kafkaConsumer.subscribe(topics);
            // 持續拉取訊息的無限迴圈
            while (true) {
                // poll() 為 Consumer 的核心方法：拉取訊息、觸發心跳、參與 Rebalance
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }

            }
        }
    }

    @Test
    public void earliestConsume() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition，每個 Partition 只會被群組內的一個 Consumer 消費
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "earliestConsume");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            // 訂閱 Topic（支援訂閱多個 Topic）
            kafkaConsumer.subscribe(topics);
            // 持續拉取訊息的無限迴圈
            while (true) {
                // poll() 為 Consumer 的核心方法：拉取訊息、觸發心跳、參與 Rebalance
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }

            }
        }
    }

    @Test
    public void noneConsume() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition，每個 Partition 只會被群組內的一個 Consumer 消費
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "noneConsume");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            // 訂閱 Topic（支援訂閱多個 Topic）
            kafkaConsumer.subscribe(topics);
            // 持續拉取訊息的無限迴圈
            while (true) {
                // poll() 為 Consumer 的核心方法：拉取訊息、觸發心跳、參與 Rebalance
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }

            }
        }
    }
}
