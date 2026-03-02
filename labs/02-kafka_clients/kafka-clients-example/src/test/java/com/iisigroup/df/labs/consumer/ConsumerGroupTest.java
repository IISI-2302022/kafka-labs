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

/**
 * Kafka Consumer Group 分區分配策略示範。
 * <p>
 * 本測試類別展示如何在同一個 Consumer Group 中啟動多個 Consumer，
 * 並觀察不同分區分配策略（Partition Assignment Strategy）的行為差異。
 * </p>
 *
 * <h3>分區分配策略</h3>
 * <ul>
 *     <li><b>RangeAssignor（預設）</b>— 以 Topic 為單位，將 Partition 以連續範圍分配給各 Consumer。
 *         例如 7 個 Partition、3 個 Consumer → Consumer0 得到 [0,1,2]，Consumer1 得到 [3,4]，Consumer2 得到 [5,6]</li>
 *     <li><b>RoundRobinAssignor</b>— 將所有 Topic 的 Partition 混合後，以輪詢方式逐一分配給各 Consumer，
 *         分配結果較為均勻</li>
 *     <li><b>StickyAssignor</b>— 在均勻分配的基礎上，盡量維持 Rebalance 前的分配結果，減少 Partition 搬遷</li>
 *     <li><b>CooperativeStickyAssignor</b>— 支援增量式（Incremental）Rebalance，不需一次撤回所有 Partition</li>
 * </ul>
 *
 * <h3>實驗方式</h3>
 * <p>
 * 分別執行 Consumer0、Consumer1、Consumer2 測試方法（模擬三個 Consumer 實例），
 * 觀察各 Consumer 被分配到的 Partition 編號。
 * </p>
 *
 * @see org.apache.kafka.clients.consumer.RangeAssignor
 * @see org.apache.kafka.clients.consumer.RoundRobinAssignor
 */
@Slf4j
public class ConsumerGroupTest {

    /**
     * 預設分區策略（RangeAssignor）— Consumer 0。
     * <p>
     * 群組 ID 為 {@code "test"}，使用預設的 RangeAssignor。
     * 啟動多個相同 group.id 的 Consumer 後，Kafka 會觸發 Rebalance，
     * 以 Range 策略分配 Partition。
     * </p>
     */
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

    /**
     * 預設分區策略（RangeAssignor）— Consumer 1。
     * <p>
     * 與 Consumer0 使用相同的 group.id {@code "test"}，
     * 加入群組後會觸發 Rebalance，重新分配 Partition。
     * </p>
     */
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

    /**
     * 預設分區策略（RangeAssignor）— Consumer 2。
     * <p>
     * 與 Consumer0、Consumer1 使用相同的 group.id {@code "test"}。
     * <b>注意：</b>若 Consumer 數量超過 Partition 數量，多出的 Consumer 將處於閒置（idle）狀態。
     * </p>
     */
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

    /**
     * RoundRobin 分區策略 — Consumer 0。
     * <p>
     * 群組 ID 為 {@code "test_rb"}，透過 {@code PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * 指定使用 {@link RoundRobinAssignor}。
     * RoundRobin 會將所有訂閱 Topic 的 Partition 以輪詢方式分配，
     * 相較 Range 策略更為均勻（尤其在訂閱多個 Topic 時）。
     * </p>
     */
    @Test
    public void rBPartitionAssignmentStrategyConsumer0() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_rb");

        // 指定 RoundRobin 分區分配策略
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

    /**
     * RoundRobin 分區策略 — Consumer 1。
     * <p>
     * 與 Consumer0 相同群組 {@code "test_rb"}，使用 RoundRobin 策略。
     * </p>
     */
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

    /**
     * RoundRobin 分區策略 — Consumer 2。
     * <p>
     * 與 Consumer0、Consumer1 相同群組 {@code "test_rb"}，使用 RoundRobin 策略。
     * </p>
     */
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
