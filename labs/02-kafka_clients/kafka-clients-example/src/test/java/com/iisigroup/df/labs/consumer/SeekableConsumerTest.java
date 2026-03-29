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

/**
 * Kafka Consumer 進階消費模式示範。
 * <p>
 * 本測試類別展示四種進階消費方式：
 * <ol>
 *     <li><b>指定 Partition 消費</b>— 使用 {@code assign()} 手動指派 Partition，繞過 Consumer Group 機制</li>
 *     <li><b>從指定 Offset 消費</b>— 使用 {@code seek()} 定位到特定 Offset</li>
 *     <li><b>從指定 Offset 消費（搭配 Rebalance Listener）</b>— 在 Rebalance 回呼中定位 Offset</li>
 *     <li><b>從指定時間點消費</b>— 使用 {@code offsetsForTimes()} 查詢特定時間對應的 Offset</li>
 * </ol>
 * </p>
 *
 * @see org.apache.kafka.clients.consumer.KafkaConsumer#assign(Collection)
 * @see org.apache.kafka.clients.consumer.KafkaConsumer#seek(TopicPartition, long)
 * @see org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(Map)
 */
@Slf4j
public class SeekableConsumerTest {


    /**
     * 指定 Partition 消費（Manual Partition Assignment）。
     * <p>
     * 使用 {@code assign()} 手動指派要消費的 Partition（此處為 Partition 0、1、6），
     * 而非透過 {@code subscribe()} 讓 Kafka 自動分配。
     * </p>
     * <p>
     * <b>注意：</b>
     * <ul>
     *     <li>{@code assign()} 不會加入 Consumer Group，因此不會觸發 Rebalance</li>
     *     <li>{@code assign()} 與 {@code subscribe()} 互斥，不可同時使用</li>
     *     <li>適合已知 Partition 數量且需精確控制消費範圍的場景</li>
     * </ul>
     * </p>
     */
    @Test
    public void consumePartition() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：雖然用了 assign() 不走 Consumer Group 機制，但 Kafka 仍需要 group.id 來管理 Offset
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_partition");
        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            // 手動指派要消費的 Partition
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

    /**
     * 從指定 Offset 開始消費（Seek to Specific Offset）。
     * <p>
     * 透過 {@code seek(TopicPartition, offset)} 將 Consumer 的讀取位置定位到指定 Offset。
     * 本範例將所有被分配的 Partition 都定位到 Offset 20。
     * </p>
     * <p>
     * <b>流程：</b>
     * <ol>
     *     <li>先呼叫 {@code subscribe()} 訂閱 Topic</li>
     *     <li>呼叫 {@code poll()} 觸發 Partition 分配（Rebalance）</li>
     *     <li>透過 {@code assignment()} 取得已分配的 Partition 列表</li>
     *     <li>對每個 Partition 呼叫 {@code seek()} 定位到目標 Offset</li>
     * </ol>
     * </p>
     * <p>
     * <b>注意：</b>若指定的 Offset 超過該 Partition 的最大 Offset，
     * Consumer 會根據 {@code auto.offset.reset} 策略決定行為（預設為 latest）。
     * </p>
     */
    @Test
    public void consumeFromSpecificOffset() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_offset");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {

            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);

            // 等待 Partition 分配完成（assignment 不為空表示 Rebalance 已完成）
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }
            // 將所有 Partition 的消費位置設定到 Offset 20
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


    /**
     * 從指定 Offset 開始消費 — 搭配 ConsumerRebalanceListener。
     * <p>
     * 使用 {@link ConsumerRebalanceListener} 在 Rebalance 完成時自動定位 Offset。
     * 相較於手動輪詢等待 assignment，此方式更為優雅且能正確處理後續的 Rebalance。
     * </p>
     * <p>
     * <b>回呼時機：</b>
     * <ul>
     *     <li>{@code onPartitionsRevoked()} — 在 Partition 被撤回前觸發，適合在此提交 Offset</li>
     *     <li>{@code onPartitionsAssigned()} — 在新 Partition 被分配後觸發，適合在此呼叫 {@code seek()}</li>
     * </ul>
     * </p>
     */
    @Test
    public void consumeFromSpecificOffsetUseConsumerGroupRebalance() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_offset_rebalance");

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {

            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            // 訂閱時註冊 Rebalance Listener
            kafkaConsumer.subscribe(topics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Partition 被撤回前觸發（可在此提交已處理的 Offset）
                    log.info("Partitions revoked: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // 新 Partition 分配後觸發 → 定位到目標 Offset
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

    /**
     * 從指定時間點開始消費（Consume from Specific DateTime）。
     * <p>
     * 使用 {@code offsetsForTimes()} API 查詢每個 Partition 在指定時間戳（timestamp）
     * 之後的第一筆訊息 Offset，再透過 {@code seek()} 定位。
     * </p>
     * <p>
     * 本範例將消費起始點設為「目前時間 - 24 小時」（即一天前），
     * 適用於需要回溯消費特定時間範圍資料的場景。
     * </p>
     * <p>
     * <b>注意：</b>若某個 Partition 在指定時間點之後沒有任何訊息，
     * {@code offsetsForTimes()} 對該 Partition 會回傳 null。
     * </p>
     */
    @Test
    public void consumeFromSpecificDateTime() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_from_specific_datetime");


        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);

            // 等待 Partition 分配完成
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }

            // 計算目標時間戳：目前時間 - 24 小時（86400000 ms = 24hr）
            val value = System.currentTimeMillis() - 86400000;
            val timestampToSearch = new HashMap<TopicPartition, Long>();
            for (TopicPartition topicPartition : assignment) {
                timestampToSearch.put(topicPartition, value);
            }

            // 查詢每個 Partition 在目標時間戳之後的第一筆 Offset
            val offsets = kafkaConsumer.offsetsForTimes(timestampToSearch);
            for (TopicPartition topicPartition : assignment) {
                val offsetAndTimestamp = offsets.get(topicPartition);
                if (offsetAndTimestamp != null) {
                    // 定位到查詢到的 Offset
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