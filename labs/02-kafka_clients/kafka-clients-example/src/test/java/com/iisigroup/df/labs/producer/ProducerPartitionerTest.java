package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.partitioner.ZeroOnlyPartitioner;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;

/**
 * Kafka Producer 分區策略（Partitioning Strategy）示範。
 * <p>
 * 本測試類別展示 Producer 將訊息分配至不同 Partition 的四種方式：
 * <ol>
 *     <li><b>指定 Partition</b>— 直接在 {@link ProducerRecord} 中指定分區編號</li>
 *     <li><b>相同 Key</b>— 相同 Key 的訊息會被路由到同一個 Partition（hash(key) % numPartitions）</li>
 *     <li><b>不同 Key</b>— 不同 Key 依雜湊值分散至各 Partition</li>
 *     <li><b>自訂 Partitioner</b>— 透過自訂 {@link org.apache.kafka.clients.producer.Partitioner} 控制分區邏輯</li>
 * </ol>
 * </p>
 *
 * <h3>DefaultPartitioner 分區規則</h3>
 * <ul>
 *     <li>若指定 partition → 直接使用該 partition</li>
 *     <li>若有 key → {@code hash(key) % numPartitions}</li>
 *     <li>若無 key → Sticky Partitioning（在同一 batch 內盡量黏在同一 partition，提升吞吐量）</li>
 * </ul>
 *
 * @see org.apache.kafka.clients.producer.internals.DefaultPartitioner
 */
@Slf4j
public class ProducerPartitionerTest {

    /** 用於示範「相同 Key」場景的固定 Key 值 */
    public static final String STATIC_KEY = "staticKey";

    /**
     * 明確指定 Partition 編號發送。
     * <p>
     * 使用四參數建構子 {@code new ProducerRecord(topic, partition, key, value)}，
     * 直接將訊息寫入指定的 Partition。此方式會繞過所有 Partitioner 邏輯。
     * </p>
     */
    @Test
    public void defaultPartitionerProduceWithPartition() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 7; i++) {
                // 第二個參數 i 為指定的 Partition 編號，第三個參數 null 為 key
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, i, null, VALUE_PREFIX + i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send message error", exception);
                        return;
                    }
                    val offset = metadata.offset();
                    val partition = metadata.partition();
                    val timestamp = metadata.timestamp();
                    val topic = metadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                });
            }
        }
    }


    /**
     * 以相同 Key 發送訊息。
     * <p>
     * 所有訊息使用相同的 Key（{@code "staticKey"}），
     * DefaultPartitioner 會對 Key 進行 murmur2 雜湊運算，
     * 因此所有訊息一定會被路由到同一個 Partition。
     * </p>
     * <p>
     * <b>應用場景：</b>需要保證特定業務鍵（如訂單 ID、用戶 ID）的訊息順序時，
     * 可使用固定 Key 確保同一 Key 的訊息都在同一 Partition 中被有序處理。
     * </p>
     */
    @Test
    public void defaultPartitionerProduceWithSameKey() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                // 相同 Key = "staticKey" → 同一 Partition
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, STATIC_KEY, VALUE_PREFIX + i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send message error", exception);
                        return;
                    }
                    val offset = metadata.offset();
                    val partition = metadata.partition();
                    val timestamp = metadata.timestamp();
                    val topic = metadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                });
            }
        }
    }

    /**
     * 以不同 Key 發送訊息。
     * <p>
     * 每筆訊息使用不同的 Key（{@code "staticKey0"}, {@code "staticKey1"}, ...），
     * DefaultPartitioner 會對各 Key 進行雜湊，不同 Key 的雜湊值可能落在不同 Partition。
     * 可觀察 log 中的 partition 欄位，確認訊息是否分散至多個 Partition。
     * </p>
     */
    @Test
    public void defaultPartitionerProduceWithDifferentKey() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                val key = STATIC_KEY + i;
                // 印出 key 與其 hashCode，方便驗證 partition 分配邏輯
                log.info("key: {} , hash: {}", key, key.hashCode());
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, key, VALUE_PREFIX + i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send message error", exception);
                        return;
                    }
                    val offset = metadata.offset();
                    val partition = metadata.partition();
                    val timestamp = metadata.timestamp();
                    val topic = metadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                });
            }
        }
    }

    /**
     * 使用自訂 Partitioner 發送訊息。
     * <p>
     * 透過 {@code ProducerConfig.PARTITIONER_CLASS_CONFIG} 掛載 {@link ZeroOnlyPartitioner}，
     * 使所有訊息固定路由至 Partition 0，覆蓋預設的分區策略。
     * </p>
     * <p>
     * <b>實務應用：</b>自訂 Partitioner 可用於實現地理路由、優先級佇列等進階分區需求。
     * </p>
     *
     * @see ZeroOnlyPartitioner
     */
    @Test
    public void customPartitionerProduce() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 指定自訂 Partitioner — 所有訊息一律寫入 Partition 0
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ZeroOnlyPartitioner.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send message error", exception);
                        return;
                    }
                    val offset = metadata.offset();
                    val partition = metadata.partition();
                    val timestamp = metadata.timestamp();
                    val topic = metadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                });
            }
        }
    }

}
