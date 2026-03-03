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

/**
 * Kafka Transactional Consumer 示範。
 * <p>
 * 搭配 Transactional Producer 使用，透過設定 Consumer 的隔離級別（Isolation Level）
 * 來控制是否只讀取已提交的交易訊息。
 * </p>
 *
 * <h3>隔離級別（isolation.level）</h3>
 * <ul>
 *     <li><b>{@code read_uncommitted}（預設）</b>— 讀取所有訊息，包含尚未提交或已中止的交易訊息</li>
 *     <li><b>{@code read_committed}</b>— 只讀取已成功提交的交易訊息，
 *         被 {@code abortTransaction()} 中止的訊息會被自動過濾</li>
 * </ul>
 *
 * <p>
 * <b>注意：</b>在 {@code read_committed} 模式下，Consumer 的讀取位置會受到
 * LSO（Last Stable Offset）的限制。LSO 是尚未結束的最早交易的起始 Offset，
 * Consumer 不會讀取超過 LSO 的訊息，以避免讀到未完成交易的資料。
 * 長時間未結束的交易會導致 Consumer 的消費延遲增加。
 * </p>
 *
 * @see com.iisigroup.df.labs.producer.TransactionalProduceTest
 */
@Slf4j
public class TransactionalConsumeTest {

    /**
     * 以 {@code read_committed} 隔離級別消費交易訊息。
     * <p>
     * 設定 {@code isolation.level=read_committed} 後，只有已提交（committed）的交易訊息
     * 會被此 Consumer 讀取。已中止（aborted）的交易訊息會被自動跳過。
     * </p>
     * <p>
     * 搭配 {@link com.iisigroup.df.labs.producer.TransactionalProduceTest#transactionRollback()}
     * 測試，可觀察到被回滾的訊息不會出現在此 Consumer 的輸出中。
     * </p>
     */
    @Test
    public void consumeForTransaction() {
        val properties = new Properties();
        // Kafka 叢集的連線位址（host:port），Consumer 會透過這個位址找到整個叢集
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 訊息的 value 要用什麼方式從 byte[] 轉回物件，這裡用字串反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_read_committed");

        // 設定隔離級別為 read_committed：只讀取 Producer 已經 commitTransaction() 的訊息
        // 被 abortTransaction() 回滾的訊息會被自動跳過，不會被這個 Consumer 讀到
        // 預設值是 read_uncommitted（什麼訊息都讀，包含還沒提交或已回滾的）
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
