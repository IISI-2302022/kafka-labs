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
 * Kafka Consumer 基本消費示範 — 單一消費者。
 * <p>
 * 本測試建立一個隸屬於消費者群組 {@code "test_single"} 的 Consumer，
 * 透過 {@code subscribe()} 訂閱 Topic，並在無限迴圈中持續 {@code poll()} 拉取訊息。
 * </p>
 *
 * <h3>Consumer 消費流程</h3>
 * <ol>
 *     <li>建立 {@link KafkaConsumer} 並設定基本連線、反序列化器、群組 ID</li>
 *     <li>呼叫 {@code subscribe(topics)} 訂閱一個或多個 Topic</li>
 *     <li>在迴圈中呼叫 {@code poll(timeout)} 拉取訊息</li>
 *     <li>處理拉取到的 {@link ConsumerRecord}（offset、partition、key、value）</li>
 * </ol>
 *
 * <p>
 * <b>注意：</b>若群組內只有一個 Consumer，該 Consumer 會被分配到所有 Partition。
 * </p>
 *
 * @see org.apache.kafka.clients.consumer.KafkaConsumer
 */
@Slf4j
public class SingleConsumerTest {

    /**
     * 單一消費者持續消費訊息。
     * <p>
     * Consumer 加入群組 {@code "test_single"} 後，Kafka 會觸發 Rebalance，
     * 將 Topic 的所有 Partition 分配給此唯一的 Consumer。
     * {@code poll(Duration.ofSeconds(1))} 最多阻塞 1 秒等待訊息，
     * 若有訊息則立即返回。
     * </p>
     */
    @Test
    public void singleConsumer() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消費者群組 ID：相同 group.id 的 Consumer 會共同分擔 Partition
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_single");

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
