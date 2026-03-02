package com.iisigroup.df.labs.producer;

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
 * Kafka Producer 進階配置示範。
 * <p>
 * 本測試類別逐步展示如何透過 Producer 參數調校來達成不同目標：
 * <ol>
 *     <li><b>提升吞吐量</b>（batch size、linger.ms、buffer memory、壓縮）</li>
 *     <li><b>提升資料可靠性</b>（acks=all、retries）</li>
 *     <li><b>訊息去重（Idempotent Producer）</b>（enable.idempotence=true）</li>
 *     <li><b>訊息有序性</b>（max.in.flight.requests.per.connection 搭配冪等性）</li>
 * </ol>
 * </p>
 *
 * @see org.apache.kafka.clients.producer.ProducerConfig
 */
@Slf4j
public class OtherProducerTest {

    /**
     * 提升吞吐量（Increase Throughput）。
     * <p>
     * 透過以下參數組合提升 Producer 的發送效率：
     * <ul>
     *     <li>{@code batch.size=16384} — 每個 batch 最大 16 KB，累積多筆訊息後一次發送，減少網路往返</li>
     *     <li>{@code linger.ms=50} — 即使 batch 未滿，最多等待 50ms 後強制發送，在延遲與吞吐量之間取得平衡</li>
     *     <li>{@code buffer.memory=67108864} — Producer 端緩衝區大小為 64 MB，可容納更多待發送訊息</li>
     *     <li>{@code compression.type=zstd} — 使用 Zstandard 壓縮演算法，有效降低網路傳輸量與 Broker 儲存空間</li>
     * </ul>
     * </p>
     */
    @Test
    public void increaseThroughput() {
        val properties = new Properties();

        // ---- 吞吐量調校參數 ----
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);          // batch 大小（bytes），預設 16384
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);              // 等待湊滿 batch 的最大時間（ms）

        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);    // 緩衝區總大小（bytes），預設 32 MB，此處設為 64 MB

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");   // 壓縮類型：none / gzip / snappy / lz4 / zstd

        // ---- 基本連線與序列化設定 ----
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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

    /**
     * 提升資料可靠性（Improve Data Reliability）。
     * <p>
     * 在吞吐量調校的基礎上，加入可靠性相關參數：
     * <ul>
     *     <li>{@code acks=all}（等同 acks=-1）— 需所有 ISR（In-Sync Replicas）副本確認寫入後才視為成功，
     *         提供最高等級的持久性保證</li>
     *     <li>{@code retries=3} — 發送失敗時自動重試 3 次（搭配預設 retry.backoff.ms=100）</li>
     * </ul>
     * </p>
     * <p>
     * <b>注意：</b>acks=all 會增加延遲，但可避免在 Leader 故障時遺失已確認的訊息。
     * </p>
     */
    @Test
    public void improveDataReliability() {
        val properties = new Properties();

        // ---- 吞吐量調校參數（同上）----
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);

        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // ---- 可靠性參數 ----
        properties.put(ProducerConfig.ACKS_CONFIG, "all");   // 等待所有 ISR 副本確認
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);    // 失敗重試次數

        // ---- 基本連線與序列化設定 ----
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }

    /**
     * 訊息去重 — 冪等性 Producer（Idempotent Producer）。
     * <p>
     * 開啟 {@code enable.idempotence=true} 後，Producer 會為每批訊息附帶
     * Producer ID（PID）與 Sequence Number，Broker 端會自動偵測並丟棄重複訊息。
     * </p>
     * <p>
     * 冪等性的前提條件（Kafka 自動強制設定）：
     * <ul>
     *     <li>{@code acks} 必須為 {@code all}</li>
     *     <li>{@code retries} 必須大於 0</li>
     *     <li>{@code max.in.flight.requests.per.connection} 必須 ≤ 5</li>
     * </ul>
     * </p>
     * <p>
     * <b>注意：</b>冪等性保證的範圍是「單一 Producer Session 內、單一 Partition」，
     * 跨 Session 或跨 Partition 的去重需搭配 Kafka Transactions。
     * </p>
     */
    @Test
    public void produceDeduplicatedMessages() {
        val properties = new Properties();

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);

        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 開啟冪等性：Broker 會依據 PID + Sequence Number 自動去重
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }

    /**
     * 訊息有序性保證（Ordered Messages）。
     * <p>
     * 在開啟冪等性的前提下，{@code max.in.flight.requests.per.connection} 最多可設為 5，
     * Kafka 會利用 Sequence Number 在 Broker 端對亂序的 batch 重新排序，
     * 確保同一 Partition 內的訊息順序與發送順序一致。
     * </p>
     *
     * <h3>兩種有序性策略比較</h3>
     * <table border="1">
     *     <tr><th>方案</th><th>冪等性</th><th>max.in.flight</th><th>吞吐量</th></tr>
     *     <tr><td>方案 A（推薦）</td><td>true</td><td>≤ 5</td><td>較高</td></tr>
     *     <tr><td>方案 B</td><td>false</td><td>1</td><td>較低（每次只能有 1 個 in-flight request）</td></tr>
     * </table>
     */
    @Test
    public void produceOrderedMessages() {
        val properties = new Properties();

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);

        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 方案 A（推薦）：冪等性 + max.in.flight ≤ 5，Broker 端會自動排序
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // todo 最大尚未 ack 的 request 數量
        //  每個 request 最多只包含一個 topic + partition batch 資料
        //  如果開啟冪等性 , broker 會使用 batch 的 sequence number 進行排序 (只接受最多 5 個 batch , 超過不保證順序)
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 方案 B（替代）：關閉冪等性，限制 in-flight 為 1，以犧牲吞吐量換取順序保證
        //        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        //        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }


}
