package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.consumer.TransactionalConsumeTest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;

/**
 * Kafka Transactional Producer 示範。
 * <p>
 * Kafka 交易（Transaction）提供跨 Partition 的原子性寫入保證：
 * 一個交易內的所有訊息要嘛全部被消費者可見，要嘛全部不可見。
 * </p>
 *
 * <h3>交易使用前提</h3>
 * <ul>
 *     <li>必須設定 {@code transactional.id}（唯一識別此 Producer 實例）</li>
 *     <li>冪等性（idempotence）會被自動開啟</li>
 *     <li>{@code acks} 會被自動設為 {@code all}</li>
 * </ul>
 *
 * <h3>交易 API 流程</h3>
 * <pre>{@code
 * producer.initTransactions();       // 1. 初始化交易（僅需呼叫一次）
 * producer.beginTransaction();       // 2. 開始交易
 * producer.send(...);                // 3. 發送訊息（可多次）
 * producer.commitTransaction();      // 4a. 提交交易（成功）
 * producer.abortTransaction();       // 4b. 中止交易（失敗時回滾）
 * }</pre>
 *
 * <p>
 * 搭配 Consumer 端設定 {@code isolation.level=read_committed}，
 * 即可確保消費者只讀取到已提交的交易訊息。
 * </p>
 *
 * @see TransactionalConsumeTest
 */
@Slf4j
public class TransactionalProduceTest {

    /**
     * 交易成功提交（Transaction Commit）。
     * <p>
     * 所有訊息在 {@code commitTransaction()} 後才會對 {@code read_committed} 的 Consumer 可見。
     * 若在 {@code beginTransaction()} 與 {@code commitTransaction()} 之間未發生例外，
     * 交易會正常提交。
     * </p>
     */
    @Test
    public void transactionSuccess() {
        val properties = new Properties();

        // ---- 吞吐量 & 壓縮 ----
        // 每個 batch 最大 16 KB，把多筆訊息打包一起送，減少網路來回次數
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 就算 batch 還沒裝滿，最多等 50 毫秒就強制送出
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // Producer 本地端的緩衝區大小，設為 64 MB（預設 32 MB），能暫存更多還沒送出的訊息
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        // 壓縮演算法，zstd 壓縮率好且速度快，能減少傳輸資料量
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // ---- 可靠性 ----
        // acks=all 表示要等所有副本（ISR）都確認收到訊息才算成功，最安全但延遲較高
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 發送失敗時自動重試 3 次，避免因暫時性網路問題就丟訊息
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // ---- 冪等性（交易模式會自動開啟冪等性，此處明確設定以便閱讀）----
        // 開啟冪等性：Broker 會依據 PID + Sequence Number 自動去重
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // ---- 交易 ID ----
        // 每個交易 Producer 都必須設定唯一的 transactional.id，用來讓 Broker 識別這個 Producer 的交易範圍
        // 如果 Producer 重啟但使用相同的 transactional.id，Broker 會自動回滾該 ID 上一次未完成的交易
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");

        // ---- 基本連線與序列化 ----
        // Kafka 叢集的連線位址（host:port），Producer 會透過這個位址找到整個叢集
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 訊息的 value 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            // 初始化交易狀態（向 Transaction Coordinator 註冊）
            kafkaProducer.initTransactions();
            try {
                // 開始一個新的交易
                kafkaProducer.beginTransaction();

                // 收集所有 send() 回傳的 Future，稍後可驗證每筆訊息的 metadata
                val futureList = new ArrayList<Future<RecordMetadata>>();
                for (int i = 0; i < 5; i++) {
                    futureList.add(kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)));
                }

                // 逐一取得發送結果（此時訊息已寫入 Broker，但交易尚未提交）
                for (Future<RecordMetadata> recordMetadataFuture : futureList) {
                    val recordMetadata = recordMetadataFuture.get();
                    val offset = recordMetadata.offset();
                    val partition = recordMetadata.partition();
                    val timestamp = recordMetadata.timestamp();
                    val topic = recordMetadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                }

                // 提交交易：所有訊息對 read_committed 的 Consumer 變為可見
                kafkaProducer.commitTransaction();
            } catch (Exception e) {
                // 發生任何例外時中止交易：已寫入的訊息會被標記為 aborted，Consumer 不會消費到
                kafkaProducer.abortTransaction();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 交易回滾（Transaction Rollback）。
     * <p>
     * 在 {@code commitTransaction()} 前故意拋出例外，觸發 {@code abortTransaction()}。
     * 已經發送但尚未提交的訊息會被標記為中止（aborted），
     * 使用 {@code read_committed} 隔離級別的 Consumer 不會消費到這些訊息。
     * </p>
     * <p>
     * <b>注意：</b>使用 {@code read_uncommitted}（預設）的 Consumer 仍然可以讀到被中止的訊息。
     * </p>
     */
    @Test
    public void transactionRollback() {
        val properties = new Properties();

        // 每個 batch 最大 16 KB，把多筆訊息打包一起送，減少網路來回次數
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 就算 batch 還沒裝滿，最多等 50 毫秒就強制送出
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // Producer 本地端的緩衝區大小，設為 64 MB（預設 32 MB），能暫存更多還沒送出的訊息
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        // 壓縮演算法，zstd 壓縮率好且速度快，能減少傳輸資料量
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // acks=all 表示要等所有副本（ISR）都確認收到訊息才算成功，最安全但延遲較高
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 發送失敗時自動重試 3 次，避免因暫時性網路問題就丟訊息
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 開啟冪等性：Broker 會依據 PID + Sequence Number 自動去重
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 使用不同的 transactional.id，每個交易 Producer 實例都要有自己唯一的 ID，避免跟其他 Producer 衝突
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_1");

        // Kafka 叢集的連線位址（host:port），Producer 會透過這個位址找到整個叢集
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 訊息的 value 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            kafkaProducer.initTransactions();
            try {
                kafkaProducer.beginTransaction();

                val futureList = new ArrayList<Future<RecordMetadata>>();
                for (int i = 0; i < 5; i++) {
                    futureList.add(kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)));
                }

                for (Future<RecordMetadata> recordMetadataFuture : futureList) {
                    val recordMetadata = recordMetadataFuture.get();
                    val offset = recordMetadata.offset();
                    val partition = recordMetadata.partition();
                    val timestamp = recordMetadata.timestamp();
                    val topic = recordMetadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                }

                // 模擬業務邏輯錯誤，強制觸發回滾
                if (true) {
                    throw new RuntimeException("test rollback");
                }

                kafkaProducer.commitTransaction();
            } catch (Exception e) {
                // 中止交易：所有未提交的訊息對 read_committed Consumer 不可見
                kafkaProducer.abortTransaction();
                throw new RuntimeException(e);
            }
        }
    }

}
