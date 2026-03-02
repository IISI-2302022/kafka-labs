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
 * Kafka Consumer Offset 提交策略示範。
 * <p>
 * Offset 是 Consumer 追蹤消費進度的核心機制。Kafka 將每個 Consumer Group
 * 對每個 Partition 的已消費 Offset 儲存在內部 Topic {@code __consumer_offsets} 中。
 * </p>
 *
 * <h3>Offset 提交方式</h3>
 * <ul>
 *     <li><b>自動提交（Auto Commit）</b>— 由 Consumer 背景定期提交，簡單但可能導致重複消費或資料遺失</li>
 *     <li><b>手動同步提交（Sync Commit）</b>— 阻塞等待 Broker 確認，保證 Offset 已提交成功</li>
 *     <li><b>手動非同步提交（Async Commit）</b>— 非阻塞，吞吐量較高，但失敗時不會自動重試</li>
 * </ul>
 *
 * <h3>提交粒度</h3>
 * <ul>
 *     <li><b>Batch 級別</b>— 處理完一整批 {@code poll()} 回傳的訊息後，一次提交</li>
 *     <li><b>Record 級別</b>— 每處理完一筆訊息就提交，精確但效能較低</li>
 * </ul>
 *
 * <p>
 * <b>注意：</b>提交的 Offset 值應為「下一筆要消費的 Offset」，即 {@code consumerRecord.offset() + 1}。
 * </p>
 *
 * @see org.apache.kafka.clients.consumer.KafkaConsumer#commitSync()
 * @see org.apache.kafka.clients.consumer.KafkaConsumer#commitAsync()
 */
@Slf4j
public class ConsumerCommitOffsetTest {

    /**
     * 自動提交 Offset（Auto Commit）。
     * <p>
     * 設定 {@code enable.auto.commit=true}（預設值）與 {@code auto.commit.interval.ms=1000}，
     * Consumer 會在每次 {@code poll()} 時檢查是否已到達自動提交間隔，若是則自動提交。
     * </p>
     * <p>
     * <b>風險：</b>
     * <ul>
     *     <li>若訊息已被 poll 但尚未處理完就自動提交 → Consumer 重啟後會遺失這些訊息</li>
     *     <li>若處理完但尚未到達提交間隔 → Consumer 重啟後會重複消費</li>
     * </ul>
     * </p>
     */
    @Test
    public void autoCommit() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_auto_commit");

        // 開啟自動提交（預設即為 true）
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自動提交間隔：每 1000ms 提交一次（預設為 5000ms）
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

    /**
     * 手動同步提交 — Batch 級別（Sync Commit for Batch）。
     * <p>
     * 關閉自動提交後，在每次 {@code poll()} 迴圈結束時呼叫 {@code commitSync()}。
     * 此方法會阻塞直到 Broker 確認 Offset 提交成功，保證 at-least-once 語義。
     * </p>
     * <p>
     * {@code commitSync()} 不帶參數時會提交本次 {@code poll()} 回傳的所有 Partition 的最新 Offset。
     * </p>
     */
    @Test
    public void manuallySyncCommitForBatch() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_sync_batch");

        // 關閉自動提交，改為手動控制
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
                // 同步提交：阻塞等待 Broker 確認，確保 Offset 已持久化
                kafkaConsumer.commitSync();
            }
        }
    }


    /**
     * 手動同步提交 — Record 級別（Sync Commit for Record）。
     * <p>
     * 每處理完一筆 {@link ConsumerRecord} 後，精確提交該筆紀錄的 Offset。
     * 使用帶參數的 {@code commitSync(Map)} 指定要提交的 Partition 與 Offset。
     * </p>
     * <p>
     * <b>注意：</b>提交的 Offset 值為 {@code consumerRecord.offset() + 1}，
     * 代表「下一筆要消費的位置」。這是 Kafka Offset 語義的慣例。
     * </p>
     * <p>
     * <b>效能考量：</b>每筆都提交會大幅增加對 Broker 的請求數，
     * 建議僅在精確度要求極高的場景使用。
     * </p>
     */
    @Test
    public void manuallySyncCommitForRecord() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_sync_record");

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());

                    // 建構要提交的 TopicPartition 與 OffsetAndMetadata
                    val partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                    val offsetMeta = new OffsetAndMetadata(consumerRecord.offset() + 1); // +1 = 下一筆要消費的 offset
                    val commitMap = Collections.singletonMap(partition, offsetMeta);

                    // 精確提交單筆紀錄的 offset
                    kafkaConsumer.commitSync(commitMap);
                }
            }
        }
    }

    /**
     * 手動非同步提交 — Batch 級別（Async Commit for Batch）。
     * <p>
     * 呼叫 {@code commitAsync()} 不會阻塞，提交請求在背景發送。
     * 吞吐量高於 {@code commitSync()}，但若提交失敗不會自動重試
     * （因後續的 commitAsync 可能已提交更新的 offset，重試舊的會造成 offset 倒退）。
     * </p>
     * <p>
     * <b>最佳實務：</b>日常消費用 {@code commitAsync()}，Consumer 關閉前用 {@code commitSync()} 做最後確認。
     * </p>
     */
    @Test
    public void manuallyAsyncCommitForBatch() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_async_batch");

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
                // 非同步提交：不阻塞，背景發送提交請求
                kafkaConsumer.commitAsync();
            }
        }
    }


    /**
     * 手動非同步提交 — Record 級別（Async Commit for Record）+ Callback。
     * <p>
     * 結合 Record 級別的精確提交與 Async 的非阻塞特性。
     * 透過 {@link OffsetCommitCallback} 回呼函式得知每次提交的成功或失敗。
     * </p>
     * <p>
     * <b>注意：</b>Callback 內不應做複雜的錯誤恢復（如重試提交），
     * 因為更新的 offset 可能已經被後續的 commitAsync 提交。
     * </p>
     */
    @Test
    public void manuallyAsyncCommitForRecord() {
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_manual_commit_async_record");

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (val kafkaConsumer = new KafkaConsumer<String, String>(properties)) {
            val topics = new ArrayList<String>();
            topics.add(TEST_TOPIC);
            kafkaConsumer.subscribe(topics);
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("offset: {}, partition: {}, key: {}, value: {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                    val partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                    val offsetMeta = new OffsetAndMetadata(consumerRecord.offset() + 1);
                    val commitMap = Collections.singletonMap(partition, offsetMeta);
                    // 非同步提交 + Callback：得知提交結果
                    kafkaConsumer.commitAsync(commitMap, (metadataMap, e) -> {
                        if (e == null) {
                            log.info("commit success for offset: {}, partition: {}", consumerRecord.offset(), consumerRecord.partition());
                        } else {
                            log.error("commit failed for offset: {}, partition: {}", consumerRecord.offset(), consumerRecord.partition(), e);
                        }
                    });
                }
            }
        }
    }


}