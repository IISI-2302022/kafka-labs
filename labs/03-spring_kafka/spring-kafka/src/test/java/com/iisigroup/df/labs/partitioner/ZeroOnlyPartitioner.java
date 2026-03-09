package com.iisigroup.df.labs.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自訂分區器（Custom Partitioner）— 固定將所有訊息路由至 Partition 0。
 * <p>
 * 實作 {@link Partitioner} 介面，覆寫分區邏輯，使得無論 key / value 為何，
 * 所有訊息一律被寫入 Partition 0。此分區器主要用於教學示範，
 * 說明如何透過 {@code ProducerConfig.PARTITIONER_CLASS_CONFIG} 掛載自訂分區策略。
 * </p>
 *
 * <h3>使用方式</h3>
 * <pre>{@code
 * properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ZeroOnlyPartitioner.class.getName());
 * }</pre>
 *
 * @see Partitioner
 */
@Slf4j
public class ZeroOnlyPartitioner implements Partitioner {

    /**
     * 計算訊息應寫入的分區編號。
     * <p>
     * 本實作忽略所有參數，固定回傳 0，代表所有訊息都會被寫入 Partition 0。
     * </p>
     *
     * @param topic      目標 Topic 名稱
     * @param key        訊息的 Key（可為 null）
     * @param keyBytes   序列化後的 Key bytes
     * @param value      訊息的 Value
     * @param valueBytes 序列化後的 Value bytes
     * @param cluster    當前 Kafka 叢集的 metadata 資訊
     * @return 固定回傳 0（Partition 0）
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        log.info("Routing message to partition 0: topic={}, key={}, value={}", topic, key, value);
        return 0;
    }

    /** 關閉分區器時釋放資源（本實作無需額外處理） */
    @Override
    public void close() {

    }

    /** 初始化分區器設定（本實作無需額外設定） */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
