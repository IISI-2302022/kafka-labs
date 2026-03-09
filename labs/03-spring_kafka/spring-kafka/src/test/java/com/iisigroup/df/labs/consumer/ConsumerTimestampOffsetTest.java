package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import lombok.val;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@MySpringBootTest
// todo ConsumerSeekAware 是 class 範疇 , 裡面的 listener 方法會使用相同 consumer seek aware
public class ConsumerTimestampOffsetTest implements ConsumerSeekAware {

    // todo 如果 rebalance 拿到相同的 topic + partition , 還是會從指定 offset 開始消費
    //  除非自己實作邏輯保證不重複
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        val oneDayAgoTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
        callback.seekToTimestamp(assignments.keySet(), oneDayAgoTimestamp);
    }

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // 自動提交 offset
        registry.add("spring_kafka_consumer_enable_auto_commit", () -> true);
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(5);
    }

    // 一次處理一筆
    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerSpecificOffsetTest.listen0")
    public void listen0(String message) {
        System.out.println("Received message: " + message);
    }

}
