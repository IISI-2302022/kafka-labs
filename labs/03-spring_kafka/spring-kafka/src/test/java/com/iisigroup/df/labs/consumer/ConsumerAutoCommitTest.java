package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@MySpringBootTest
public class ConsumerAutoCommitTest {

    public static final String TEST_TOPIC = "abc";

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
    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerAutoCommitTest.listen2")
    public void listen2(ConsumerRecord<String, String> message) {
        System.out.println("Received message: " + message);
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "ConsumerAutoCommitTest.listen4")
    public void listen4(List<ConsumerRecord<String, String>> message) {
        System.out.println("Received message: " + message);
    }


}
