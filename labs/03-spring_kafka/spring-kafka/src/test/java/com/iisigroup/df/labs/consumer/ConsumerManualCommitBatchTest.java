package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@MySpringBootTest
public class ConsumerManualCommitBatchTest {
    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // 目前預設是 batch 且 auto commit false
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(5);
    }

    // 一次處理一筆
    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerManualCommitBatchTest.listen2")
    public void listen2(ConsumerRecord<String, String> message) {
        System.out.println("Received message: " + message);
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "ConsumerManualCommitBatchTest.listen4")
    public void listen4(List<ConsumerRecord<String, String>> message) {
        System.out.println("Received message: " + message);
    }
}
