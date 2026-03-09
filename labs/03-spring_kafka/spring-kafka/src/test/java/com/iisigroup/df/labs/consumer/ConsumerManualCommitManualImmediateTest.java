package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@MySpringBootTest
public class ConsumerManualCommitManualImmediateTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // 及時 offset commit
        // 可針對 單筆 或 整批 進行
        // 監聽器單筆處理 -> 調用 acknowledgment.acknowledge() 進行單筆提交
        // 監聽器批次處理 -> 調用 acknowledgment.acknowledge(index) 進行單筆提交 , acknowledgment.acknowledge() 進行整批提交
        registry.add("spring_kafka_listener_ack_mode", () -> ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(60);
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerManualCommitManualImmediateTest.listen1")
    public void listen1(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
        System.out.println("Received message: " + message);
        acknowledgment.acknowledge();
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "ConsumerManualCommitManualImmediateTest.listen3")
    public void listen3(List<ConsumerRecord<String, String>> message, Acknowledgment acknowledgment) {
        System.out.println("Received message: " + message);
        for (int i = 0; i < message.size(); i++) {
            acknowledgment.acknowledge(i);
        }
    }

}
