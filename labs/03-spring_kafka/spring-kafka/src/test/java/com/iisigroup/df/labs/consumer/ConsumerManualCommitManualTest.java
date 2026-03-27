package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@MySpringBootTest
public class ConsumerManualCommitManualTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // 非及時 offset commit (先與 MessageListenerContainer 通知可提交 , MessageListenerContainer 再進行 offset commit)
        // 只針對整批進行
        // 監聽器不論單筆或批次處理 -> 調用 acknowledgment.acknowledge() 進行提交
        registry.add("spring_kafka_listener_ack_mode", () -> ContainerProperties.AckMode.MANUAL);

        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(60);
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerManualCommitManualTest.listen1")
    public void listen1(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
        System.out.println("Received message: " + message);
        acknowledgment.acknowledge();
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "ConsumerManualCommitManualTest.listen3")
    public void listen3(List<ConsumerRecord<String, String>> message, Acknowledgment acknowledgment) {
        System.out.println("Received message: " + message);
        acknowledgment.acknowledge();
    }

}
