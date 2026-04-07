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

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@Slf4j
@MySpringBootTest
public class SingleConsumerTest {
    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(60);
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "SingleConsumerTest.listen0")
    public void listen0(ConsumerRecord<String, String> record) {
        System.out.println("Received message: " + record);
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "SingleConsumerTest.listen1")
    public void listen1(List<ConsumerRecord<String, String>> records) {
        System.out.println("Received message: " + records);
    }

}
