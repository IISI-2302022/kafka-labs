package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@MySpringBootTest
@Import(ConsumerConfig.class)
public class ConsumerErrorHandleTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // batch ack mode & auto commit false
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(60);
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerErrorHandleTest.listen0", errorHandler = "myErrorHandler")
    public void listen0(ConsumerRecord<String, String> message) {
        System.out.println("Received message single1: " + message);
        throw new RuntimeException("ggg");
    }

    @KafkaListener(topics = TEST_TOPIC, batch = "true", groupId = "ConsumerErrorHandleTest.listen1", errorHandler = "myErrorHandler")
    public void listen1(List<ConsumerRecord<String, String>> message) {
        System.out.println("Received message batch1: " + message);
        throw new RuntimeException("ggg");
    }

}
