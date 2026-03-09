package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@MySpringBootTest
@KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerMultiDataTypeTest")
public class ConsumerMultiDataTypeTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        // batch ack mode & auto commit false
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(5);
    }

    @KafkaHandler
    public void listen1(String message) {
        System.out.println("Received message: " + message);
    }

    @KafkaHandler(isDefault = true)
    public void listen2(Object message) {
        System.out.println("Received message: " + message);
    }

}
