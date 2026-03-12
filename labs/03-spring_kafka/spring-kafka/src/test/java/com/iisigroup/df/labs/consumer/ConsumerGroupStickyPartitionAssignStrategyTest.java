package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@MySpringBootTest
public class ConsumerGroupStickyPartitionAssignStrategyTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.consumer.properties.partition.assignment.strategy", CooperativeStickyAssignor.class::getName);
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(60);
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "ConsumerGroupStickyPartitionAssignStrategyTest")
    public void listen0(ConsumerRecord<String, String> record) {
        System.out.println("Received message: " + record);
    }

}
