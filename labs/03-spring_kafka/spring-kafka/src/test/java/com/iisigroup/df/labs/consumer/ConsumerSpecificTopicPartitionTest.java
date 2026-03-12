package com.iisigroup.df.labs.consumer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@Slf4j
@MySpringBootTest
public class ConsumerSpecificTopicPartitionTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void awaitForTestConsumer() throws InterruptedException {
        TimeUnit.MINUTES.sleep(5);
    }

    // 一次處理一筆
    @KafkaListener(groupId = "ConsumerSpecificTopicPartitionTest", topicPartitions = {@TopicPartition(topic = TEST_TOPIC, partitions = "0,1,6")})
    public void listen0(String message) {
        System.out.println("Received message: " + message);
    }

}
