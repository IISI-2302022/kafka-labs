package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@Slf4j
@MySpringBootTest
public class ProducerPartitionerTest {

    public static final String STATIC_KEY = "staticKey";

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void defaultPartitionerProduceWithPartition() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 5; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, i, null, "haha" + i));
            val sendResult = future.get(5, TimeUnit.SECONDS);
            val recordMetadata = sendResult.getRecordMetadata();
            log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        }
    }


    @Test
    public void defaultPartitionerProduceWithSameKey() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 5; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, STATIC_KEY, "haha" + i));
            val sendResult = future.get(5, TimeUnit.SECONDS);
            val recordMetadata = sendResult.getRecordMetadata();
            log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        }
    }

    @Test
    public void defaultPartitionerProduceWithDifferentKey() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 5; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, STATIC_KEY + i, "haha" + i));
            val sendResult = future.get(5, TimeUnit.SECONDS);
            val recordMetadata = sendResult.getRecordMetadata();
            log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        }
    }


}
