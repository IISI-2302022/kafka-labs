package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import com.iisigroup.df.labs.partitioner.ZeroOnlyPartitioner;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@Slf4j
@MySpringBootTest
public class CustomProducerPartitionerTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
        registry.add("spring.kafka.producer.properties.partitioner.class", ZeroOnlyPartitioner.class::getName);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void sendSync() throws InterruptedException {
        val limit = 5;
        val countDownLatch = new CountDownLatch(limit);

        for (int i = 0; i < limit; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, i, null, "haha" + i));
            // 不管成功失敗都會進入 callback func
            future.whenComplete((sendResult, throwable) -> {
                countDownLatch.countDown();
                if (throwable != null) {
                    // 發送失敗
                    log.error("send message error", throwable);
                    return;
                }
                // kafka 回傳之結果資訊
                val recordMetadata = sendResult.getRecordMetadata();
                log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
            });
        }

        countDownLatch.await(10, TimeUnit.SECONDS);
    }
}
