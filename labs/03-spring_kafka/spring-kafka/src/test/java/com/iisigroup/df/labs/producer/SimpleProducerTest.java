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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@Slf4j

@MySpringBootTest
public class SimpleProducerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void sendSync() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 5; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, "haha" + i));
            // 同步發送 , 建議採 timeout 方式 , 時間請自行斟酌
            // 發送失敗會拋例外 , 如要處理 , 採用 try cache 方式
            val sendResult = future.get(5, TimeUnit.SECONDS);
            // kafka 回傳之結果資訊
            val recordMetadata = sendResult.getRecordMetadata();
            log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        }
    }

    @Test
    public void sendAsyncWithCallback() throws InterruptedException {
        val limit = 5;
        val countDownLatch = new CountDownLatch(limit);

        for (int i = 0; i < limit; i++) {
            val future = kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, "haha" + i));
            // 不管成功失敗都會進入 callback func
            future.whenComplete((sendResult, throwable) -> {
                try {
                    if (throwable != null) {
                        // 發送失敗
                        log.error("send message error", throwable);
                        return;
                    }
                    // kafka 回傳之結果資訊
                    val recordMetadata = sendResult.getRecordMetadata();
                    log.info("topic: {}, partition: {}, offset: {}, timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await(10, TimeUnit.SECONDS);

    }


}
