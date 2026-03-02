package com.iisigroup.df.labs.producer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;


@Slf4j
public class OtherProducerTest {

    @Test
    public void increaseThroughput() {
        val properties = new Properties();
        // 16K 這個沒有變化
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 64M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send message error", exception);
                        return;
                    }
                    val offset = metadata.offset();
                    val partition = metadata.partition();
                    val timestamp = metadata.timestamp();
                    val topic = metadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                });
            }
        }
    }


    @Test
    public void improveDataReliability() {
        val properties = new Properties();
        // 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 64M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // acks -1 or all
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 不無止盡 retry
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }

    @Test
    public void produceDeduplicatedMessages() {
        val properties = new Properties();
        // 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 64M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // acks -1 or all
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 不無止盡 retry
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 開啟冪等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }

    @Test
    public void produceOrderedMessages() {
        val properties = new Properties();
        // 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 64M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // acks -1 or all
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 不無止盡 retry
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 開啟冪等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 前面 5 個請求若 失敗重試 或 未接收到 ack , 則不再發送後續請求
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        // 如果沒有開啟冪等性 , 則只能設 1
//        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }
    }


}
