package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.partitioner.ZeroOnlyPartitioner;
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
public class ProducerPartitionerTest {

    public static final String STATIC_KEY = "staticKey";

    @Test
    public void defaultPartitionerProduceWithPartition() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 7; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, i, null, VALUE_PREFIX + i), (metadata, exception) -> {
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
    public void defaultPartitionerProduceWithSameKey() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, STATIC_KEY, VALUE_PREFIX + i), (metadata, exception) -> {
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
    public void defaultPartitionerProduceWithDifferentKey() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                val key = STATIC_KEY + i;
                log.info("key: {} , hash: {}", key, key.hashCode());
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, key, VALUE_PREFIX + i), (metadata, exception) -> {
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
    public void customPartitionerProduce() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ZeroOnlyPartitioner.class.getName());

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

}
