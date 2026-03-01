package com.iisigroup.df.labs.producer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;

@Slf4j
public class SimpleProduceTest {
    @Test
    public void asyncProduce() {
        val properties = new Properties();

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
    public void asyncProduceWithCallback() {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(
                        new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)
                        , (metadata, exception) -> {
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
    public void syncProduce() throws ExecutionException, InterruptedException {
        val properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)).get();
            }
        }
    }

}
