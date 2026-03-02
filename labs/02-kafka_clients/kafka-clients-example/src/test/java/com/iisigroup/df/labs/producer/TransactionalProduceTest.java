package com.iisigroup.df.labs.producer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;

@Slf4j
public class TransactionalProduceTest {

    @Test
    public void transactionSuccess() {
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
        // transaction id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            kafkaProducer.initTransactions();
            try {
                kafkaProducer.beginTransaction();

                val futureList = new ArrayList<Future<RecordMetadata>>();
                for (int i = 0; i < 5; i++) {
                    futureList.add(kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)));
                }

                for (Future<RecordMetadata> recordMetadataFuture : futureList) {
                    val recordMetadata = recordMetadataFuture.get();
                    val offset = recordMetadata.offset();
                    val partition = recordMetadata.partition();
                    val timestamp = recordMetadata.timestamp();
                    val topic = recordMetadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                }

                kafkaProducer.commitTransaction();
            } catch (Exception e) {
                kafkaProducer.abortTransaction();
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void transactionRollback() {
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
        // transaction id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            kafkaProducer.initTransactions();
            try {
                kafkaProducer.beginTransaction();

                val futureList = new ArrayList<Future<RecordMetadata>>();
                for (int i = 0; i < 5; i++) {
                    futureList.add(kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)));
                }

                for (Future<RecordMetadata> recordMetadataFuture : futureList) {
                    val recordMetadata = recordMetadataFuture.get();
                    val offset = recordMetadata.offset();
                    val partition = recordMetadata.partition();
                    val timestamp = recordMetadata.timestamp();
                    val topic = recordMetadata.topic();
                    log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                }

                if (true) {
                    throw new RuntimeException("test rollback");
                }

                kafkaProducer.commitTransaction();
            } catch (Exception e) {
                kafkaProducer.abortTransaction();
                throw new RuntimeException(e);
            }
        }
    }

}
