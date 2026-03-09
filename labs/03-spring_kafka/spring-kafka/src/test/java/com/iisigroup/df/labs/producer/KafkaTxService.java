package com.iisigroup.df.labs.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@RequiredArgsConstructor
@Transactional
public class KafkaTxService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send() {
        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send(new ProducerRecord<>(TEST_TOPIC, "haha" + i));
        }
    }

    public void sendAndThrows() {
        send();
        throw new RuntimeException("test exception");
    }
}