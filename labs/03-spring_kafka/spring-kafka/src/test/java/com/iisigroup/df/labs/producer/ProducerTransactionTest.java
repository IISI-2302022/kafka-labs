package com.iisigroup.df.labs.producer;

import com.iisigroup.df.labs.config.MySpringBootTest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.support.TestPropertySourceUtils;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;


@Slf4j
@Import(KafkaTxService.class)
@MySpringBootTest
@ContextConfiguration(initializers = ProducerTransactionTest.Initializer.class)
public class ProducerTransactionTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaTxService kafkaTxService;

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Test
    public void sendWithTransactional() {
        kafkaTxService.send();
    }

    @Test
    public void sendWithTransactionalThrows() {
        Assertions.assertThrows(
                RuntimeException.class,
                () -> kafkaTxService.sendAndThrows()
        );
    }

    @Test
    public void sendWithTransactional1() {
        kafkaTemplate.executeInTransaction((operations) -> {
            for (int i = 0; i < 5; i++) {
                operations.send(new ProducerRecord<>(TEST_TOPIC, "haha" + i));
            }
            return true;
        });
    }

    @Test
    public void sendWithTransactional1Throws() {
        Assertions.assertThrows(RuntimeException.class,
                () ->
                        kafkaTemplate.executeInTransaction((operations) -> {
                            for (int i = 0; i < 5; i++) {
                                operations.send(new ProducerRecord<>(TEST_TOPIC, "haha" + i));
                            }
                            throw new RuntimeException("test exception");
                        })
        );
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            val environment = applicationContext.getEnvironment();

            val originalValue = environment.getProperty("spring.application.name");

            val dynamicValue = originalValue + "-";

            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext, "spring.kafka.producer.transaction-id-prefix=" + dynamicValue
            );
        }
    }


}
