package com.iisigroup.df.labs.admin;

import com.iisigroup.df.labs.config.MySpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Import(AdminConfig.class)
@MySpringBootTest
public class AdminTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_admin_auto_create", () -> true);
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:29092");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void testAutoCreateTopic() throws InterruptedException, ExecutionException, TimeoutException {
        TimeUnit.SECONDS.sleep(5);
    }

}
