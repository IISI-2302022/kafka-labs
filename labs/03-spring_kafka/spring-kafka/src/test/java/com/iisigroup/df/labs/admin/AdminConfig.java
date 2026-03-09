package com.iisigroup.df.labs.admin;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import static com.iisigroup.df.labs.consumer.ConsumerAutoCommitTest.TEST_TOPIC;

@TestConfiguration
public class AdminConfig {

    @Bean
    public NewTopic singleTopic() {
        return TopicBuilder.name(TEST_TOPIC)
                .partitions(7)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaAdmin.NewTopics multipleTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("defaultBoth")
                        .build(),
                TopicBuilder.name("defaultPart")
                        .partitions(4)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("defaultRepl")
                        .compact()
                        .partitions(3)
                        .build()
        );
    }
}