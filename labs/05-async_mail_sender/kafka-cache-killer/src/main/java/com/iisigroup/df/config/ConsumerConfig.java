package com.iisigroup.df.config;

import lombok.val;
import org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ConsumerConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<?, ?> kafkaTemplate) {
        // dlq 機制, retry 後還依然失敗 or not retry exception
        val recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        // retry 機制
        val backOff = new FixedBackOff(50L, 3);

        val errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // todo 自行決定
        errorHandler.addNotRetryableExceptions(UnretryableException.class);

        errorHandler.setAckAfterHandle(true);
        errorHandler.setSeekAfterError(true);
        errorHandler.setCommitRecovered(true);
        errorHandler.setLogLevel(KafkaException.Level.ERROR);

        return errorHandler;
    }

}
