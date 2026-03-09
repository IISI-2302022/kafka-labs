package com.iisigroup.df.labs.consumer;

import lombok.val;
import org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.util.backoff.FixedBackOff;

@TestConfiguration
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

    // 單筆資料進行轉換
    @Bean
    public RecordInterceptor<?, ?> recordInterceptor() {
        return (record, consumer) -> {
            System.out.println("recordInterceptor");
            return record;
        };
    }

    // 整批資料進行轉換
    @Bean
    public BatchInterceptor<?, ?> batchInterceptor() {
        return (records, consumer) -> {
            System.out.println("batchInterceptor");
            return records;
        };
    }

    // 每筆資料是否要過濾掉
    @Bean
    public RecordFilterStrategy<?, ?> recordFilterStrategy() {
        return consumerRecord -> {
            System.out.println("recordFilterStrategy");
            return false;
        };
    }

    // 整批或單筆資料錯誤處理
    @Bean
    public KafkaListenerErrorHandler myErrorHandler() {
        return (message, exception) -> {
            System.out.println("myErrorHandler");
            // 繼續往下拋 , 等於此次單筆或整批失敗 , 不會提交 offset , 會重複拿到相同資料
            throw new RuntimeException("ggg");
//            return null;
        };
    }

}
