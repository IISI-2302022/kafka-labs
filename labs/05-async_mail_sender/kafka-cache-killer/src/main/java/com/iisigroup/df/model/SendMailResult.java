package com.iisigroup.df.model;

import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public class SendMailResult {

    private final CompletableFuture<SendResult<String, SendMailEvent>> future;

    public SendMailResult(CompletableFuture<SendResult<String, SendMailEvent>> future) {
        this.future = future;
    }

}
