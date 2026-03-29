package com.iisigroup.df.service;

import com.iisigroup.df.model.SendMailEvent;
import com.iisigroup.df.model.SendMailResult;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class SendMailServiceImpl implements SendMailService {

    private final KafkaTemplate<String, SendMailEvent> kafkaTemplate;

    public SendMailResult sendMail(SendMailEvent sendMailEvent) {
        val future = kafkaTemplate.sendDefault(sendMailEvent);
        return new SendMailResult(future);
    }

}
