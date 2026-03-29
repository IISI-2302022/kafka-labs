package com.iisigroup.df.service;

import com.iisigroup.df.model.SendMailEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class MailServiceImpl implements MailService {

    private final JavaMailSender javaMailSender;

    @Value("${app.mail.from}")
    private String fromAddress;

    @Override
    public void send(SendMailEvent event) {
        val message = new SimpleMailMessage();
        message.setFrom(fromAddress);
        message.setTo(event.getTo());
        message.setSubject(event.getSubject());
        message.setText(event.getBody());
        try {
            javaMailSender.send(message);
            log.info("Mail 發送成功: {} ", message);
        } catch (MailException e) {
            log.error("Mail 發送失敗: {}", message, e);
            // 拋出例外讓 Kafka listener 的錯誤處理機制接手 (不 ack, 可觸發 retry)
            throw e;
        }
    }
}

