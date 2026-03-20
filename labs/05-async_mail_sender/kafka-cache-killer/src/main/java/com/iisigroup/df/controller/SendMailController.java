package com.iisigroup.df.controller;

import com.iisigroup.df.model.SendMailRqDto;
import com.iisigroup.df.service.SendMailService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
public class SendMailController {

    private final SendMailService sendMailService;

    @PostMapping(
            value = "/mail/send",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<?> sendMail(@Valid @RequestBody SendMailRqDto sendMailRqDto) {
        val sendMailEvent = SendMailRqDto.SendMailMapper.INSTANCE.toEvent(sendMailRqDto);
        // 非同步發送 mail event 至 kafka
        sendMailService.sendMail(sendMailEvent);
        return ResponseEntity.ok()
                .build();
    }


}
