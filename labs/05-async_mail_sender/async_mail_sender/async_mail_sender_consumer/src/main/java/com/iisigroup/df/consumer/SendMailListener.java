package com.iisigroup.df.consumer;

import com.iisigroup.df.model.SendMailEvent;
import com.iisigroup.df.service.MailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@RequiredArgsConstructor
@Component
public class SendMailListener {

    private final MailService mailService;

    @KafkaListener(
            topics = "${spring.kafka.template.default-topic}"
    )
    public void listen(SendMailEvent sendMailEvent) {
        log.info("Received mail event: {}", sendMailEvent);
        mailService.send(sendMailEvent);
        log.info("Sent mail successfully: {} ", sendMailEvent);
        // 可優化 , 將 send mail event 加上唯一標示 : msgId
        // 方法 1 搭配 redis
        // 接收 event 時
        //  檢查 msgId 在 redis 是否已存在且為已發送狀態
        //  若是則直接忽略不處理
        // 若不存在
        //   則新增一筆資料(setnx), 狀態是發送中至 redis(若同時有多個 consumer 收到相同 msgId , 只會有一個成功)
        //   新增成功之 consumer 發送 mail
        //     發送成功就將狀態改成已發送
        //     發送失敗就將狀態改成失敗
        //     狀態改變都有機會失敗(服務突然關閉,機率低而已)
        // 若存在且為發送失敗狀態
        //   嘗試取得 redis 鎖來發送(redisson 套件 or 自行實作)
        //   取得鎖成功後之 consumer 就發送 mail
        //      發送成功就將狀態改成已發送
        //      不管成功與否將鎖 unlock (鎖建議設 ttl)
        //   取得鎖失敗的 consumer 就不處理

        // 若存在且為發送中狀態
        //  代表目前有人發送中或發送成功或失敗但狀態未改變成功
        //  檢查 timeout
        //    如果未超過 timeout 時間 , 就視作某個 consumer 正在發送 , 就不處理
        //    如果已超過 timeout 時間 , 就嘗試取得鎖來發送
        //  取的鎖成功後之 consumer 就發送 mail
        //      發送成功就將狀態改成已發送
        //      不管成功與否將鎖 unlock (鎖建議設 ttl)
        //   取得鎖失敗的 consumer 就不處理

        // 方法 2 使用 outbox pattern , 發送 mail event 時 , 同時將 event 資訊寫入 db 的 outbox table
        // 由另一個獨立的 process 定期掃描 outbox table , 發送 mail event 並更新狀態

    }

}
