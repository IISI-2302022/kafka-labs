package com.iisigroup.df.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendMailEvent {
    // 代表唯一值
//    private String msgId;
    // 傳遞給誰 (mail address)
    private String to;
    // 主旨
    private String subject;
    // 文字
    private String body;
}
