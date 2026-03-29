package com.iisigroup.df.service;

import com.iisigroup.df.model.SendMailEvent;

/**
 * 實際發送 mail 的 service
 */
public interface MailService {

    /**
     * 發送 mail
     *
     * @param event 發送 mail 事件
     */
    void send(SendMailEvent event);
}

