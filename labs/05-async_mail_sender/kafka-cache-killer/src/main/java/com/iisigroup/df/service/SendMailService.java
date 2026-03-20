package com.iisigroup.df.service;

import com.iisigroup.df.model.SendMailEvent;
import com.iisigroup.df.model.SendMailResult;


public interface SendMailService {
    SendMailResult sendMail(SendMailEvent sendMailEvent);
}
