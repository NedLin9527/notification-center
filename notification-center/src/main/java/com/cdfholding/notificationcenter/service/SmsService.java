package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Sms;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsRequest;

public interface SmsService {

  Sms send(AllowedUserSmsRequest request);
}
