package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Sms;

public interface SmsServerService {
  public boolean send(Sms sms);
}
