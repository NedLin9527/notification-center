package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Sms;
import org.springframework.stereotype.Service;

@Service
public class FakeSmsServerServiceImpl implements SmsServerService{

  /**
   * @param sms
   * @return
   */
  @Override
  public boolean send(Sms sms) {
    return System.currentTimeMillis() % 2 == 0;
  }
}
