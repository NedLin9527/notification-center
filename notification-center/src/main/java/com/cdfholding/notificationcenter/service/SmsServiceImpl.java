package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Sms;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SmsServiceImpl implements SmsService{
  @Autowired
  SmsServerService smsServerService;

  /**
   * @param request
   * @return
   */
  @Override
  public Sms send(AllowedUserSmsRequest request) {
    Sms sms = convert(request);
    sms.setIsSuccess(smsServerService.send(sms));
    return sms;
  }

  private Sms convert(AllowedUserSmsRequest request) {
    Sms sms = new Sms();
    sms.setUuid(request.getUuid());
    sms.setTimestamp(request.getTimestamp());
    sms.setContent(request.getContent());
    sms.setAdUser(request.getAdUser());
    sms.setPhoneNum(request.getPhoneNum());
    return sms;
  }
}
