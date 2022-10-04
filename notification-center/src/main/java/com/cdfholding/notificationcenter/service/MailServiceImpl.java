package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import org.springframework.stereotype.Service;

@Service
public class MailServiceImpl implements MailService {

  @Override
  public SendMail send(AllowedUserMailRequest request) {
    SendMail sendMail = convert(request);
    sendMail.setIsSuccess(System.currentTimeMillis()%2 == 0);
    return sendMail;
  }

  /**
   * AllowedUserMailRequest to SendMail
   * @param request
   * @return
   */
  private SendMail convert(AllowedUserMailRequest request) {
    SendMail sendMail = new SendMail();
    sendMail.setMailTo(request.getMailTo());
    sendMail.setMailFrom(request.getMailFrom());
    sendMail.setContent(request.getContent());
    sendMail.setTimestamp(request.getTimestamp());
    sendMail.setUuid(request.getUuid());
    sendMail.setAdUser(request.getAdUser());
    sendMail.setTitle(request.getTitle());

    return sendMail;
  }
}
