package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.stereotype.Service;

@Service
public class MailServiceImpl implements MailService {
  @Autowired
  private JavaMailSender mailSender;

  @Value("${spring.mail.username}")
  private String sender;

  @Override
  public SendMail send(AllowedUserMailRequest request) {
    SendMail sendMail = convert(request);
    prepareAndSend(sendMail);
    return sendMail;
  }

  private void prepareAndSend(SendMail sendMail) {
    try {

      // Creating a simple mail message
      SimpleMailMessage mailMessage
          = new SimpleMailMessage();

      // Setting up necessary details
      mailMessage.setFrom(sender);
      mailMessage.setTo(sendMail.getMailTo().split(","));
      mailMessage.setText(sendMail.getContent());
      mailMessage.setSubject(sendMail.getTitle());

      // Sending the mail
      mailSender.send(mailMessage);
      sendMail.setIsSuccess(true);
    } catch (Exception e) {
      e.fillInStackTrace();
      sendMail.setIsSuccess(false);
    }
  }

  /**
   * AllowedUserMailRequest to SendMail
   *
   * @param request
   * @return
   */
  private SendMail convert(AllowedUserMailRequest request) {
    SendMail sendMail = new SendMail();
    sendMail.setMailTo(request.getMailTo());
    sendMail.setContent(request.getContent());
    sendMail.setTimestamp(request.getTimestamp());
    sendMail.setUuid(request.getUuid());
    sendMail.setAdUser(request.getAdUser());
    sendMail.setTitle(request.getTitle());

    return sendMail;
  }
}
