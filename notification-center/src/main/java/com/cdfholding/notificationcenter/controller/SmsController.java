package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.domain.Sms;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsResponse;
import com.cdfholding.notificationcenter.dto.CheckSmsResponse;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.UserService;
import com.cdfholding.notificationcenter.util.Generator;
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SmsController {

  @Autowired
  KafkaService<AllowedUserSmsRequest, Sms> kafkaService;
  @Autowired
  private UserService userService;

  @PostMapping(path = "/sms")
  public AllowedUserSmsResponse sms(@RequestBody AllowedUserSmsRequest request) {
    //check allowed user
    if (userService.query(request.getAdUser()) == null) {
      return new AllowedUserSmsResponse(request.getAdUser(), "Failed", "User not allowed",
          new Timestamp(System.currentTimeMillis()));
    }

    //generate uuid
    String uuid = Generator.createUuid();
    System.out.println("uuid = " + uuid);
    request.setUuid(uuid);

    //set type=sms
    request.setTimestamp(new Timestamp(System.currentTimeMillis()));

    //send channel-command topic
    try {
      AllowedUserSmsRequest value = kafkaService.sendToTopic(
          "sms-command", request.getUuid(), request, 10);
      return new AllowedUserSmsResponse(value.getAdUser(), value.getUuid(), null,
          value.getTimestamp());
    } catch (InterruptedException e) {
      e.printStackTrace();
      return new AllowedUserSmsResponse(request.getAdUser(), uuid, "InterruptedException",
          request.getTimestamp());
    } catch (ExecutionException e) {
      e.printStackTrace();
      return new AllowedUserSmsResponse(request.getAdUser(), uuid, "ExecutionException",
          request.getTimestamp());
    } catch (TimeoutException | java.util.concurrent.TimeoutException e) {
      e.printStackTrace();
      return new AllowedUserSmsResponse(request.getAdUser(), uuid, "TimeoutException",
          request.getTimestamp());
    }
  }

  @GetMapping(path = "/checkSms/{uuid}")
  public CheckSmsResponse checkSms(@PathVariable("uuid") String uuid) {
    //call SmsService.checkSms()
    return new CheckSmsResponse(uuid,
        kafkaService.getKTable("smsEventsTable", "remoteCheckSms/" + uuid, uuid, Sms.class, false));
  }

  @GetMapping(path = "/remoteCheckSms/{uuid}")
  public Sms remoteCheckSms(@PathVariable("uuid") String uuid) {
    //call SmsService.checkSms()
    return kafkaService.getKTable("smsEventsTable", null, uuid, Sms.class, true);
  }
}
