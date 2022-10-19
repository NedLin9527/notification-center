package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.config.HostConfigProperties;
import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailResponse;
import com.cdfholding.notificationcenter.dto.CheckMailResponse;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.cdfholding.notificationcenter.service.UserService;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailController {

  @Autowired
  RestTemplateService restTemplateService;

  @Autowired
  KafkaService<AllowedUserMailRequest, SendMail> kafkaService;
  @Autowired
  private HostConfigProperties hostConfig;

  @Autowired
  private UserService userService;

  @SneakyThrows
  @PostMapping(path = "/sendEmail")
  public AllowedUserMailResponse mail(@RequestBody AllowedUserMailRequest request) {
    if (userService.query(request.getAdUser()) == null) {
      return new AllowedUserMailResponse(request.getAdUser(), "Failed", "User not allowed",
          new Timestamp(System.currentTimeMillis()));
    }

    //generator uuid
    String uuid = UUID.randomUUID().toString();
    System.out.println("uuid = " + uuid);

    //send to channel-command
    request.setUuid(uuid);
    request.setTimestamp(new Timestamp(System.currentTimeMillis()));

    try {
      AllowedUserMailRequest value = kafkaService.sendToTopic(
          "mail-command", request.getUuid(), request, 10);
      return new AllowedUserMailResponse(value.getAdUser(), value.getUuid(), null,
          value.getTimestamp());
    } catch (InterruptedException e) {
      e.printStackTrace();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "InterruptedException",
          request.getTimestamp());
    } catch (ExecutionException e) {
      e.printStackTrace();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "ExecutionException",
          request.getTimestamp());
    } catch (TimeoutException e) {
      e.printStackTrace();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "TimeoutException",
          request.getTimestamp());
    }
  }

  @SneakyThrows
  @GetMapping(path = "/remoteCheckMail/{uuid}")
  public SendMail remoteCheckMail(@PathVariable("uuid") String uuid) {
    return kafkaService.getKTable("mailEventsTable", null, uuid, SendMail.class, true);
  }

  @SneakyThrows
  @GetMapping(path = "/checkMail/{uuid}")
  public CheckMailResponse checkMail(@PathVariable("uuid") String uuid) {
    return new CheckMailResponse(uuid,
        kafkaService.getKTable("mailEventsTable", "remoteCheckMail/" + uuid, uuid, SendMail.class,
            false));
  }

}
