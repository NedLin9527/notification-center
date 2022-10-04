package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailResponse;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import java.util.UUID;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailController {
  KafkaTemplate<String, Object> kafkaTemplate;

  public EmailController(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @SneakyThrows
  @PostMapping(path = "/sendEmail")
  public AllowedUserMailResponse mail(@RequestBody AllowedUserMailRequest request) {
    //generator uuid
    String uuid = UUID.randomUUID().toString();
    //send to channel-command
    request.setType("mail");
    kafkaTemplate.send("channel-command", request.getAdUser(), request);

    //return uuid


    return new AllowedUserMailResponse(request.getAdUser(), uuid, null);
  }

}
