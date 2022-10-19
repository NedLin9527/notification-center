package com.cdfholding.notificationcenter.kafka;

import com.cdfholding.notificationcenter.config.KafkaConfigProperties;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.LdapService;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class KafkaTraceListener {

  @Autowired
  KafkaConfigProperties kafkaConfigProperties;

  @Autowired
  KafkaService<User, User> kafkaService;

  @Autowired
  LdapService ldapService;


  @KafkaListener(topics = "allowed-user-command", groupId = "group_id")
  @SendTo("allowed-user-events")
  public AllowedUserAppliedEvent applyListener(
      @Payload(required = false) AllowedUserApplyRequest request,
      @Header(KafkaHeaders.OFFSET) String offSet)
      throws ExecutionException, InterruptedException, TimeoutException {
    log.info("OffSet: " + offSet + " Received Message: " + request.toString());
    User user = queryLdap(request.getAdUser());

    if (user.getLdapInfo().getIsValid()) {
      log.info(request.getAdUser() + " is valid!!");
      User value = kafkaService.sendToTopic("allowed-user", request.getAdUser(), user, 10);
    } else {
      log.info(request.getAdUser() + " is not Valid!!");
    }

    return allowedUserAppliedEvent(request.getAdUser(), user);
  }

//  @KafkaListener(topics = "allowed-user-command", groupId = "group_id")
//  public void listenKafkaTest(ConsumerRecord<String,AllowedUserApplyRequest> record) {
//    System.out.println("received = " + record.value() + " with key " + record.key());
//  }

  private User queryLdap(String adUser) {
    User user = new User();
    user.setAdUser(adUser);
    user.setLdapInfo(ldapService.query(adUser));
    return user;
  }

  private AllowedUserAppliedEvent allowedUserAppliedEvent(String adUser, User user) {

    AllowedUserAppliedEvent appliedEvent = new AllowedUserAppliedEvent();
    appliedEvent.setAdUser(adUser);

    if (null != user & user.getLdapInfo().getIsValid()) {
      appliedEvent.setResult("Success");
      appliedEvent.setReason(null);
    } else {
      appliedEvent.setResult("Failure");
      appliedEvent.setReason("error");
    }

    return appliedEvent;

  }

}
