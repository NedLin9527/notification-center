package com.cdfholding.notificationcenter.controller;


import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyResponse;
import com.cdfholding.notificationcenter.dto.DeletedAllowedUserResponse;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.cdfholding.notificationcenter.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@Tag(name = "Admin Management 管理功能")
@Log4j2
@RestController
public class AdminController {

  @Autowired
  ReplyingKafkaTemplate<String, AllowedUserApplyRequest, AllowedUserAppliedEvent> template;
  @Autowired
  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;
  @Autowired
  StreamsBuilderFactoryBean factoryBean;
  @Autowired
  UserService userService;
  @Autowired
  RestTemplateService restTemplateService;
  @Autowired
  KafkaService<AllowedUserApplyRequest, AllowedUserAppliedEvent> kafkaService;

  public AdminController(KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }


  @Operation(summary = "新增", description = "將使用者資訊傳入並新增")
  @PostMapping(path = "/apply")
  public AllowedUserApplyResponse apply(@RequestBody AllowedUserApplyRequest request) {
    request.setType("apply");

    // send to Kafka
    try {
      ProducerRecord<String, AllowedUserApplyRequest> record = new ProducerRecord<>(
          "allowed-user-command", request);
      TopicPartition replyPartition = template.getAssignedReplyTopicPartitions().iterator().next();
      if (template.getAssignedReplyTopicPartitions() != null &&
          template.getAssignedReplyTopicPartitions().iterator().hasNext()) {
        replyPartition = template.getAssignedReplyTopicPartitions().iterator().next();

      }
      record.headers()
          .add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "allowed-user-events".getBytes()))
          .add(new RecordHeader(KafkaHeaders.REPLY_PARTITION,
              intToBytesBigEndian(replyPartition.partition())));
      RequestReplyFuture<String, AllowedUserApplyRequest, AllowedUserAppliedEvent> replyFuture = template.sendAndReceive(
          record);
      SendResult<String, AllowedUserApplyRequest> sendResult = replyFuture.getSendFuture()
          .get(10, TimeUnit.SECONDS);
      log.info("Sent ok: " + sendResult.getRecordMetadata());
      ConsumerRecord<String, AllowedUserAppliedEvent> consumerRecord = replyFuture.get(10,
          TimeUnit.SECONDS);
      log.info("Return value: " + consumerRecord.value());

      return new AllowedUserApplyResponse(consumerRecord.value().getAdUser(),
          consumerRecord.value().getResult(), consumerRecord.value().getReason());
    } catch (ExecutionException e) {
      return new AllowedUserApplyResponse(request.getAdUser(), "ExecutionException", null);
    } catch (InterruptedException e) {
      return new AllowedUserApplyResponse(request.getAdUser(), "InterruptedException", null);
    } catch (TimeoutException e) {
      return new AllowedUserApplyResponse(request.getAdUser(), "TimeoutException", null);
    }
  }

  private static byte[] intToBytesBigEndian(final int data) {
    return new byte[]{(byte) ((data >> 24) & 0xff), (byte) ((data >> 16) & 0xff),
        (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff),};
  }

  @SneakyThrows
  @Operation(summary = "刪除", description = "刪除使用者使用權限")
  @GetMapping(path = "/delete/{adUser}")
  public DeletedAllowedUserResponse delete(
      @Parameter(required = true, description = "使用者名稱", example = "AD12345") @PathVariable("adUser") String adUser) {

    DeletedAllowedUserResponse response;
    int deleteNum = userService.delete(adUser);
    if (deleteNum <= 0) {

      response = new DeletedAllowedUserResponse(adUser, "Failure", "USER NOT FOUND");

    } else {

      response = new DeletedAllowedUserResponse(adUser, "Successful", "");

    }
    return response;
  }

  @SneakyThrows
  @Operation(summary = "檢查", description = "檢查使用者新增結果")
  @GetMapping(path = "/checkEvent/{adUser}")
  public AllowedUserAppliedEvent checkEvent(
      @Parameter(required = true, description = "使用者名稱", example = "AD12345") @PathVariable("adUser") String adUser) {
    return kafkaService.getKTable("eventTable", null, adUser, AllowedUserAppliedEvent.class, true);
  }

}
