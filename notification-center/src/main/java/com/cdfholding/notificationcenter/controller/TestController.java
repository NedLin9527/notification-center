package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyResponse;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.KafkaServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Test")
@RestController
public class TestController {


  KafkaTemplate<String, String> kafkaTemplate;

  public TestController(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Operation(summary = "新增", description = "將使用者資訊傳入並新增")
  @GetMapping(path = "/test/{topic}&{key}&{value}")
  public String test(
      @Parameter(description = "topic", example = "kafka-test") @PathVariable("topic") String topic,
      @Parameter(description = "Key", example = "1234") @PathVariable("key") String key,
      @Parameter(description = "Value", example = "1234") @PathVariable("value") String value) {

    kafkaTemplate.send(topic, key, value);

    return "";
  }
}
