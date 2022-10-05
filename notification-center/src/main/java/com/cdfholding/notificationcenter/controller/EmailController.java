package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailResponse;
import com.cdfholding.notificationcenter.service.MailService;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailController {

  KafkaTemplate<String, Object> kafkaTemplate;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  @Autowired
  RestTemplateService restTemplateService;

  @Autowired
  MailService mailService;

  final HostInfo hostInfo = new HostInfo("127.0.0.1", 8080);

  public EmailController(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @SneakyThrows
  @PostMapping(path = "/sendEmail")
  public AllowedUserMailResponse mail(@RequestBody AllowedUserMailRequest request) {
    //generator uuid
    String uuid = UUID.randomUUID().toString();
    System.out.println("uuid = " + uuid);

    //send to channel-command
    request.setUuid(uuid);
    request.setType("mail");
    request.setTimestamp(new Timestamp(System.currentTimeMillis()));

    ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send("channel-command", request.getUuid(), request);
    try {
      SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
      AllowedUserMailRequest value = (AllowedUserMailRequest)sendResult.getProducerRecord().value();
      return new AllowedUserMailResponse(value.getAdUser(), value.getUuid(), null,
          value.getTimestamp());
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      //Thread.currentThread.interrupt();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "InterruptedException",
          request.getTimestamp());
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "ExecutionException",
          request.getTimestamp());
    } catch (TimeoutException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return new AllowedUserMailResponse(request.getAdUser(), uuid, "TimeoutException",
          request.getTimestamp());
    }
  }

  @SneakyThrows
  @GetMapping(path = "/remoteCheckMail/{uuid}")
  public SendMail remoteCheckMail(@PathVariable("uuid") String uuid) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      System.out.println("state = " + kafkaStreams.state());
      Thread.sleep(500);
    }
    // stream mailTable find HostInfo
    ReadOnlyKeyValueStore<String, SendMail> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("mailEventsTable", QueryableStoreTypes.keyValueStore()));

    SendMail value = keyValueStore.get(uuid);
    System.out.println("value = " + value);

    return value;
  }

  @SneakyThrows
  @GetMapping(path = "/checkMail/{uuid}")
  public SendMail checkMail(@PathVariable("uuid") String uuid) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      System.out.println("state = " + kafkaStreams.state());
      Thread.sleep(500);
    }
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada = kafkaStreams.queryMetadataForKey("mailEventsTable", uuid,
        stringSerializer);

    SendMail value = new SendMail();

    if (!hostInfo.equals(keyMetada.activeHost())) {
      System.out.println("HostInfo is different!!" + keyMetada.activeHost());

      // Print all metadata HostInfo
      Collection<StreamsMetadata> metadata = kafkaStreams.metadataForAllStreamsClients();
      System.out.println("MetaDataclient:" + metadata.size());
      for (StreamsMetadata streamsMetadata : metadata) {
        System.out.println(
            "Host info -> " + streamsMetadata.hostInfo().host() + " : " + streamsMetadata.hostInfo()
                .port());
        System.out.println(streamsMetadata.stateStoreNames());
      }

      // Remote
      ObjectMapper mapper = new ObjectMapper();

      Object req = restTemplateService.restTemplate(
          "remoteCheckMail/" + uuid, keyMetada.activeHost().host(),
          keyMetada.activeHost().port());

      value = mapper.convertValue(req, SendMail.class);
      System.out.println("remote value = " + value);
    } else {
      ReadOnlyKeyValueStore<String, SendMail> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("mailEventsTable", QueryableStoreTypes.keyValueStore()));

      value = keyValueStore.get(uuid);
      System.out.println("value = " + value);
    }

    return value;
  }

}
