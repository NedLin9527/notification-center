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
import lombok.SneakyThrows;
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
    kafkaTemplate.send("channel-command", request.getUuid(), request);
    System.out.println("send channel-command");

    //get mailTable
    // Create KafkaStreams
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();

    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)){
      System.out.println("state = " + kafkaStreams.state());
      Thread.sleep(500);
    }
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada = kafkaStreams.queryMetadataForKey("mailTable", request.getUuid(),
        stringSerializer);

    AllowedUserMailRequest value = new AllowedUserMailRequest();

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
          "checkMail/" + request.getUuid(), keyMetada.activeHost().host(),
          keyMetada.activeHost().port());

//      value = mapper.convertValue(req, AllowedUserAppliedEvent.class);

    } else {

      ReadOnlyKeyValueStore<String, AllowedUserMailRequest> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("mailTable", QueryableStoreTypes.keyValueStore()));

      value = keyValueStore.get(request.getUuid());
      //while loop until get the data
      while (value == null ) {
        System.out.println("value is null");
        Thread.sleep(500);
        keyValueStore = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType("mailTable", QueryableStoreTypes.keyValueStore()));
        value = keyValueStore.get(request.getUuid());
      }

      System.out.println("value = " + value);
    }

    //send Mail
    SendMail sendMail = mailService.send(request);

    //send channel-mail-event
    kafkaTemplate.send("channel-mail-event", sendMail.getUuid(), sendMail);

    //return uuid
    return new AllowedUserMailResponse(value.getAdUser(), value.getUuid(), null, value.getTimestamp());
  }

  @SneakyThrows
  @GetMapping(path = "/checkMail/{uuid}")
  public AllowedUserMailRequest checkMail(@PathVariable("uuid") String uuid) {

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)){
      Thread.sleep(500);
    }
    ReadOnlyKeyValueStore<String, AllowedUserMailRequest> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("mailTable", QueryableStoreTypes.keyValueStore()));

    AllowedUserMailRequest value = keyValueStore.get(uuid);
    //while loop until get the data
    while (value == null ) {
      Thread.sleep(500);
      keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("mailTable", QueryableStoreTypes.keyValueStore()));
      value = keyValueStore.get(uuid);
    }
    System.out.println("value = " + value);

    return value;
  }

}
