package com.cdfholding.notificationcenter.controller;


import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyResponse;
import com.cdfholding.notificationcenter.dto.DeletedAllowedUserResponse;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collection;
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
import lombok.SneakyThrows;

@RestController
public class AdminController {

  final HostInfo hostInfo = new HostInfo("192.168.190.63", 8080);

  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  @Autowired
  RestTemplateService restTemplateService;

  public AdminController(KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }


  @SneakyThrows
  @PostMapping(path = "/apply")
  public AllowedUserApplyResponse apply(@RequestBody AllowedUserApplyRequest request) {
    request.setType("apply");
    // send to Kafka
    kafkaTemplate.send("allowed-user-command", request.getAdUser(), request);

    // Create KafkaStreams
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();

    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada =
        kafkaStreams.queryMetadataForKey("eventTable", request.getAdUser(), stringSerializer);

    AllowedUserAppliedEvent value = new AllowedUserAppliedEvent();

    if (!hostInfo.equals(keyMetada.activeHost())) {
      System.out.println("HostInfo is different!!" + keyMetada.activeHost());

      // Print all metadata HostInfo
      Collection<StreamsMetadata> metadata = kafkaStreams.metadataForAllStreamsClients();
      System.out.println("MetaDataclient:" + metadata.size());
      for (StreamsMetadata streamsMetadata : metadata) {
        System.out.println("Host info -> " + streamsMetadata.hostInfo().host() + " : "
            + streamsMetadata.hostInfo().port());
        System.out.println(streamsMetadata.stateStoreNames());
      }

      // Remote
      ObjectMapper mapper = new ObjectMapper();

      Object req = restTemplateService.restTemplate("checkEvent/" + request.getAdUser(),
          keyMetada.activeHost().host(), keyMetada.activeHost().port());

      value = mapper.convertValue(req, AllowedUserAppliedEvent.class);

    } else {

      ReadOnlyKeyValueStore<String, AllowedUserAppliedEvent> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("eventTable", QueryableStoreTypes.keyValueStore()));

      value = keyValueStore.get(request.getAdUser());
      // while loop until get the data
      while (value == null) {
        Thread.sleep(500);
        keyValueStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("eventTable",
            QueryableStoreTypes.keyValueStore()));
        value = keyValueStore.get(request.getAdUser());
      }
      System.out.println(value);
    }

    return new AllowedUserApplyResponse(value.getAdUser(), value.getResult(), value.getReason());
  }

  @SneakyThrows
  @GetMapping(path = "/delete/{adUser}")
  public DeletedAllowedUserResponse delete(@PathVariable("adUser") String adUser) {

    DeletedAllowedUserResponse response = null;

    // Create KafkaStreams
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();

    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada =
        kafkaStreams.queryMetadataForKey("userTable", adUser, stringSerializer);
    // Print all metadata HostInfo
    Collection<StreamsMetadata> metadata = kafkaStreams.metadataForAllStreamsClients();
    System.out.println("MetaDataclient:" + metadata.size());
    for (StreamsMetadata streamsMetadata : metadata) {
      System.out.println("Host info -> " + streamsMetadata.hostInfo().host() + " : "
          + streamsMetadata.hostInfo().port());
      System.out.println(streamsMetadata.stateStoreNames());
    }
    User value = new User();

    if (!hostInfo.equals(keyMetada.activeHost())) {
      System.out.println("HostInfo is different!!");
      System.out.println("HostInfo is different!!" + "Host:" + hostInfo);
      System.out.println("HostInfo is different!!" + "key Host:" + keyMetada.activeHost());
      // Remote
      ObjectMapper mapper = new ObjectMapper();

      Object req = restTemplateService.restTemplate("checkUser/" + adUser,
          keyMetada.activeHost().host(), keyMetada.activeHost().port());

      value = mapper.convertValue(req, User.class);

    } else {

      ReadOnlyKeyValueStore<String, User> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));

      value = keyValueStore.get(adUser);


    }

    if (null == value) {

      response = new DeletedAllowedUserResponse(adUser, "Failure", "USER NOT FOUND");

    } else {

      response = new DeletedAllowedUserResponse(adUser, "Successful", "");

      kafkaTemplate.send("allowed-user", adUser, null);
    }

    return response;
  }

  @SneakyThrows
  @GetMapping(path = "/checkEvent/{adUser}")
  public AllowedUserAppliedEvent checkEvent(@PathVariable("adUser") String adUser) {

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    ReadOnlyKeyValueStore<String, AllowedUserAppliedEvent> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("eventTable", QueryableStoreTypes.keyValueStore()));

    AllowedUserAppliedEvent value = keyValueStore.get(adUser);
    // while loop until get the data
    while (value == null) {
      Thread.sleep(500);
      keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("eventTable", QueryableStoreTypes.keyValueStore()));
      value = keyValueStore.get(adUser);
    }
    System.out.println(value);

    return value;
  }

}
