package com.cdfholding.notificationcenter.controller;

import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyResponse;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.kafka.KafkaStreamsConfig;
import com.cdfholding.notificationcenter.kafka.NotificationTopology;
import java.util.Collection;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdminController {

  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  public AdminController(KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }


  @PostMapping(path = "/apply")
  public AllowedUserApplyResponse apply(@RequestBody AllowedUserApplyRequest request) {
    request.setType("apply");
    kafkaTemplate.send("allowed-user-command", request.getAdUser(), request);

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

    StringSerializer stringSerializer =new StringSerializer();
    //Collection<StreamsMetadata> metadata =
    KeyQueryMetadata a = kafkaStreams.queryMetadataForKey("eventTable", request.getAdUser(),
        stringSerializer);
    System.out.println(a.activeHost());
    // System.out.println(metadata.size());
//    for (StreamsMetadata streamsMetadata : metadata) {
//      System.out.println(
//          "Host info -> " + streamsMetadata.hostInfo().host() + " : " + streamsMetadata.hostInfo()
//              .port());
//      System.out.println(streamsMetadata.stateStoreNames());
//    }
    ReadOnlyKeyValueStore<String, AllowedUserAppliedEvent> eventStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("eventTable", QueryableStoreTypes.keyValueStore()));

    AllowedUserAppliedEvent event = eventStore.get(request.getAdUser());
    int eventTimeout = 0;
    while (eventTimeout < 60 && event == null) {
      eventTimeout++;
      System.out.println("timeout " + eventTimeout);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println(event);

    KeyValueIterator<String, AllowedUserAppliedEvent> range = eventStore.all();

    Collection<StreamsMetadata> metadata = kafkaStreams.metadataForAllStreamsClients();
    System.out.println(metadata.size());
    for (StreamsMetadata streamsMetadata : metadata) {
      System.out.println(
          "Host info -> " + streamsMetadata.hostInfo().host() + " : " + streamsMetadata.hostInfo()
              .port());
      System.out.println(streamsMetadata.stateStoreNames());
    }

    return new AllowedUserApplyResponse(event.getAdUser(), event.getResult(), event.getReason());
  }

  @PostMapping(path = "/queryUser")
  public User queryUser(@RequestBody AllowedUserApplyRequest request){
    request.setType("queryUser");
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

    ReadOnlyKeyValueStore<String, User> userStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    User user = userStore.get(request.getAdUser());

    KeyValueIterator<String, User> allUser = userStore.all();
    while (allUser.hasNext()) {
      System.out.println(allUser.next().value);
    }

    return user;
  }
}
