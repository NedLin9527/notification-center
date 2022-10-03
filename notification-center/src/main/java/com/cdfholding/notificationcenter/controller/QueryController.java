package com.cdfholding.notificationcenter.controller;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
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
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedSuccess;
import com.cdfholding.notificationcenter.service.RestTemplateService;

@RestController
public class QueryController {
  
  final HostInfo hostInfo = new HostInfo("127.0.0.1", 8100);
  
  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;
  
  @Autowired
  StreamsBuilderFactoryBean factoryBean; 

  @Autowired
  RestTemplateService restTemplateService;
  
  public QueryController(
      KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate) {
          this.kafkaTemplate = kafkaTemplate;
  }  
  
  // List all users
  @PostMapping(path="listAllUsers")
  public List<AllowedUserAppliedSuccess> listAllUsers(
      @RequestBody AllowedUserApplyRequest request) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)){
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }
    ReadOnlyKeyValueStore<String, AllowedUserAppliedSuccess> 
      keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(
          "userTable", QueryableStoreTypes.keyValueStore()));

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    keyValueStore = kafkaStreams.store(
      StoreQueryParameters.fromNameAndType("userTable", 
        QueryableStoreTypes.keyValueStore()));
    List<AllowedUserAppliedSuccess> userValues = new ArrayList<>();
    keyValueStore.all().forEachRemaining(AllowedUserAppliedSuccess -> userValues.add(AllowedUserAppliedSuccess.value));

    return userValues;      
  }
  
  // Query user
  @GetMapping(path = "/queryUser/{adUser}")
  public AllowedUserAppliedSuccess queryUser(
      @PathVariable("adUser") String adUser) {
      KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
      // while loop until KafkaStreams.State.RUNNING
      while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)){
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
      }
      ReadOnlyKeyValueStore<String, AllowedUserAppliedSuccess> 
        keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(
            "userTable", QueryableStoreTypes.keyValueStore()));
      AllowedUserAppliedSuccess value = 
        keyValueStore.get(adUser);
      for(int i = 0; i < keyValueStore.approximateNumEntries(); i++) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          keyValueStore = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType("userTable", 
              QueryableStoreTypes.keyValueStore()));
          value = keyValueStore.get(adUser);
      }
      if(null == value) {
        value = new AllowedUserAppliedSuccess();
        value.setReason("The queried user does not exist.");
      }
      System.out.println(value);

      return value;   
  }

}
