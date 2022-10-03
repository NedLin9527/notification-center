package com.cdfholding.notificationcenter.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.cdfholding.notificationcenter.domain.LdapInfo;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class QueryController {
  
  final HostInfo hostInfo = new HostInfo("127.0.0.1", 8080);
  
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
  public List<User> listAllUsers(
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
    ReadOnlyKeyValueStore<String, User> 
      keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(
          "userTable", QueryableStoreTypes.keyValueStore()));

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    List<User> userValues = new ArrayList<>();
    keyValueStore.all().forEachRemaining(User -> userValues.add(User.value));
    for(User user: userValues) {
      LdapInfo ldapInfo = new LdapInfo();
      ldapInfo.setAdUser(user.getAdUser());
      ldapInfo.setIsValid(true);
      user.setLdapInfo(ldapInfo);       
    }

    return userValues;      
  }
  
  // Query user
  @PostMapping(path = "/queryUser/{adUser}")
  public User queryUser(
      @PathVariable("adUser") String adUser, 
      @RequestBody AllowedUserApplyRequest request) {
    request.setType("queryUser");

    kafkaTemplate.send("allowed-user", request.getAdUser(), request);

    // Create KafkaStreams
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();

    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)){
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada = kafkaStreams.queryMetadataForKey("userTable", request.getAdUser(),
        stringSerializer);

    User value = new User();

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
          "queryUser/" + request.getAdUser(), keyMetada.activeHost().host(),
          keyMetada.activeHost().port());

      value = mapper.convertValue(req, User.class);

    } else {

      ReadOnlyKeyValueStore<String, User> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));

      //while loop until get the data
      for(int i = 0; i < keyValueStore.approximateNumEntries(); i++) {
        value = keyValueStore.get(request.getAdUser());
        if(null == value) {
          value = new User();
          value.setAdUser(adUser);
          LdapInfo ldapInfo = new LdapInfo();
          ldapInfo.setAdUser(adUser);
          ldapInfo.setIsValid(false);
          value.setLdapInfo(ldapInfo);
        } else {
          LdapInfo ldapInfo = new LdapInfo();
          ldapInfo.setAdUser(adUser);
          ldapInfo.setIsValid(true);
          value.setLdapInfo(ldapInfo);
          break;
        }
      }
      System.out.println(value);
      
    }

    return value;
  }

}
