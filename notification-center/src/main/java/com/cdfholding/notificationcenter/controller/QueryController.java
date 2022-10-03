package com.cdfholding.notificationcenter.controller;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
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
import com.cdfholding.notificationcenter.domain.LdapInfo;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;

@RestController
public class QueryController { 
  
  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;
  
  @Autowired
  StreamsBuilderFactoryBean factoryBean;
  
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
    keyValueStore = kafkaStreams.store(
      StoreQueryParameters.fromNameAndType("userTable", 
        QueryableStoreTypes.keyValueStore()));
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
  @GetMapping(path = "/queryUser/{adUser}")
  public User queryUser(
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
      ReadOnlyKeyValueStore<String, User> 
        keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(
            "userTable", QueryableStoreTypes.keyValueStore()));
      User value = 
        keyValueStore.get(adUser);
      for(int i = 0; i < keyValueStore.approximateNumEntries(); i++) {
          value = keyValueStore.get(adUser);
      }
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
      }
      System.out.println(value);

      return value;   
  }

}
