package com.cdfholding.notificationcenter.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import com.cdfholding.notificationcenter.domain.LdapInfo;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserQueryResponse;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.UserService;
import lombok.SneakyThrows;

@RestController
public class QueryController {

  @Autowired
  UserService userService;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  @Autowired
  KafkaService<?, User> kafkaService;

  // List all users
  @SneakyThrows
  @GetMapping(path = "/listAllUsers")
  public AllowedUserQueryResponse listAllUsers() throws InterruptedException {

    AllowedUserQueryResponse queryResponse = new AllowedUserQueryResponse();
    List<User> adUserList = new ArrayList<>();
    try {
      adUserList = userService.list();
    } catch (CancellationException cancellationException) {
      cancellationException.printStackTrace();
      queryResponse.setMessage("CancellationException");
    } catch (ExecutionException executionException) {
      executionException.printStackTrace();
      queryResponse.setMessage("ExecutionException");
    } catch (InterruptedException interruptedException) {
      interruptedException.printStackTrace();
      queryResponse.setMessage("InterruptedException");
    } catch (TimeoutException timeoutException) {
      timeoutException.printStackTrace();
      queryResponse.setMessage("TimeoutException");
    }

    if (StringUtils.isBlank(queryResponse.getMessage())) {
      queryResponse.setAdUserList(adUserList);
    }

    return queryResponse;
  }

  // List all users without Kafka Streams DSL
  @SneakyThrows
  @GetMapping(path = "/listUsersWithoutStreams")
  public AllowedUserQueryResponse listUsersWithoutStreams() throws InterruptedException {
    AllowedUserQueryResponse queryResponse = new AllowedUserQueryResponse();
    // TODO
    return queryResponse;
  }

  // Query user
  @SneakyThrows
  @GetMapping(path = "/queryUser/{adUser}")
  public User queryUser(@PathVariable("adUser") String adUser) throws InterruptedException {
    return userService.query(adUser);
  }

  @SneakyThrows
  @GetMapping(path = "/checkUser/{adUser}")
  public User checkUser(@PathVariable("adUser") String adUser) throws InterruptedException {
    return kafkaService.getKTable("userTable", null, adUser, User.class, true);
  }

  @SneakyThrows
  @GetMapping(path = "/getAllowedUser")
  public List<User> getAllowedUser() throws InterruptedException {

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    ReadOnlyKeyValueStore<String, User> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));

    List<User> userValues = new ArrayList<>();
    // Set<User> setUsers = new HashSet<>();
    keyValueStore.all().forEachRemaining(User -> userValues.add(User.value));
    for (User user : userValues) {
      LdapInfo ldapInfo = new LdapInfo();
      ldapInfo.setAdUser(user.getAdUser());
      ldapInfo.setIsValid(true);
      user.setLdapInfo(ldapInfo);
    }

    return userValues;
  }

}
