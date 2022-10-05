package com.cdfholding.notificationcenter.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.springframework.web.bind.annotation.RestController;
import com.cdfholding.notificationcenter.domain.LdapInfo;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.service.RestTemplateService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

@RestController
public class QueryController {


  final HostInfo hostInfo = new HostInfo("192.168.190.63", 8080);


  KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  @Autowired
  RestTemplateService restTemplateService;

  public QueryController(KafkaTemplate<String, AllowedUserApplyRequest> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  // List all users
  @SneakyThrows
  @GetMapping(path = "/listAllUsers")
  public Set<User> listAllUsers() throws InterruptedException {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }

    Collection<org.apache.kafka.streams.StreamsMetadata> metadataList =
        kafkaStreams.streamsMetadataForStore("userTable");
    ExecutorService executor = Executors.newFixedThreadPool(metadataList.size());
    Set<User> setUsers = new HashSet<>();
    for (StreamsMetadata streamsMetadata : metadataList) {
      executor.submit(() -> {
        hostInfoCheck(kafkaStreams, streamsMetadata, setUsers);
      });
    }

    Thread.sleep(500);
    return setUsers;
  }

  private void hostInfoCheck(KafkaStreams kafkaStreams, StreamsMetadata streamsMetadata,
      Set<User> setUsers) {
    System.out.println("Host info -> " + streamsMetadata.hostInfo().host() + " : "
        + streamsMetadata.hostInfo().port());
    System.out.println(streamsMetadata.stateStoreNames());

    if (!hostInfo.equals(streamsMetadata.hostInfo())) {
      // Remote
      ObjectMapper mapper = new ObjectMapper();

      // List<String> o = mapper.readValue("[\"hello\"]", collectionLikeType);

      Object res = restTemplateService.restTemplate("getAllowedUser",
          streamsMetadata.hostInfo().host(), streamsMetadata.hostInfo().port());
      System.out.println(res.toString());
      // mapper.readValue(json, new TypeReference<List<Person>>() {});
      try {
        List<User> o = mapper.convertValue(res, new TypeReference<List<User>>() {});
        setUsers.addAll(o);
      } catch (Exception ex) {
        System.out.println(ex.toString());
      }
      // List<User> o = mapper.convertValue(res, collectionLikeType.class);

    } else {
      ReadOnlyKeyValueStore<String, User> keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));
      keyValueStore.all().forEachRemaining(User -> setUsers.add(User.value));
    }
  }

  // Query user
  @SneakyThrows
  @GetMapping(path = "/queryUser/{adUser}")
  public User queryUser(@PathVariable("adUser") String adUser) throws InterruptedException {

    // kafkaTemplate.send("allowed-user", request.getAdUser(), request);

    // Create KafkaStreams
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();

    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    // stream userTable find HostInfo
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
      System.out.println("HostInfo is different!!" + keyMetada.activeHost());
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

    if (null != value) {
      LdapInfo ldapInfo = new LdapInfo();
      ldapInfo.setAdUser(adUser);
      ldapInfo.setIsValid(true);
      value.setLdapInfo(ldapInfo);
    }

    return value;
  }

  @SneakyThrows
  @GetMapping(path = "/checkUser/{adUser}")
  public User checkUser(@PathVariable("adUser") String adUser) throws InterruptedException {

    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }
    ReadOnlyKeyValueStore<String, User> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("userTable", QueryableStoreTypes.keyValueStore()));

    User value = keyValueStore.get(adUser);

    System.out.println(value);

    return value;
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
