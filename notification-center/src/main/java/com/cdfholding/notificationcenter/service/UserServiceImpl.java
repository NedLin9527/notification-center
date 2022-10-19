package com.cdfholding.notificationcenter.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import com.cdfholding.notificationcenter.config.HostConfigProperties;
import com.cdfholding.notificationcenter.domain.User;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

@Service
public class UserServiceImpl implements UserService {

  @Autowired
  private HostConfigProperties hostConfig;

  @Autowired
  StreamsBuilderFactoryBean factoryBean;

  @Autowired
  KafkaService<?, User> kafkaService;

  @Autowired
  RestTemplateService restTemplateService;

  /**
   * @param
   * @return
   */
  @SuppressWarnings("unchecked")
  @SneakyThrows
  @Override
  public List<User> list()
      throws CancellationException, ExecutionException, InterruptedException, TimeoutException {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      Thread.sleep(500);
    }

    Collection<org.apache.kafka.streams.StreamsMetadata> metadataList =
        kafkaStreams.streamsMetadataForStore("userTable");
    ExecutorService executor = Executors.newFixedThreadPool(metadataList.size());
    List<Future<Set<User>>> futures = new ArrayList<>();
    Set<User> setUsers = new HashSet<>();
    for (StreamsMetadata streamsMetadata : metadataList) {
      Future<Set<User>> future = (Future<Set<User>>) executor.submit(() -> {
        getHostInfoCheckResult(kafkaStreams, streamsMetadata, setUsers);
      });
      futures.add(future);
    }

    for (Future<Set<User>> future : futures) {
      future.get(1, TimeUnit.SECONDS);
    }

    List<User> adUserList = new ArrayList<>(setUsers);

    return adUserList;
  }

  private Set<User> getHostInfoCheckResult(KafkaStreams kafkaStreams,
      StreamsMetadata streamsMetadata, Set<User> setUsers) {

    HostInfo hostInfo = new HostInfo(hostConfig.getAddress(), hostConfig.getPort());

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

    return setUsers;
  }

  /**
   * @param adUser
   * @return
   */
  @Override
  @SneakyThrows
  public User query(String adUser) {
    return kafkaService.getKTable("userTable", "checkUser/" + adUser, adUser, User.class, false);
  }

  /**
   * @param adUser
   * @return number of deletion
   */
  @SneakyThrows
  @Override
  public int delete(String adUser) {
    int deleteNum = 0;

    User value =
        kafkaService.getKTable("userTable", "checkUser/" + adUser, adUser, User.class, false);

    if (null != value) {

      kafkaService.sendToTopic("allowed-user", adUser, null, 10);
      deleteNum = 1;

    }

    return deleteNum;
  }

}
