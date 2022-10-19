package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.config.HostConfigProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class KafkaServiceImpl<S, T> implements KafkaService<S, T> {

  @Autowired
  private HostConfigProperties hostConfig;
  KafkaTemplate<String, Object> kafkaTemplate;
  @Autowired
  StreamsBuilderFactoryBean factoryBean;
  @Autowired
  RestTemplateService restTemplateService;

  public KafkaServiceImpl(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * @param topicName
   * @param key
   * @param value
   * @return
   */
  @Override
  public S sendToTopic(String topicName, String key, S value, int timeout)
      throws ExecutionException, InterruptedException, TimeoutException {
    ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, key, value);
    SendResult<String, Object> sendResult = future.get(timeout, TimeUnit.SECONDS);
    return (S) sendResult.getProducerRecord().value();
  }

  /**
   * @param storeName
   * @param remoteUrl
   * @param T
   * @return
   */
  @SneakyThrows
  @Override
  public T getKTable(String storeName, String remoteUrl, String key, Class<? extends T> T,
      boolean isLocal) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    StringSerializer stringSerializer = new StringSerializer();
    // while loop until KafkaStreams.State.RUNNING
    while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
      System.out.println("state = " + kafkaStreams.state());
      Thread.sleep(500);
    }

    T value;
    // stream eventTable find HostInfo
    KeyQueryMetadata keyMetada = kafkaStreams.queryMetadataForKey(storeName, key,
        stringSerializer);
    HostInfo hostInfo = new HostInfo(hostConfig.getAddress(), hostConfig.getPort());

    if (!isLocal && !hostInfo.equals(keyMetada.activeHost())) {
      System.out.println("HostInfo is different!!" + keyMetada.activeHost());

      // Print all metadata HostInfo
      Collection<StreamsMetadata> metadata = kafkaStreams.metadataForAllStreamsClients();
      System.out.println("MetaDataclient:" + metadata.size());
      for (StreamsMetadata streamsMetadata : metadata) {
        System.out.println(
            "Host info -> " + streamsMetadata.hostInfo().host() + " : "
                + streamsMetadata.hostInfo()
                .port());
        System.out.println(streamsMetadata.stateStoreNames());
      }

      // Remote
      ObjectMapper mapper = new ObjectMapper();

      Object req = restTemplateService.restTemplate(
          remoteUrl, keyMetada.activeHost().host(),
          keyMetada.activeHost().port());

      value = mapper.convertValue(req, T);
      System.out.println("Remote value = " + value);
    } else {
      value = getLocalStore(storeName, key, kafkaStreams);
    }
    return value;
  }

  private T getLocalStore(String storeName, String key, KafkaStreams kafkaStreams) {
    ReadOnlyKeyValueStore<String, T> keyValueStore = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

    T value = keyValueStore.get(key);
    int timeout = 0;
    // 有時會發生value=null的情況，故先跑迴圈觀察
    while (null == value && timeout < 30) {
      timeout++;
      System.out.println("Local value is null " + timeout);
      keyValueStore = kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
      value = keyValueStore.get(key);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Local value = " + value);
    return value;
  }
}
