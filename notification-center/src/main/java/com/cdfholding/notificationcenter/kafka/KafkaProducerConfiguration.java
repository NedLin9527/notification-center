package com.cdfholding.notificationcenter.kafka;

import com.cdfholding.notificationcenter.config.KafkaConfigProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfiguration {

  final KafkaConfigProperties kafkaConfigProperties;


  public KafkaProducerConfiguration(KafkaConfigProperties kafkaConfigProperties) {
    this.kafkaConfigProperties = kafkaConfigProperties;
  }

  // ReplyingKafkaTemplate
  @Bean
  public ReplyingKafkaTemplate<String, ?, ?> replyingTemplate(
      ProducerFactory<String, ?> pf,
      ConcurrentMessageListenerContainer<String, ?> repliesContainer) {

    return new ReplyingKafkaTemplate<>(pf, repliesContainer);
  }

  @Bean
  public ConcurrentMessageListenerContainer<String, ?> repliesContainer(
      ConcurrentKafkaListenerContainerFactory<String, ?> containerFactory) {

    ConcurrentMessageListenerContainer<String, ?> repliesContainer =
        containerFactory.createContainer("allowed-user-events");
    repliesContainer.getContainerProperties().setGroupId("notification-center");
    repliesContainer.setAutoStartup(false);
    return repliesContainer;
  }

  @Bean
  public ProducerFactory<String, ?> producerFactory() {
    Map<String, Object> props = new HashMap<>();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaConfigProperties.getProducer_bootstrap_servers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
        kafkaConfigProperties.getStreams_replication_factor());

    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<String, ?> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }


}
