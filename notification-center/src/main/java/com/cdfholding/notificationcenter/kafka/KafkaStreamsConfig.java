package com.cdfholding.notificationcenter.kafka;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.springframework.kafka.support.KafkaHeaders.GROUP_ID;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

  @Value("#{systemProperties['spring.kafka.bootstrap-servers'] ?: '192.168.140.63:29092'}")
  private String bootstrapAddress;
  @Value("#{systemProperties['spring.kafka.rpcEndpoint'] ?: '192.168.140.63:8080'}")
  private String rpcEndpoint;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, "notification-center");
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(APPLICATION_SERVER_CONFIG, rpcEndpoint);

    return new KafkaStreamsConfiguration(props);
  }


}
