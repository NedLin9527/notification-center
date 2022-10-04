package com.cdfholding.notificationcenter.kafka;


import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {
  @Value("#{systemProperties['spring.kafka.bootstrap-servers'] ?: '192.168.223.63:29092,192.168.223.63:29093,192.168.223.63:29094'}")
  private String bootstrapAddress;
  @Value("#{systemProperties['spring.kafka.rpcEndpoint'] ?: '192.168.223.63:8080'}")
  private String rpcEndpoint;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, "notification-center");
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(APPLICATION_SERVER_CONFIG, rpcEndpoint);
    props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
    return new KafkaStreamsConfiguration(props);
  }


}
