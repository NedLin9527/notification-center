package com.cdfholding.notificationcenter.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class KafkaConfigProperties {


  @Value("${spring.kafka.consumer.bootstrap-servers}")
  String consumer_bootstrap_servers;

  @Value("${spring.kafka.consumer.group-id}")
  String consumer_group_id;


  @Value("${spring.kafka.producer.bootstrap-servers}")
  String producer_bootstrap_servers;

  @Value("${spring.kafka.producer.group-id}")
  String producer_group_id;

  // @Value("${spring.kafka.streams.state-dir}")
  // String state_dir;


  @Value("${spring.kafka.streams.application-id}")
  String streams_application_id;
  @Value("${spring.kafka.streams.bootstrap-servers}")
  String streams_bootstrap_servers;
  @Value("${spring.kafka.streams.replication-factor}")
  Integer streams_replication_factor;


}
