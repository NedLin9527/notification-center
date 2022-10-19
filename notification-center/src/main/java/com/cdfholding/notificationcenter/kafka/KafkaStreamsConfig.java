package com.cdfholding.notificationcenter.kafka;


import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
// import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import com.cdfholding.notificationcenter.config.HostConfigProperties;
import com.cdfholding.notificationcenter.config.KafkaConfigProperties;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

  @Autowired
  KafkaConfigProperties kafkaConfigProperties;

  @Autowired
  HostConfigProperties hostConfig;


  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, kafkaConfigProperties.getStreams_application_id());
    props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaConfigProperties.getStreams_bootstrap_servers());
    props.put(APPLICATION_SERVER_CONFIG,
        hostConfig.getAddress() + ":" + hostConfig.getPort().toString());
    // props.put(STATE_DIR_CONFIG, kafkaConfigProperties.getState_dir());
    props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
        kafkaConfigProperties.getStreams_replication_factor());
    return new KafkaStreamsConfiguration(props);
  }


}
