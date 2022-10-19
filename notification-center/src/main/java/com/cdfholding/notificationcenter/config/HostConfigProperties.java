package com.cdfholding.notificationcenter.config;


import lombok.Getter;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class HostConfigProperties {


  @Value("${server.address}")
  String address;

  @Value("${server.port}")
  Integer port;

}
