package com.cdfholding.notificationcenter.kafka;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "")
@ConditionalOnProperty(prefix = "", name = "enabled", matchIfMissing = true)
public class KafkaProperties {

}
