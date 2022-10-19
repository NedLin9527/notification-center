package com.cdfholding.notificationcenter;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@OpenAPIDefinition(info = @Info(title = "MCC", version = "1.0.0"))
@SpringBootApplication
public class NotificationCenterApplication {

  public static void main(String[] args) {
    SpringApplication.run(NotificationCenterApplication.class, args);
  }

}
