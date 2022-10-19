package com.cdfholding.notificationcenter.dto;

import java.sql.Timestamp;
import lombok.Data;

@Data
public class AllowedUserSmsRequest {

  private String adUser;
  private String uuid;
  private String phoneNum;
  private String content;
  private Timestamp timestamp;
}
