package com.cdfholding.notificationcenter.domain;

import java.sql.Timestamp;
import lombok.Data;

@Data
public class SendMail {

  private String uuid;
  private String adUser;
  private String mailFrom;
  private String mailTo;
  private String title;
  private String content;
  private Timestamp timestamp;
  private Boolean isSuccess;
}
