package com.cdfholding.notificationcenter.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.sql.Timestamp;
import lombok.Data;

@Data
public class SendMail {

  private String uuid;
  private String adUser;
  private String mailTo;
  private String title;
  private String content;
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone="Asia/Taipei")
  private Timestamp timestamp;
  private Boolean isSuccess;
}
