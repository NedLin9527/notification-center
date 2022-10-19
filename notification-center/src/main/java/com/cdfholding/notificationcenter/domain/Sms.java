package com.cdfholding.notificationcenter.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sms {

  private String uuid;
  private String adUser;
  private String phoneNum;
  private String content;
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone="Asia/Taipei")
  private Timestamp timestamp;
  private Boolean isSuccess;
}
