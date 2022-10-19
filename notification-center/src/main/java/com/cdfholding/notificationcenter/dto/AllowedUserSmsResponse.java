package com.cdfholding.notificationcenter.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AllowedUserSmsResponse {

  private String adUser;
  private String result;
  private String reason;
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Taipei")
  private Timestamp timestamp;
}
