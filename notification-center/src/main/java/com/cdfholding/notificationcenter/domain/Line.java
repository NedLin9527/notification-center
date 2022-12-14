package com.cdfholding.notificationcenter.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Line {

  private String uuid;
  private String adUser;
  private String lineID;
  private String content;
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Taipei")
  private Timestamp timestamp;
  private Boolean isSuccess;
}
