package com.cdfholding.notificationcenter.dto;

import java.sql.Timestamp;
import lombok.Data;

@Data
public class AllowedUserMailRequest{
  private String adUser;
  private String type;
  private String mailTo;
  private String content;
  private String title;
  private Timestamp timestamp;
  private String uuid;
}
