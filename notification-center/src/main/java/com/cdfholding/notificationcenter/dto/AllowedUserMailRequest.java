package com.cdfholding.notificationcenter.dto;

import lombok.Data;

@Data
public class AllowedUserMailRequest extends AllowedUserApplyRequest{
//  private String adUser;
//  private String type;
  private String mailTo;
  private String content;
  private String title;
}
