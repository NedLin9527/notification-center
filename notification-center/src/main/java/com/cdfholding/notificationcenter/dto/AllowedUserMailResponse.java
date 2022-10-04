package com.cdfholding.notificationcenter.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AllowedUserMailResponse {
  private String adUser;
  private String result;
  private String reason;
}
