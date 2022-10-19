package com.cdfholding.notificationcenter.dto;

import com.cdfholding.notificationcenter.domain.Sms;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CheckSmsResponse {

  private String uuid;
  private Sms result;
}
