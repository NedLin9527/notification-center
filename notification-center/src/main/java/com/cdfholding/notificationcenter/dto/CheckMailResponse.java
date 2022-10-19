package com.cdfholding.notificationcenter.dto;

import com.cdfholding.notificationcenter.domain.SendMail;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CheckMailResponse {
  private String uuid;
  private SendMail result;
}
