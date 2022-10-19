package com.cdfholding.notificationcenter.dto;

import com.cdfholding.notificationcenter.domain.Push;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CheckPushResponse {

  private String uuid;
  private Push result;
}
