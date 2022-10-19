package com.cdfholding.notificationcenter.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@AllArgsConstructor
public class AllowedUserApplyResponse {

  @Getter
  @Schema(description = "使用者名稱")
  String adUser;

  @Getter
  @Schema(description = "結果")
  String result;


  @Getter
  @Schema(description = "原因")
  String reason;
}
