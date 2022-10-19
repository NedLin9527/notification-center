package com.cdfholding.notificationcenter.events;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class AllowedUserAppliedEvent {

  @Schema(description = "使用者名稱")
  String adUser;

  @Schema(description = "結果")
  String result;

  @Schema(description = "原因")
  String reason;

}
