package com.cdfholding.notificationcenter.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
@AllArgsConstructor
public class DeletedAllowedUserResponse {

  @Schema(description = "使用者名稱")
  String adUser;

  @Schema(description = "結果")
  String result;

  @Schema(description = "原因")
  String reason;

}
