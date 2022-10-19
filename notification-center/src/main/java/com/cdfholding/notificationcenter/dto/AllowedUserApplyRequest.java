package com.cdfholding.notificationcenter.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class AllowedUserApplyRequest {


  @Schema(required = true, description = "使用者名稱")
  private String adUser;

  // request 沒有使用的欄位 先隱藏
  @Schema(hidden = true)
  private String type;
}
