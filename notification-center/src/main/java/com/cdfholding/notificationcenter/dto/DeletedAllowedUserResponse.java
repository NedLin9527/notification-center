package com.cdfholding.notificationcenter.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;

@ToString
@Data
@AllArgsConstructor
public class DeletedAllowedUserResponse {

  String adUser;

  String result;

  String reason;

}
