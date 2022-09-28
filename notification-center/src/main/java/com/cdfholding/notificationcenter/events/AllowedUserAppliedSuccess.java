package com.cdfholding.notificationcenter.events;

import lombok.Data;

@Data
public class AllowedUserAppliedSuccess {

  String adUser;

  String result;

  String reason;

}
