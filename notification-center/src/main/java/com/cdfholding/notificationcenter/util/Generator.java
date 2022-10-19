package com.cdfholding.notificationcenter.util;

import java.util.UUID;

public class Generator {
  public static String createUuid() {
    return UUID.randomUUID().toString();
  }
}
