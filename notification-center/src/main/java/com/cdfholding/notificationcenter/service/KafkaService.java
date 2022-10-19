package com.cdfholding.notificationcenter.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface KafkaService<S, T> {
  public S sendToTopic (String topicName, String key, S value, int timeout)
      throws ExecutionException, InterruptedException, TimeoutException;

  public T getKTable (String storeName, String remoteUrl, String key, Class<? extends T> T, boolean isLocal);
}
