package com.cdfholding.notificationcenter.model;

import static java.util.Collections.unmodifiableMap;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents the info fetched from the info actuator endpoint at a certain time.
 *
 * @author Johannes Edmeier
 */
public class Info implements Serializable {
  private static final long serialVersionUID = 2L;
  private static Info EMPTY = new Info(0L, null);

  @JsonIgnore
  private final long timestamp;
  private final Map<String, ? extends Serializable> values;

  protected Info(long timestamp, Map<String, ? extends Serializable> values) {
    this.timestamp = timestamp;
    this.values = values != null ? unmodifiableMap(new LinkedHashMap<>(values))
        : Collections.<String, Serializable>emptyMap();
  }

  public static Info from(Map<String, ? extends Serializable> values) {
    return new Info(System.currentTimeMillis(), values);
  }

  public static Info empty() {
    return EMPTY;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @JsonAnyGetter
  public Map<String, ? extends Serializable> getValues() {
    return values;
  }

  @Override
  public String toString() {
    return "Info [timestamp=" + timestamp + ", values=" + values + "]";
  }

}
