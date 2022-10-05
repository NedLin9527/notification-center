package com.cdfholding.notificationcenter.events;

import com.cdfholding.notificationcenter.model.Application;
import java.io.Serializable;

public class ClientApplicationEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Application application;

  private final long timestamp;
  private final String type;

  protected ClientApplicationEvent(Application application, String type) {
    this.application = application;
    this.timestamp = System.currentTimeMillis();
    this.type = type;
  }

  /**
   * @return system time in milliseconds when the event happened.
   */
  public final long getTimestamp() {
    return this.timestamp;
  }

  /**
   * @return affected application.
   */
  public Application getApplication() {
    return application;
  }

  /**
   * @return event type (for JSON).
   */
  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((application == null) ? 0 : application.hashCode());
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ClientApplicationEvent other = (ClientApplicationEvent) obj;
    if (application == null) {
      if (other.application != null) {
        return false;
      }
    } else if (!application.equals(other.application)) {
      return false;
    }
    if (timestamp != other.timestamp) {
      return false;
    }
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }
}