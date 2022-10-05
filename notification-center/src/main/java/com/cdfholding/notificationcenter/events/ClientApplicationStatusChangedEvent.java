package com.cdfholding.notificationcenter.events;

import com.cdfholding.notificationcenter.model.Application;
import com.cdfholding.notificationcenter.model.StatusInfo;

/**
 * This event gets emitted when an application is registered.
 *
 * @author Johannes Stelzer
 */
public class ClientApplicationStatusChangedEvent extends ClientApplicationEvent {
  private static final long serialVersionUID = 1L;
  private final StatusInfo from;
  private final StatusInfo to;

  public ClientApplicationStatusChangedEvent(Application application, StatusInfo from,
      StatusInfo to) {
    super(application, "STATUS_CHANGE");
    this.from = from;
    this.to = to;
  }

  public StatusInfo getFrom() {
    return from;
  }

  public StatusInfo getTo() {
    return to;
  }
}
