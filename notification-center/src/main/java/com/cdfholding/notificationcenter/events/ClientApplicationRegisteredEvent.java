package com.cdfholding.notificationcenter.events;


import com.cdfholding.notificationcenter.model.Application;

/**
 * This event gets emitted when an application is registered.
 *
 * @author Johannes Stelzer
 */
public class ClientApplicationRegisteredEvent extends ClientApplicationEvent {
  private static final long serialVersionUID = 1L;

  public ClientApplicationRegisteredEvent(Application application) {
    super(application, "REGISTRATION");
  }
}
