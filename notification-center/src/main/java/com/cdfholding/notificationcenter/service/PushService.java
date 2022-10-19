package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Push;
import com.cdfholding.notificationcenter.dto.AllowedUserPushRequest;

public interface PushService {
	Push send(AllowedUserPushRequest request);
}
