package com.cdfholding.notificationcenter.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cdfholding.notificationcenter.domain.Push;
import com.cdfholding.notificationcenter.dto.AllowedUserPushRequest;

@Service
public class PushServiceImpl implements PushService {
	@Autowired
	SmsServerService smsServerService;

	/**
	 * @param request
	 * @return
	 */
	@Override
	public Push send(AllowedUserPushRequest request) {
		Push push = convert(request);
		push.setIsSuccess(sends(push));
		return push;
	}

	private Push convert(AllowedUserPushRequest request) {
		Push push = new Push();
		push.setUuid(request.getUuid());
		push.setTimestamp(request.getTimestamp());
		push.setContent(request.getContent());
		push.setAdUser(request.getAdUser());

		return push;
	}

	public boolean sends(Push push) {
		return System.currentTimeMillis() % 2 == 0;
	}
}
