package com.cdfholding.notificationcenter.controller;

import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.cdfholding.notificationcenter.domain.Push;
import com.cdfholding.notificationcenter.dto.AllowedUserPushRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserPushResponse;
import com.cdfholding.notificationcenter.dto.CheckPushResponse;
import com.cdfholding.notificationcenter.service.KafkaService;
import com.cdfholding.notificationcenter.service.UserService;
import com.cdfholding.notificationcenter.util.Generator;

@RestController
public class PushController {

	@Autowired
	private UserService userService;

	@Autowired
	KafkaService<AllowedUserPushRequest, Push> kafkaService;

	@PostMapping(path = "/push")
	public AllowedUserPushResponse push(@RequestBody AllowedUserPushRequest request) {
		System.out.println("start push");
		if (userService.query(request.getAdUser()) == null) {
			return new AllowedUserPushResponse(request.getAdUser(), "Failed", "User not allowed",
					new Timestamp(System.currentTimeMillis()));
		}

		// generate uuid
		String uuid = Generator.createUuid();
		System.out.println("uuid = " + uuid);

		// set type=push
		request.setTimestamp(new Timestamp(System.currentTimeMillis()));

		// send channel-command topic
		try {
			AllowedUserPushRequest value = kafkaService.sendToTopic("push-command", request.getUuid(), request, 10);
			return new AllowedUserPushResponse(value.getAdUser(), value.getUuid(), null, value.getTimestamp());
		} catch (InterruptedException e) {
			e.printStackTrace();
			return new AllowedUserPushResponse(request.getAdUser(), uuid, "InterruptedException",
					request.getTimestamp());
		} catch (ExecutionException e) {
			e.printStackTrace();
			return new AllowedUserPushResponse(request.getAdUser(), uuid, "ExecutionException", request.getTimestamp());
		} catch (TimeoutException | java.util.concurrent.TimeoutException e) {
			e.printStackTrace();
			return new AllowedUserPushResponse(request.getAdUser(), uuid, "TimeoutException", request.getTimestamp());
		}
	}

	@GetMapping(path = "/checkPush/{uuid}")
	public CheckPushResponse checkPush(@PathVariable("uuid") String uuid) {
		return new CheckPushResponse(uuid,
				kafkaService.getKTable("pushEventsTable", "remoteCheckPush/" + uuid, uuid, Push.class, false));
	}

	@GetMapping(path = "/remoteCheckPush/{uuid}")
	public Push remoteCheckPush(@PathVariable("uuid") String uuid) {
		// call pushService.checkpush()
		return kafkaService.getKTable("pushEventsTable", null, uuid, Push.class, true);
	}

}
