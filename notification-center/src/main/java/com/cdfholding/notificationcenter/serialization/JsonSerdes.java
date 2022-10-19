package com.cdfholding.notificationcenter.serialization;

import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.domain.Sms;
import com.cdfholding.notificationcenter.domain.Push;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserPushRequest;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedSuccess;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {

  public static Serde<AllowedUserApplyRequest> AllowedUserApplyRequest() {
    JsonSerializer<AllowedUserApplyRequest> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<AllowedUserApplyRequest> jsonDeserializer = new JsonDeserializer<>(
        AllowedUserApplyRequest.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<User> User() {
    JsonSerializer<User> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<User> jsonDeserializer = new JsonDeserializer<>(User.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<AllowedUserAppliedEvent> AllowedUserAppliedEvent() {
    JsonSerializer<AllowedUserAppliedEvent> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<AllowedUserAppliedEvent> jsonDeserializer = new JsonDeserializer<>(
        AllowedUserAppliedEvent.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<AllowedUserAppliedSuccess> AllowedUserAppliedSuccess() {
    JsonSerializer<AllowedUserAppliedSuccess> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<AllowedUserAppliedSuccess> jsonDeserializer = new JsonDeserializer<>(
        AllowedUserAppliedSuccess.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<AllowedUserMailRequest> AllowedUserMailRequest() {
    JsonSerializer<AllowedUserMailRequest> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<AllowedUserMailRequest> jsonDeserializer = new JsonDeserializer<>(
        AllowedUserMailRequest.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<SendMail> SendMail() {
    JsonSerializer<SendMail> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<SendMail> jsonDeserializer = new JsonDeserializer<>(
        SendMail.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<AllowedUserSmsRequest> AllowedUserSmsRequest() {
    JsonSerializer<AllowedUserSmsRequest> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<AllowedUserSmsRequest> jsonDeserializer = new JsonDeserializer<>(
        AllowedUserSmsRequest.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }

  public static Serde<Sms> Sms() {
    JsonSerializer<Sms> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<Sms> jsonDeserializer = new JsonDeserializer<>(
        Sms.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }
  
	public static Serde<AllowedUserPushRequest> AllowedUserPushRequest() {
		JsonSerializer<AllowedUserPushRequest> jsonSerializer = new JsonSerializer<>();
		JsonDeserializer<AllowedUserPushRequest> jsonDeserializer = new JsonDeserializer<>(AllowedUserPushRequest.class);

		return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
	}

	public static Serde<Push> Push() {
		JsonSerializer<Push> jsonSerializer = new JsonSerializer<>();
		JsonDeserializer<Push> jsonDeserializer = new JsonDeserializer<>(Push.class);

		return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
	}
  
}
