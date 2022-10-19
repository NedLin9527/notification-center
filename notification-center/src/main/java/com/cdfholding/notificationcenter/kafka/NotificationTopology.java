package com.cdfholding.notificationcenter.kafka;

import com.cdfholding.notificationcenter.domain.Push;
import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.domain.Sms;
import com.cdfholding.notificationcenter.domain.User;
import com.cdfholding.notificationcenter.dto.AllowedUserApplyRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserPushRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserSmsRequest;
import com.cdfholding.notificationcenter.events.AllowedUserAppliedEvent;
import com.cdfholding.notificationcenter.serialization.JsonSerdes;
import com.cdfholding.notificationcenter.service.LdapService;
import com.cdfholding.notificationcenter.service.MailService;
import com.cdfholding.notificationcenter.service.PushService;
import com.cdfholding.notificationcenter.service.SmsService;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NotificationTopology {


  @Autowired
  LdapService ldapService;

  @Autowired
  MailService mailService;

  @Autowired
  SmsService smsService;

  @Autowired
  PushService pushService;
  
  @Autowired
  void smsPipeLine(StreamsBuilder streamsBuilder) {
    System.out.println();
    System.out.println("========================== It's smsPipeLine ==========================");
    System.out.println();

    KStream<String, AllowedUserSmsRequest> commandStream = streamsBuilder.stream(
        "sms-command",
        Consumed.with(Serdes.String(), JsonSerdes.AllowedUserSmsRequest()));

    commandStream.print(Printed.toSysOut());

    Map<String, KStream<String, AllowedUserSmsRequest>> branches = commandStream.split(
            Named.as("SmsBranch-"))
//        .branch((key, value) -> value.getType().equals("sms"), Branched.as("SmsRequest"))
        .defaultBranch(Branched.as("Request"));

    KStream<String, AllowedUserSmsRequest> smsRequestKStream = branches.get(
        "SmsBranch-Request");

    KStream<String, Sms> smsKStream = smsRequestKStream.mapValues(
        allowedUserSmsRequest -> sms(allowedUserSmsRequest));

    smsKStream.to("sms-events",
        Produced.with(Serdes.String(), JsonSerdes.Sms()));

    streamsBuilder.table("sms-events",
        Consumed.with(Serdes.String(), JsonSerdes.Sms()),
        Materialized.as("smsEventsTable"));
  }

  /**
   * Send SMS
   * @param request
   * @return
   */
  private Sms sms(AllowedUserSmsRequest request) {
    return smsService.send(request);
  }

  @Autowired
  void mailPipeLine(StreamsBuilder streamsBuilder) {
    System.out.println();
    System.out.println("========================== It's mailPipeLine ==========================");
    System.out.println();

    KStream<String, AllowedUserMailRequest> commandStream = streamsBuilder.stream(
        "mail-command",
        Consumed.with(Serdes.String(), JsonSerdes.AllowedUserMailRequest()));

    commandStream.print(Printed.toSysOut());

    Map<String, KStream<String, AllowedUserMailRequest>> branches = commandStream.split(
            Named.as("MailBranch-"))
//        .branch((key, value) -> value.getType().equals("mail"), Branched.as("MailRequest"))
        .defaultBranch(Branched.as("Request"));

    KStream<String, AllowedUserMailRequest> mailRequestKStream = branches.get(
        "MailBranch-Request");

    KStream<String, SendMail> mailKStream = mailRequestKStream.mapValues(
        allowedUserMailRequest -> sendMail(allowedUserMailRequest));

    mailKStream.to("mail-events",
        Produced.with(Serdes.String(), JsonSerdes.SendMail()));

    streamsBuilder.table("mail-events",
        Consumed.with(Serdes.String(), JsonSerdes.SendMail()),
        Materialized.as("mailEventsTable"));
  }

  //@Autowired
  void pipeline(StreamsBuilder streamsBuilder) {
    System.out.println("========================== It's pipeLine ==========================");
    KStream<String, AllowedUserApplyRequest> commandStream = streamsBuilder.stream(
        "allowed-user-command",
        Consumed.with(Serdes.String(), JsonSerdes.AllowedUserApplyRequest()));

    commandStream.print(Printed.toSysOut());

    Map<String, KStream<String, AllowedUserApplyRequest>> branches = commandStream.split(
            Named.as("Branch-"))
        .branch((key, value) -> value.getType().equals("apply"), Branched.as("ApplyRequest"))
        .defaultBranch(Branched.as("Others"));

    KStream<String, AllowedUserApplyRequest> applyRequestKStream = branches.get(
        "Branch-ApplyRequest");
    // KStream<String, AllowedUserApplyRequest> otherKStream = branches.get("Branch-Others");
    KStream<String, User> userKStream = applyRequestKStream.mapValues(
        allowedUserApplyRequest -> queryLdap(allowedUserApplyRequest.getAdUser()));

    Map<String, KStream<String, User>> userBranches = userKStream.split(Named.as("Branch2-"))
        .branch((key, value) -> value.getLdapInfo().getIsValid(), Branched.as("ValidUsers"))
        .branch((key, value) -> !value.getLdapInfo().getIsValid(), Branched.as("InvalidUsers"))
        .noDefaultBranch();

    KStream<String, User> validUsers = userBranches.get("Branch2-ValidUsers");
    // KStream<String, User> invalidUsers = userBranches.get("Branch2-InvalidUsers");
    // allowed-user
    validUsers.to("allowed-user", Produced.with(Serdes.String(), JsonSerdes.User()));

    // event
    KStream<String, AllowedUserAppliedEvent> eventStream = userKStream.map(
        (String, User) -> new KeyValue<>(String, allowedUserAppliedEvent(String, User)));

    eventStream.to("allowed-user-events",
        Produced.with(Serdes.String(), JsonSerdes.AllowedUserAppliedEvent()));

    streamsBuilder.table("allowed-user",
        Consumed.with(Serdes.String(), JsonSerdes.User()),
        Materialized.as("userTable"));

    streamsBuilder.table("allowed-user-events",
        Consumed.with(Serdes.String(), JsonSerdes.AllowedUserAppliedEvent()),
        Materialized.as("eventTable"));
  }


  private AllowedUserAppliedEvent allowedUserAppliedEvent(String adUser, User user) {

    AllowedUserAppliedEvent appliedEvent = new AllowedUserAppliedEvent();
    appliedEvent.setAdUser(adUser);

    if (null != user & user.getLdapInfo().getIsValid()) {
      appliedEvent.setResult("Success");
      appliedEvent.setReason(null);
    } else {
      appliedEvent.setResult("Failure");
      appliedEvent.setReason("error");
    }

    return appliedEvent;

  }

  private User queryLdap(String adUser) {
    User user = new User();
    user.setAdUser(adUser);
    user.setLdapInfo(ldapService.query(adUser));
    return user;
  }

  private SendMail sendMail(AllowedUserMailRequest request) {
    System.out.println("AllowedUserMailRequest = " + request);
    return mailService.send(request);
  }

	@Autowired
	void pushPipeLine(StreamsBuilder streamsBuilder) {
		System.out.println("========================== It's pushPipeLine ==========================");

		KStream<String, AllowedUserPushRequest> commandStream = streamsBuilder.stream("push-command",
				Consumed.with(Serdes.String(), JsonSerdes.AllowedUserPushRequest()));

		commandStream.print(Printed.toSysOut());

		Map<String, KStream<String, AllowedUserPushRequest>> branches = commandStream.split(Named.as("pushBranch-"))
				.defaultBranch(Branched.as("Request"));

		KStream<String, AllowedUserPushRequest> pushRequestKStream = branches.get("pushBranch-Request");

		KStream<String, Push> pushKStream = pushRequestKStream
				.mapValues(allowedUserPushRequest -> push(allowedUserPushRequest));

		pushKStream.to("push-events", Produced.with(Serdes.String(), JsonSerdes.Push()));

		streamsBuilder.table("push-events", Consumed.with(Serdes.String(), JsonSerdes.Push()),
				Materialized.as("pushEventsTable"));
	}

	/**
	 * Send push
	 * 
	 * @param request
	 * @return
	 */
	private Push push(AllowedUserPushRequest request) {
		return pushService.send(request);
	}
}
