package com.cdfholding.notificationcenter.line;

import com.cdfholding.notificationcenter.events.ClientApplicationEvent;
import com.cdfholding.notificationcenter.events.ClientApplicationRegisteredEvent;
import com.cdfholding.notificationcenter.events.ClientApplicationStatusChangedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Component
public class LineNotifier {

  @Autowired
  private LineProperties lineProperties;
  private static final String DEFAULT_MESSAGE = "#{application.name} (#{application.id}) is #{to.status}";
  private final SpelExpressionParser parser = new SpelExpressionParser();


  //@Override
  public void doNotify() throws Exception {
    if (lineProperties.isEnabled() == false) {
      return;
    }

    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    //headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON_UTF8));
    headers.add("Authorization",
        String.format("%s %s", "Bearer", lineProperties.getChannelToken()));

    HashMap object = new HashMap<>();
    object.put("to", "0938669057");
    List messages = new ArrayList();
    HashMap message = new HashMap<>();
    message.put("type", "text");
    message.put("text", "test message");
    messages.add(message);
    object.put("messages", messages);

    HttpEntity<HashMap> entity = new HttpEntity<HashMap>(object, headers);
    ResponseEntity<String> response = restTemplate.exchange(
        "https://api.line.me/v2/bot/message/push",
        HttpMethod.POST, entity, String.class);
    if (response.getStatusCode().is2xxSuccessful()) {
      System.out.println(response.getBody());
    }

  }
}
