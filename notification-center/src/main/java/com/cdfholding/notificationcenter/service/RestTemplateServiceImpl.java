package com.cdfholding.notificationcenter.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class RestTemplateServiceImpl implements RestTemplateService {


  @Override
  public Object restTemplate(String method, String host, Integer port) {

    String url = String.format("http://%s:%d/%s", host,
        port, method);

    RestTemplate restemp = new RestTemplate();



    return restemp.getForObject(url, Object.class);


  }
}
