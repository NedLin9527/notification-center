package com.cdfholding.notificationcenter.service;

public interface RestTemplateService {

  Object restTemplate(String method,String host,Integer port);

}
