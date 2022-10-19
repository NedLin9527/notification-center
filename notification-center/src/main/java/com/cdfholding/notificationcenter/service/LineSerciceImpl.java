package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.Line;
import com.cdfholding.notificationcenter.line.LineNotifier;

public class LineSerciceImpl implements  LineService{

  @Override
  public Line sendLine() throws Exception {

    LineNotifier lineNotifier= new LineNotifier();

    lineNotifier.doNotify();

    return null;
  }
}
