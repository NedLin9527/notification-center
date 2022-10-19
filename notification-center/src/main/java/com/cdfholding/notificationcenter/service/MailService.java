package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.SendMail;
import com.cdfholding.notificationcenter.dto.AllowedUserMailRequest;
import com.cdfholding.notificationcenter.dto.AllowedUserMailResponse;

public interface MailService {

  SendMail send(AllowedUserMailRequest request);

}
