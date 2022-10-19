package com.cdfholding.notificationcenter.dto;

import java.util.List;
import com.cdfholding.notificationcenter.domain.User;
import lombok.Data;

@Data
public class AllowedUserQueryResponse {

  List<User> adUserList;

  String message;

}
