package com.cdfholding.notificationcenter.service;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import com.cdfholding.notificationcenter.domain.User;

public interface UserService {

  List<User> list() throws CancellationException, ExecutionException, InterruptedException, TimeoutException;

  User query(String adUser);

  int delete(String adUser);

}
