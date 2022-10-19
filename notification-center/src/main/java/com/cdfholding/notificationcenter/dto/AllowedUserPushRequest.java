package com.cdfholding.notificationcenter.dto;

import java.sql.Timestamp;

import lombok.Data;

@Data
public class AllowedUserPushRequest {
	private String adUser;
	private String uuid;
	private String content;
	private String title;
	private String message;
	private Timestamp timestamp;

}
