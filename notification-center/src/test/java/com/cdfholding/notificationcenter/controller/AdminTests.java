package com.cdfholding.notificationcenter.controller;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AdminController.class)
@WebAppConfiguration
public class AdminTests {

  @Autowired
  private WebApplicationContext webApplicationContext;
  MockMvc mvc; //創建MockMvc類的物件

  @Before
  public void setup(){
//		Memberaccount memberaccount = new Memberaccount();
//		memberaccount.setId(1);
    mvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }

  @Test
  public void contextLoads() throws Exception {
    String uri = "/apply";
    MvcResult result = mvc.perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON)).andReturn();
    int status = result.getResponse().getStatus();
    System.out.println(status);
    Assert.assertEquals("錯誤",200,status);
  }

}
