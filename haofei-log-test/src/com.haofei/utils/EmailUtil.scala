package com.haofei.utils

import org.apache.commons.mail._

case object EmailUtil {

  def sendSimpleTextEmail(header:String,msg:String): Unit ={
    val email = new SimpleEmail();
    email.setHostName("smtp.haofeigame.com");
    email.setSmtpPort(465);
    email.setAuthenticator(new DefaultAuthenticator("jiangwen@haofeigame.com", "jw123456A"));
    email.setSSLOnConnect(true);
    email.setFrom("jiangwen@haofeigame.com");
    email.setSubject(header);
    email.setMsg(msg);
    email.addTo("dwithj@126.com");
    email.send();
  }
}
