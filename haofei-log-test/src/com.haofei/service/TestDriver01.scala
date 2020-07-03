package com.haofei.service

import org.joda.time.DateTime

object TestDriver01 {
  def main(args: Array[String]): Unit = {
    while (true) {
      val flag = System.currentTimeMillis()/1000 % 60 < 5
      Thread.sleep(5000)
      if (flag) println(new DateTime())
    }
  }
}
