package com.haofei.domain

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource


object SXQDDataSource extends DataSourceTrait {
  val c3p0 = new ComboPooledDataSource()

  val driverClass = "com.mysql.jdbc.Driver"
  val jdbcUrl = "jdbc:mysql://rm-wz919h3957zsehk2p.mysql.rds.aliyuncs.com:3306/sxqd_tslog?useSSL=false&useUnicode=true&characterEncoding=UTF-8&connectTimeout=3000"
  val user = "hf_root"
  val password = "T+o7eyalZqUAWf8sFAZPSFs5plQ="

  c3p0.setDriverClass(driverClass)
  c3p0.setJdbcUrl(jdbcUrl)
  c3p0.setUser(user)
  c3p0.setPassword(password)

  def getConnection(): Connection = {
    c3p0.getConnection
  }

}
