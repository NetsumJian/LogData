package com.haofei.domain

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

case object TestDataSource extends DataSourceTrait {
  val c3p0 = new ComboPooledDataSource()

  val driverClass = "com.mysql.jdbc.Driver"
  val jdbcUrl = "jdbc:mysql://192.168.1.162:3306/data_bi_pay_value?useSSL=false&useUnicode=true&characterEncoding=UTF-8&connectTimeout=3000"
  val user = "root"
  val password = "MvMVoAFLVI9mAznX"

  c3p0.setDriverClass(driverClass)
  c3p0.setJdbcUrl(jdbcUrl)
  c3p0.setUser(user)
  c3p0.setPassword(password)

  override def getConnection(): Connection = {
    c3p0.getConnection
  }

}
