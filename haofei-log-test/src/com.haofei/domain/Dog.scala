package com.haofei.domain

object Dog extends Animal {
  val p.name = "jiji"
  override def test(): Unit ={
    println(p.name)
  }
}
