package com.haofei.domain

case class Animal(){
  val p = new People("lili",18)
  def test(): Unit ={
    println(p.age)
  }
}
