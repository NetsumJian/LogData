while (true) {
  val flag = System.currentTimeMillis()/1000 % 300 < 5
  Thread.sleep(5000)
  println(flag)
}
