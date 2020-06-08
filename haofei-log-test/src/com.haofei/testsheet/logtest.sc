
import scala.collection.mutable

val arrays = new mutable.ArrayBuffer[String]
arrays.append("hello")
arrays.append("world")
arrays.foreach(println)
arrays.clear()
arrays.size
try {
  val i = 1 / 0
} catch {
  case e => println(e.getMessage)
}

val s = """insert into delete_treature_stage_record_flow(`event_time`,`stage`,`details`) values ('1591201803','187459','{"details":"[{\"sail_player\":{\"nickname\":\"花花丶芯\",\"uid\":\"276447499\",\"img_url\":\"http:\/\/e004198dd4.hnfeisheng.com\/shouxin-channel-production\/\/default_head.png\",\"gold\":46028218,\"route\":1,\"icon_box_id\":0,\"vip\":8},\"uid\":\"276447499\",\"result\":1312798,\"sail_time\":1589990360},{\"sail_player\":{\"nickname\":\"曹生\",\"uid\":\"276447352\",\"img_url\":\"http:\/\/e004198dd4.hnfeisheng.com\/shouxin-channel-production\/\/default_head.png\",\"gold\":52216192,\"route\":2,\"icon_box_id\":0,\"vip\":2},\"uid\":\"276447352\",\"result\":1248099,\"sail_time\":1589990360},{\"sail_player\":{\"nickname\":\"132****6219\",\"uid\":\"276447330\",\"img_url\":\"http:\/\/e004198dd4.hnfeisheng.com\/shouxin-channel-production\/\/default_head.png\",\"gold\":-1317380,\"route\":3,\"icon_box_id\":0,\"vip\":5},\"uid\":\"276447330\",\"the_luckiest\":true,\"sail_time\":1589990360,\"result\":1881268},{\"is_punished\":true,\"uid\":\"434309356633236077\",\"sail_time\":1589990360,\"result\":-4592163,\"sail_player\":{\"nickname\":\"20040630嘟嘟\",\"uid\":\"434309356633236077\",\"img_url\":\"\",\"gold\":5091533,\"route\":8,\"vip\":3}}]","create_time":1589990360,"stage":187459}');"""
val s1 = s.replace("\\","")
val s2 = s1.replace("/","")

Array(1,"sadkfj")
