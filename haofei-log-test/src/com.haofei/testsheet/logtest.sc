
import scala.collection.mutable

val arrays = new mutable.ArrayBuffer[String]
arrays.append("hello")
arrays.append("world")
arrays.foreach(println)
arrays.clear()
arrays.size