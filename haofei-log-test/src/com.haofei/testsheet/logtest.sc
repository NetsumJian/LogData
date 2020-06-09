
import java.text.SimpleDateFormat
import java.util

import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

val sdf = new SimpleDateFormat("yyyy-MM-dd")
val today = sdf.format(System.currentTimeMillis())
println(today)