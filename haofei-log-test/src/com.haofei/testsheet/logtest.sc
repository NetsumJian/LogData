import com.haofei.utils.EmailUtil

try {
  val i = 1 / 0
} catch {
  case  e =>{
    EmailUtil.sendSimpleTextEmail("test",s"${e.getStackTrace.mkString("\n")}")
  }
}