package haofei.domain

import java.sql.Connection

trait DataSourceTrait{

  def getConnection(): Connection

}
