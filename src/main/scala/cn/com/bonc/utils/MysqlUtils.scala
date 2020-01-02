package cn.com.bonc.utils

import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.mysql.jdbc.Driver
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.flink.util.ExceptionUtils

/**
 * mysql处理相关
 *
 * @author wzq
 * @date 2019-10-11
 **/
object MysqlUtils {
  
  private val hikariConfig = new HikariConfig()
  hikariConfig.setDriverClassName(classOf[Driver].getName)
  hikariConfig.setJdbcUrl(PropertiesUtils.getValue("mysql.url"))
  hikariConfig.setUsername(PropertiesUtils.getValue("mysql.username"))
  hikariConfig.setPassword(PropertiesUtils.getValue("mysql.password"))
  hikariConfig.setMaximumPoolSize(150)
  val dataSource = new HikariDataSource(hikariConfig)
  
  /**
   * 从连接池中获取连接
   *
   * @return 连接
   */
  def getConnect: Connection = {
    dataSource.getConnection
  }
  
  /**
   * 将数据写入mysql数据库，需要保证数据一共有五个信息，第一个信息为一个long型数字，第二个信息字符长度不超过250
   *
   * @param errorLocate  错误位置
   * @param errorMessage 错误对象
   * @param dataType     数据类型
   * @param dataKey      数据key值
   * @param dataValue    数据value值
   */
  def insertInto(errorLocate: String, errorMessage: Any, dataType: String, dataKey: String, dataValue: String): Unit = {
    val connect = getConnect
    val sql = s"insert into ${PropertiesUtils.getValue("mysql.database")}.${PropertiesUtils.getValue("mysql.table")}" +
        s"(time, error_locate, error_message, data_type, data_key, data_value) " +
        s"values(?, ?, ?, ?, ?, ?);"
    val statement = connect.prepareStatement(sql)
    statement.setString(1, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
    statement.setString(2, errorLocate)
    val message: String = errorMessage match {
      case s: String => s
      case t: Throwable => ExceptionUtils.stringifyException(t)
    }
    statement.setString(3, if (message.length > 1000) message.substring(0, Math.min(1000, message.length) - 1) else message)
    statement.setString(4, if (dataType.length > 1000) dataType.substring(0, 999) else dataType)
    statement.setString(5, dataKey)
    statement.setString(6, if (dataValue.length > 1000) dataValue.substring(0, 999) else dataValue)
    statement.execute()
    statement.close()
    connect.close()
  }
  
}
