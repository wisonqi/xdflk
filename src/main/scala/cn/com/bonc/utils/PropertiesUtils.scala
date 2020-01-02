package cn.com.bonc.utils

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

/**
 * connect.properties配置文件处理类
 *
 * @author wzq
 * @date 2019-08-22
 **/
object PropertiesUtils extends Serializable {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val properties: Properties = new Properties()
  
  //加载配置文件，初始化properties对象
  properties.load(this.getClass.getClassLoader.getResourceAsStream("connect.properties"))
  logger.info("默认配置文件加载完毕。")
  private val sb = new StringBuilder()
  private val iter = properties.entrySet().iterator()
  while (iter.hasNext) {
    val entry = iter.next()
    sb.append("\t\t").append(entry.getKey).append("=").append(entry.getValue).append("\n")
  }
  logger.info("配置文件内容为：\n" + sb.toString())
  
  /**
   * 获取配置文件对应的properties对象
   *
   * @return 配置文件对应的properties对象
   */
  def getProperties: Properties = {
    properties
  }
  
  /**
   * 获取指定key对应的value值
   *
   * @param key 字符串类型的key值
   * @return key对应的value值，如果没有对应的key则返回null
   */
  def getValue(key: String): String = {
    val value = properties.get(key)
    if (value != null) {
      value.toString
    } else {
      null
    }
  }
  
  /**
   * 将新值设置到properties对象中
   *
   * @param key   非空key值
   * @param value 非null value值
   * @return 是否设置成功；如果有null或者key为空，则不允许设置
   */
  def setValue(key: String, value: String): Boolean = {
    if (key != null && !key.isEmpty && value != null) {
      properties.setProperty(key, value)
      true
    } else {
      false
    }
  }
  
}
