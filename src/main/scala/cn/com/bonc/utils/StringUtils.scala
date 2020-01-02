package cn.com.bonc.utils

/**
 * 对于字符串的一些处理
 *
 * @author wzq
 * @date 2019-09-15
 **/
object StringUtils {
  
  
  /**
   * 判断一个字符串是否为数字，要求字符串不为bull
   *
   * @param string 待判断的字符串
   * @return 字符串是否为数字
   */
  def isNumber(string: String): Boolean = {
    string.matches("^(\\-|\\+)?\\d+(\\.\\d+)?$")
  }
  
  /**
   * 截取小数点后两位
   *
   * @param string 待处理字符串
   * @return 截取小数点后两位字符串
   */
  def truncate(string: String): String = {
    if (string.contains('.') && string.indexOf('.') + 3 < string.length) {
      string.substring(0, string.indexOf('.') + 3)
    } else {
      string
    }
  }
  
  def main(args: Array[String]): Unit = {
    MysqlUtils.insertInto("type1", new NullPointerException, "realData1", "ruleData1", "pointInfo1")
  }
  
}
