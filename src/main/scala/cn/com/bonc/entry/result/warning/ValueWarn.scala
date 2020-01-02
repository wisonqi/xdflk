package cn.com.bonc.entry.result.warning

import java.util

import scala.beans.BeanProperty

/**
 * 值报警信息存储类
 *
 * @author wzq
 * @date 2019-09-07
 **/
class ValueWarn() extends Serializable {
  /**
   * 报警编码
   */
  @BeanProperty
  var warnCode: String = _
  /**
   * 用来存储已经触发过的值报警编码
   */
  @BeanProperty
  var warnCodeSet: util.HashSet[String] = new util.HashSet[String]()
  /**
   * 报警类型
   */
  @BeanProperty
  var warnStyle: String = _
  /**
   * 推送人
   */
  @BeanProperty
  var sendToPeople: String = _
  /**
   * 角色
   */
  @BeanProperty
  var sendToRole: String = _
  /**
   * 部门
   */
  @BeanProperty
  var sendToDept: String = _
  
  override def toString = s"ValueWarn(warnCode=$warnCode, warnCodeSet=$warnCodeSet, warnStyle=$warnStyle, sendToPeople=$sendToPeople, sendToRole=$sendToRole, sendToDept=$sendToDept)"
}
