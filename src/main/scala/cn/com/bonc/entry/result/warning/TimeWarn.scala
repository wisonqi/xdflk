package cn.com.bonc.entry.result.warning

import scala.beans.BeanProperty

/**
 * 时长报警信息存储类
 *
 * @author wzq
 * @date 2019-09-07
 **/
class TimeWarn() extends Serializable {
  /**
   * 报警编码
   */
  @BeanProperty
  var warnTimeCode: String = _
  /**
   * 推送人
   */
  @BeanProperty
  var sendToPeople: String = _
  /**
   * 推送角色
   */
  @BeanProperty
  var sendToRole: String = _
  /**
   * 推送领导
   */
  @BeanProperty
  var sendToCaptain: String = _
  /**
   * 部门
   */
  @BeanProperty
  var sendToDept: String = _
  
  override def toString = s"TimeWarn(warnTimeCode=$warnTimeCode, sendToPeople=$sendToPeople, sendToRole=$sendToRole, sendToCaptain=$sendToCaptain, sendToDept=$sendToDept)"
}
