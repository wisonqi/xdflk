package cn.com.bonc.entry.rule.warning

/**
 * 时长报警判断数据存储实体类
 *
 * @param warnTime      时长，小时
 * @param warnTimeCode  时长报警编码
 * @param sendToPeople  推送人
 * @param sendToRole    推送角色
 * @param sendToCaptain 推送领导
 * @param sendToDept    部门
 * @author wzq
 * @date 2019-09-05
 **/
class TimeRule(val warnTime: Long, val warnTimeCode: String, val sendToPeople: String, val sendToRole: String, val sendToCaptain: String, val sendToDept: String) extends Serializable {
  override def toString = s"TimeRule(warnTime=$warnTime, warnTimeCode=$warnTimeCode, sendToPeople=$sendToPeople, sendToRole=$sendToRole, sendToCaptain=$sendToCaptain, sendToDept=$sendToDept)"
}

object TimeRule {
  /**
   *
   * @param warnTime      时长，小时
   * @param warnTimeCode  时长报警编码
   * @param sendToPeople  推送人
   * @param sendToRole    推送角色
   * @param sendToCaptain 推送领导
   * @param sendToDept    部门
   * @return
   */
  def apply(warnTime: Long, warnTimeCode: String, sendToPeople: String, sendToRole: String, sendToCaptain: String, sendToDept: String): TimeRule = new TimeRule(warnTime, warnTimeCode, sendToPeople, sendToRole, sendToCaptain, sendToDept)
}