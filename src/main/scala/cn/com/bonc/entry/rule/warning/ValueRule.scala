package cn.com.bonc.entry.rule.warning

import java.lang

/**
 * 值报警判断数据存储实体类
 *
 * @param value_min    下限
 * @param value_max    上限
 * @param warnCode     报警编码
 * @param warnStyle    报警类型
 * @param sendToPeople 推送人员
 * @param sendRole     角色
 * @param sendToDept   部门
 * @author wzq
 * @date 2019-09-05
 **/
class ValueRule(
                   val value_min: lang.Double,
                   val value_max: lang.Double,
                   val warnCode: String,
                   val warnStyle: String,
                   val sendToPeople: String,
                   val sendRole: String,
                   val sendToDept: String
               ) extends Serializable {
  override def toString = s"ValueRule(value_min=$value_min, value_max=$value_max, warnCode=$warnCode, warnStyle=$warnStyle, sendToPeople=$sendToPeople, sendRole=$sendRole, sendToDept=$sendToDept)"
}

object ValueRule {
  /**
   *
   * @param value_min    下限
   * @param value_max    上限
   * @param warnCode     报警编码
   * @param warnStyle    报警类型
   * @param sendToPeople 推送人员
   * @param sendRole     角色
   * @param sendToDept   部门
   * @return
   */
  def apply(value_min: lang.Double, value_max: lang.Double, warnCode: String, warnStyle: String, sendToPeople: String, sendRole: String, sendToDept: String): ValueRule = new ValueRule(value_min, value_max, warnCode, warnStyle, sendToPeople, sendRole, sendToDept)
}