package cn.com.bonc.entry.result.warning

import scala.beans.BeanProperty

/**
 * 报警信息存储总类
 *
 * @author wzq
 * @date 2019-09-06
 */
class WarningData() extends Serializable {
  
  /**
   * 报警类型（0值报警， 1时长报警， 2正常）
   */
  @BeanProperty
  var tag: Int = _
  /**
   * 模板编码
   */
  @BeanProperty
  var templateCode: String = _
  /**
   * 设备编码
   */
  @BeanProperty
  var deviceCode: String = _
  /**
   * 测点编码
   */
  @BeanProperty
  var pointCode: String = _
  /**
   * 测点名称
   */
  @BeanProperty
  var pointName: String = _
  /**
   * 报警优先级
   */
  @BeanProperty
  var isWarnPoint: String = _
  /**
   * 报警编码
   */
  @BeanProperty
  var warnCode: String = _
  /**
   * 测点数值
   */
  @BeanProperty
  var value: String = _
  /**
   * 报警时长
   */
  @BeanProperty
  var warnTime: Long = _
  /**
   * 开始时间
   */
  @BeanProperty
  var startTime: Long = _
  /**
   * 结束时间
   */
  @BeanProperty
  var endTime: Long = _
  /**
   * 值报警对象
   */
  @BeanProperty
  var valueWarn: ValueWarn = _
  /**
   * 时长报警对象
   */
  @BeanProperty
  var timeWarn: TimeWarn = _
  
  override def toString = s"WarningData(tag=$tag, templateCode=$templateCode, deviceCode=$deviceCode, pointCode=$pointCode, pointName=$pointName, isWarnPoint=$isWarnPoint, warnCode=$warnCode, value=$value, warnTime=$warnTime, startTime=$startTime, endTime=$endTime, valueWarn=$valueWarn, timeWarn=$timeWarn)"
}
