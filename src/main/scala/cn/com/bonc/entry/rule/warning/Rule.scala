package cn.com.bonc.entry.rule.warning

import scala.collection.mutable

/**
 * 报警数据存储实体类，主要用来存储一个模板编码、设备编码（可能没有）、测点编码下的报警规则数据
 *
 * @param isShow      是否显隐，该值直接赋给实时数据即可，如果没有该值，则将其赋值为0
 * @param isWarnPoint 报警优先级，该值直接赋给实时数据即可
 * @param onOrDown    是否启用，该值直接赋给实时数据即可
 * @param warnCode    报警编码
 * @param valueRules  值判断规则信息存储对象集合
 * @param timeRules   时长判断规则信息存储对象集合
 * @author wzq
 * @date 2019-09-05
 **/
class Rule(
              val isShow: String,
              val isWarnPoint: String,
              val onOrDown: String,
              val warnCode: String,
              val valueRules: mutable.ListBuffer[ValueRule],
              val timeRules: mutable.ListBuffer[TimeRule]
          ) extends Serializable {
  override def toString = s"Rule($isShow, $isWarnPoint, $onOrDown, $valueRules, $timeRules)"
}

object Rule {
  /**
   * 创建规则存储类对象
   *
   * @note 返回的结果对象中的值规则存储集合没有经过自然排序，时长规则存储集合已经经过自然排序
   * @param isShow      是否显隐，该值直接赋给实时数据即可，如果没有该值，则将其赋值为0
   * @param isWarnPoint 报警优先级，该值直接赋给实时数据即可
   * @param onOrDown    是否启用，该值直接赋给实时数据即可
   * @param warnCode    报警编码
   * @param valueRules  值判断规则信息存储对象集合
   * @param timeRules   时长判断规则信息存储对象集合
   * @return 规则存储类对象
   */
  def apply(isShow: String, isWarnPoint: String, onOrDown: String, warnCode: String, valueRules: mutable.ListBuffer[ValueRule], timeRules: mutable.ListBuffer[TimeRule]): Rule = {
    new Rule(isShow, isWarnPoint, onOrDown, warnCode, valueRules, timeRules.sortBy(_.warnTime))
  }
}
