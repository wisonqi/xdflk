package cn.com.bonc.entry.rule.faultStandby

/**
 * 待机故障判断规则信息存储类
 *
 * @param prodCode        模板编码
 * @param propCode        待判断测点编码
 * @param anotherPropCode 另一个测点编码
 * @param propValue       测点判断值
 * @param greaterThanZero 是否需要当前测点值大于0
 * @param constantType    是否为常量判断
 * @param useType         设备状态
 * @param judgeType       测点异常判断依据
 * @param rulePk          规则主键 _PK_
 * @param propName        测点名称
 * @author wzq
 * @date 2019-10-31
 **/
class FaultStandbyRule(
                          var prodCode: String,
                          var propCode: String,
                          var anotherPropCode: String,
                          var propValue: Double,
                          var greaterThanZero: Boolean,
                          var constantType: Boolean,
                          var useType: String,
                          var judgeType: String,
                          var rulePk: String,
                          var propName: String
                      ) {
  
  override def toString = s"FaultStandbyRule(prodCode=$prodCode, propCode=$propCode, anotherPropCode=$anotherPropCode, propValue=$propValue, greaterThanZero=$greaterThanZero, constantType=$constantType, useType=$useType, judgeType=$judgeType, rulePk=$rulePk, propName=$propName)"
}

object FaultStandbyRule {
  /**
   *
   * @param prodCode        模板编码
   * @param propCode        待判断测点编码
   * @param anotherPropCode 另一个测点编码
   * @param propValue       测点判断值
   * @param greaterThanZero 是否需要当前测点值大于0
   * @param constantType    是否为常量判断
   * @param useType         设备状态
   * @param judgeType       测点异常判断依据
   * @param rulePk          规则主键 _PK_
   * @param propName        测点名称
   * @return StandbyFaultRule对象
   */
  def apply(prodCode: String, propCode: String, anotherPropCode: String, propValue: Double, greaterThanZero: Boolean, constantType: Boolean, useType: String, judgeType: String, rulePk: String, propName: String): FaultStandbyRule = new FaultStandbyRule(prodCode, propCode, anotherPropCode, propValue, greaterThanZero, constantType, useType, judgeType, rulePk, propName)
}
