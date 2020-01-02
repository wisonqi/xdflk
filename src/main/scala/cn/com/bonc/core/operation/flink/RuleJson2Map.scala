package cn.com.bonc.core.operation.flink

import java.util
import java.util.Objects

import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule
import cn.com.bonc.entry.rule.warning.{Rule, TimeRule, ValueRule}
import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.api.java.tuple.Tuple2

import scala.collection.mutable

/**
 * 将json格式的规则数据处理为map表
 *
 * @author wzq
 * @date 2019-09-07
 **/
object RuleJson2Map {
  
  /**
   * 处理json类型的报警数据，将其按照一定规则放到map表中
   *
   * @param json json类型的报警规则数据
   * @return map表，泛型为util.Map[String, Rule]
   */
  def WarningJsonHandle(json: String): util.Map[String, Rule] = {
    val result = new util.HashMap[String, Rule]()
    val iter: util.Iterator[AnyRef] = JSON.parseArray(json).iterator()
    while (iter.hasNext) {
      val elem = JSON.parseObject(iter.next().toString)
      val valueRules: mutable.ListBuffer[ValueRule] = mutable.ListBuffer[ValueRule]()
      val timeRules: mutable.ListBuffer[TimeRule] = mutable.ListBuffer[TimeRule]()
      val alValue: JSONArray = elem.getJSONArray("AL_VALUE")
      val alTime: JSONArray = elem.getJSONArray("AL_TIME")
      //处理值报警类型规则数据
      if (alValue != null) {
        val iter = alValue.iterator()
        while (iter.hasNext) {
          val elem = JSON.parseObject(iter.next().toString)
          valueRules.append(ValueRule(
            if (Objects.equals(elem.getString("VALUE_MIN"), "")) null else elem.getDoubleValue("VALUE_MIN"),
            if (Objects.equals(elem.getString("VALUE_MAX"), "")) null else elem.getDoubleValue("VALUE_MAX"),
            elem.getString("VALUE_CODE"),
            elem.getString("WARN_STYLE"),
            elem.getString("SEND_TO_PEOPLE"),
            elem.getString("SEND_TO_ROLE"),
            elem.getString("SEND_TO_DEPT")
          ))
        }
      }
      //处理时长报警类型规则数据
      if (alTime != null) {
        val iter = alTime.iterator()
        while (iter.hasNext) {
          val elem = JSON.parseObject(iter.next().toString)
          timeRules.append(TimeRule(
            (elem.getDoubleValue("WARN_TIME") * 3600000).toLong,
            elem.getString("WARN_TIME_CODE"),
            elem.getString("SEND_TO_PEOPLE"),
            elem.getString("SEND_TO_ROLE"),
            elem.getString("SEND_TO_CAPTAIN"),
            elem.getString("SEND_TO_DEPT")
          ))
        }
      }
      
      //处理公共数据，包括模板编码、设备编码、测点编码、测点名称、是否显隐、报警优先级、是否启用
      val templateCode: String = if (elem.getString("PRODNAME") != null) elem.getString("PRODNAME") else ""
      val deviceCode: String = if (elem.getString("DEVICECODE") != null) elem.getString("DEVICECODE") else ""
      val pointCode: String = if (elem.getString("POINTCODE") != null) elem.getString("POINTCODE") else ""
      result.put(templateCode + "~" + deviceCode + "~" + pointCode,
        Rule(
          if (elem.getString("ISSHOW") != null && !Objects.equals(elem.getString("ISSHOW"), "")) elem.getString("ISSHOW") else "0",
          if (elem.getString("IS_WARN_POINT") != null) elem.getString("IS_WARN_POINT") else "",
          if (elem.getString("ON_OR_DOWN") != null) elem.getString("ON_OR_DOWN") else "",
          if (elem.getString("WARN_CODE") != null) elem.getString("WARN_CODE") else "",
          valueRules,
          timeRules))
    }
    result
  }
  
  /**
   *
   * 处理json类型的故障待机判断规则数据，将其按照一定规则放到map表中
   *
   * @param json json类型的故障待机判断规则数据
   * @return map表，key类型为String，value类型为util.List[util.Map[String, FaultStandbyRule]<br>
   *         “FAULT” 对应的为故障判断规则，“STANDBY” 对应的为待机判断规则
   */
  def faultStandbyJsonHandle(json: String): Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]] = {
    val result: Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]] = new Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]]()
    //保存所有的模板编码，方便算子过滤数据，返回值二元组的第一个元素
    val prodCodeSet: util.Set[String] = new util.HashSet[String]()
    //外部map，用来保存故障和待机判断规则，通过key值类区分，返回值二元组的第二个元素
    val outerMap: util.HashMap[String, util.Map[String, util.List[FaultStandbyRule]]] = new util.HashMap[String, util.Map[String, util.List[FaultStandbyRule]]]()
    //内部map，用来保存故障或者是待机判断的具体规则，key值为模板编码，由于模板对应的判断规则可能不止一个，因此value值类型为list集合，用来保存所有的判断规则
    val faultMap: util.HashMap[String, util.List[FaultStandbyRule]] = new util.HashMap[String, util.List[FaultStandbyRule]]()
    val standbyMap: util.HashMap[String, util.List[FaultStandbyRule]] = new util.HashMap[String, util.List[FaultStandbyRule]]()
    val iter = JSON.parseArray(json).iterator()
    while (iter.hasNext) {
      val next: util.HashMap[String, String] = JSON.parseObject(iter.next().toString, classOf[util.HashMap[String, String]])
      val prodCode = next.get("PRODCODE")
      val propCode = next.get("PROPCODE")
      val propValue = next.get("PROPVALUE")
      val rulePk = next.get("_PK_")
      val greaterThanZero = next.get("GREATER_THAN_ZERO")
      val constantType = if (Objects.equals(next.get("CONSTANT_TYPE"), "CONSTANT")) true else false
      val propName = next.get("PROPNAME")
      val useType = next.get("USE_TYPE")
      val judgeType = next.get("JUDGE_TYPE")
      prodCodeSet.add(prodCode)
      var standbyFaultRule: FaultStandbyRule = null
      if (constantType) {
        //常量判断
        standbyFaultRule = FaultStandbyRule(prodCode, propCode, "", propValue.toDouble, greaterThanZero = false, constantType = true, useType, judgeType, rulePk, propName)
      } else {
        //变量判断
        standbyFaultRule = FaultStandbyRule(prodCode, propCode, propValue, 0, if (Objects.equals(greaterThanZero, "1")) true else false, constantType = false, useType, judgeType, rulePk, propName)
      }
      if (useType.contains("FAULT")) {
        //故障判断规则
        val rules = faultMap.getOrDefault(prodCode, new util.ArrayList[FaultStandbyRule]())
        rules.add(standbyFaultRule)
        faultMap.put(prodCode, rules)
      } else if (useType.contains("STANDBY")) {
        //待机判断规则
        val rules = standbyMap.getOrDefault(prodCode, new util.ArrayList[FaultStandbyRule]())
        rules.add(standbyFaultRule)
        standbyMap.put(prodCode, rules)
      } else {
        throw new RuntimeException("故障待机判断规则的USE_TYPE值不在规定范围之内")
      }
    }
    outerMap.put("FAULT", faultMap)
    outerMap.put("STANDBY", standbyMap)
    result.setFields(prodCodeSet, outerMap)
    result
  }
  
}
