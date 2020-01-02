package cn.com.bonc.entry.result

import java.util

/**
 * 处理结果数据保存类
 *
 * @param tag                 用来标识处理的实时数据属于哪种类型：A、B、C
 * @param fullData            全量实时数据，json格式
 * @param warningData         报警数据集合
 * @param faultStandbyData    故障待机数据集合
 * @param deviceDataChangeMap 单个设备数据缓存更改之后的数据
 * @author wzq
 * @date 2019-09-06
 **/
class HandleResult(var tag: String, var fullData: String, var warningData: util.ArrayList[String], var faultStandbyData: util.Set[String], var deviceDataChangeMap: util.Map[String, String]) extends Serializable {
  override def toString = s"HandleResult(tag=$tag, fullData=$fullData, warningData=$warningData, faultStandbyData=$faultStandbyData, deviceDataChangeMap=$deviceDataChangeMap)"
}


object HandleResult {
  /**
   *
   * @param tag                 用来标识处理的实时数据属于哪种类型：A、B、C
   * @param fullData            全量实时数据，json格式
   * @param warningData         报警数据集合
   * @param faultStandbyData    故障待机数据集合
   * @param deviceDataChangeMap 设备数据缓存更改之后的数据，如果是删除，则只需要将value值设置为空字符串即可
   * @return 处理结果数据保存类
   */
  def apply(tag: String, fullData: String, warningData: util.ArrayList[String], faultStandbyData: util.Set[String], deviceDataChangeMap: util.Map[String, String]): HandleResult = new HandleResult(tag, fullData, warningData, faultStandbyData, deviceDataChangeMap)
}

