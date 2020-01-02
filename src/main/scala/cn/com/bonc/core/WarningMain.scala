package cn.com.bonc.core

import java.util

import cn.com.bonc.core.operation.flink.MainFlink
import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule
import cn.com.bonc.entry.rule.warning.Rule
import cn.com.bonc.utils.PropertiesUtils
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{BroadcastStream, ConnectedStreams}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import redis.clients.jedis.JedisCluster

/**
 * @author wzq
 * @date 2019-09-26
 **/
object WarningMain {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //将执行环境并行度设置为1，其他需要增大并行度的地方通过配置文件更改
    env.setParallelism(1)
    //设置失败自动重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.seconds(10)))
    
    //获取两个主题数据source
    val realDataSource: DataStream[Tuple2[String, String]] = MainFlink.getKafkaTuple2Source(env, PropertiesUtils.getValue("kafka.real.topic"), PropertiesUtils.getValue("kafka.real.group")).name("实时数据流").setParallelism(PropertiesUtils.getValue("flink.kafka.source.realData.parallel").toInt)
    val rulePropertySource: DataStream[Tuple2[String, String]] = MainFlink.getKafkaTuple2Source(env, PropertiesUtils.getValue("kafka.rule.property.topic"), PropertiesUtils.getValue("kafka.rule.property.topic")).name("规则配置数据流")
    
    //对规则配置数据进行处理并广播
    val rulePropertyBroadcast: BroadcastStream[Tuple2[String, Any]] = MainFlink.rulePropertyHandle(rulePropertySource)
    //过滤实时数据
    val filteredData: DataStream[Tuple2[String, String]] = MainFlink.filterData(realDataSource)
    //根据规则配置对过滤之后的实时数据进行处理
    val result: DataStream[(String, String)] = MainFlink.pointInfoAndRule(filteredData, rulePropertyBroadcast)

    //val connecttedStream:BroadcastConnectedStream[Tuple2[String,String],Tuple2[String,Any]] = filteredData.connect(rulePropertyBroadcast)


    filteredData.connect(rulePropertyBroadcast).process(new BroadcastProcessFunction[Tuple2[String,String],Tuple2[String,Any],(String,String)] {
      var jedis: JedisCluster = _
      var pointInfoJson: String = _
      var pointInfo: util.Map[String, String] = _
      var warningRuleJson: String = _
      var warningRule: util.Map[String, Rule] = _
      var faultStandbyRuleJson: String = _
      var faultStandbyRule: Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]] = _
      //key为设备编码，value为该设备所有的报警和故障待机数据
      val deviceDataMapCache: util.Map[String, util.Map[String, Object]] = new util.HashMap[String, util.Map[String, Object]]()
      private val realDataCounter = new LongCounter(0L)
      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("realDataCount", realDataCounter)
      }

      override def processElement(value: Tuple2[String, String], ctx: BroadcastProcessFunction[Tuple2[String, String], Tuple2[String, Any], (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = ???

      override def processBroadcastElement(value: Tuple2[String, Any], ctx: BroadcastProcessFunction[Tuple2[String, String], Tuple2[String, Any], (String, String)]#Context, out: Collector[(String, String)]): Unit = ???
    })
    //将结果写入kafka
    MainFlink.toKafkaSink(result, env)
    
    env.execute("处理时长统计")
  }
  
}
