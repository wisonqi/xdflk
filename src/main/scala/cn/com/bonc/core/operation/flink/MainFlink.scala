package cn.com.bonc.core.operation.flink

import java.net.InetSocketAddress
import java.util
import java.util.regex.Pattern
import java.util.{Objects, Properties}

import cn.com.bonc.core.operation.flink.kafka.connector.KeyedKafkaSink
import cn.com.bonc.core.operation.flink.kafka.serialization.KafkaTuple2DeserializationSchema
import cn.com.bonc.core.operation.flink.mapper.RedisSetMapper
import cn.com.bonc.core.operation.handle.RuleHandle
import cn.com.bonc.entry.result.faultStandby.FaultStandbyData
import cn.com.bonc.entry.result.warning.WarningData
import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule
import cn.com.bonc.entry.rule.warning.Rule
import cn.com.bonc.utils.{MysqlUtils, PropertiesUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * 处理报警数据启动类<br>
 *
 * @author wzq
 * @date 2019-09-05
 **/
object MainFlink {
  
  
  /**
   * 获取key-value类型的kafka数据source
   *
   * @param env   流处理执行环境
   * @param topic 主题名
   * @return 泛型为二元组类型的数据流，二元组内元素分别为kafka中的key-message
   */
  def getKafkaTuple2Source(env: StreamExecutionEnvironment, topic: String, groupId: String): DataStream[Tuple2[String, String]] = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    env.addSource(
      new FlinkKafkaConsumer[Tuple2[String, String]](
        topic,
        //自定义的KeyedDeserializationSchema，将kafka中的key-message封装到二元组中
        new KafkaTuple2DeserializationSchema,
        properties
      )
    )
  }
  
  /**
   * 获取只读取message数据的kafka source
   *
   * @param env     流处理执行环境
   * @param topic   主题名
   * @param groupId 消费者组名
   * @return 泛型为字符串类型的数据流，内容为kafka中的message
   */
  def getKafkaMessageSource(env: StreamExecutionEnvironment, topic: String, groupId: String): DataStream[String] = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    env.addSource(
      new FlinkKafkaConsumer[String](
        topic,
        new SimpleStringSchema(),
        properties
      )
    )
  }
  
  /**
   * 获取处理完生成的value类型数据的kafka sink
   *
   * @param env   流处理执行环境
   * @param topic kafka主题
   * @return 处理完生成的报警数据的kafka sink
   */
  def getKafkaMessageSink(env: StreamExecutionEnvironment, topic: String): FlinkKafkaProducer[String] = {
    val sinkProps = new Properties()
    sinkProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
    sinkProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, PropertiesUtils.getValue("flink.kafka.transaction.timeout"))
    sinkProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), sinkProps)
  }
  
  /**
   * 获取写入数据到redis的sink
   *
   * @param env 流处理执行环境
   * @return 写入数据到redis的sink
   */
  def getRedisClusterSink(env: StreamExecutionEnvironment): RedisSink[(String, String)] = {
    val nodes = new util.HashSet[InetSocketAddress]()
    val hosts: Array[String] = PropertiesUtils.getValue("redis.host").split(',')
    for (host <- hosts) {
      val ipPort = host.split(':')
      nodes.add(new InetSocketAddress(ipPort(0), ipPort(1).toInt))
    }
    val config = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build()
    new RedisSink[(String, String)](config, new RedisSetMapper)
  }
  
  /**
   * 处理规则配置数据，将value数据进行类型转换之后广播出去
   *
   * @param ruleProperty 规则配置数据流
   */
  def rulePropertyHandle(ruleProperty: DataStream[Tuple2[String, String]]): BroadcastStream[Tuple2[String, Any]] = {
    ruleProperty
        .process(new ProcessFunction[Tuple2[String, String], Tuple2[String, Any]] {
          override def processElement(value: Tuple2[String, String], ctx: ProcessFunction[Tuple2[String, String], Tuple2[String, Any]]#Context, out: Collector[Tuple2[String, Any]]): Unit = {
            
            val key = value.f0
            val values = value.f1
            if (Objects.equals(key, "warning_rule")) {
              try {
                out.collect(new Tuple2[String, Any](key, RuleJson2Map.WarningJsonHandle(values)))
              } catch {
                case t: Throwable => MysqlUtils.insertInto("报警规则value值json转对象", t, "规则配置数据", key, values)
              }
            } else if (Objects.equals(key, "point_info")) {
              try {
                out.collect(new Tuple2[String, Any](key, JSON.parseObject(values, classOf[util.HashMap[String, String]])))
              } catch {
                case t: Throwable => MysqlUtils.insertInto("测点信息value值json转对象", t, "规则配置数据", key, values)
              }
            } else if (Objects.equals(key, "standby_fault_info")) {
              try {
                out.collect(new Tuple2[String, Any](key, RuleJson2Map.faultStandbyJsonHandle(values)))
              } catch {
                case t: Throwable => MysqlUtils.insertInto("待机故障判断value值json转对象", t, "规则配置数据", key, values)
              }
            } else {
              MysqlUtils.insertInto("key值不在规定范围之内", "", "规则配置数据", key, values)
            }
          }
        })
        .name("规则配置广播流")
        .broadcast(new MapStateDescriptor[String, Tuple2[String, Any]]("flink_handle_rule", TypeInformation.of(new TypeHint[String] {}), TypeInformation.of(new TypeHint[Tuple2[String, Any]] {})))
  }
  
  def filterData(realDataSource: DataStream[Tuple2[String, String]]): DataStream[Tuple2[String, String]] = {
    realDataSource
        .filter(new FilterFunction[Tuple2[String, String]] {
          //处理时间间隔
          val handleDuration: Long = PropertiesUtils.getValue("flink.kafka.source.realData.handle").toLong
          //丢弃时间间隔
          val discardDuration: Long = PropertiesUtils.getValue("flink.kafka.source.realData.discard").toLong
          //当前对比时间点，用于判断新来的数据处于哪个时间段之内
          var timing: Long = System.currentTimeMillis()
          //表示，用来判断对比时间点之后的数据是处理还是丢弃，true表示处理，false表示丢弃
          var handle: Boolean = true
          
          override def filter(value: Tuple2[String, String]): Boolean = {
            val currentTime = System.currentTimeMillis()
            val currentDuration = currentTime - timing
            if (handle) {
              if (currentDuration <= handleDuration) {
                true
              } else {
                timing = currentTime
                handle = false
                false
              }
            } else {
              if (currentDuration <= discardDuration) {
                false
              } else {
                timing = currentTime
                handle = true
                true
              }
            }
          }
        })
        .setParallelism(PropertiesUtils.getValue("flink.kafka.source.realData.parallel").toInt)
        .name("过滤部分数据")
        .partitionCustom(new CustomPartitioner, _.f0.split('/')(2))
        .filter(new FilterFunction[Tuple2[String, String]] {
          val pattern: Pattern = Pattern.compile("\"time\":\"(\\d+)\"")
          val deviceTime: mutable.HashMap[String, Long] = mutable.HashMap()
          
          override def filter(value: Tuple2[String, String]): Boolean = {
            val key = value.f0
            val matcher = pattern.matcher(value.f1)
            if (matcher.find()) {
              val realTime: Long = matcher.group(1).toLong
              if (deviceTime.contains(key)) {
                //表中包含该设备最新时间
                if (realTime >= deviceTime.getOrElse(key, 0L)) {
                  //实际数据时间正常
                  deviceTime.put(key, realTime)
                  true
                } else {
                  //实际数据时间延迟
                  false
                }
              } else {
                //表中不包含该设备最新时间
                deviceTime.put(key, realTime)
                true
              }
            } else {
              //实际数据中没有提取到time，则认为是研究院计算出的数据，全部放行
              true
            }
          }
        })
        .setParallelism(PropertiesUtils.getValue("flink.kafka.rule.handle.process.parallel").toInt)
        .name("过滤延迟数据")
  }
  
  //报警数据侧边流标记
  private val warningSide = new OutputTag[String]("warning")
  //故障待机数据侧边流标记
  private val faultStandbySide = new OutputTag[String]("faultStandbySide")
  //全量a类型侧边流标记
  private val fullDataASide = new OutputTag[String]("fullDataASide")
  //全量b类型侧边流标记
  private val fullDataBSide = new OutputTag[(String, String)]("fullDataBSide")
  //需要写入redis的数据
  private val redisDataSink = new OutputTag[(String, String)]("redisDataSide")
  
  /**
   * 对实时数据进行所有规则处理
   *
   * @param filteredData          实时数据流
   * @param rulePropertyBroadcast 已经处理完规则配置的广播流
   */
  def pointInfoAndRule(filteredData: DataStream[Tuple2[String, String]], rulePropertyBroadcast: BroadcastStream[Tuple2[String, Any]]): DataStream[(String, String)] = {
    filteredData
        .connect(rulePropertyBroadcast)
        .process(function = new BroadcastProcessFunction[Tuple2[String, String], Tuple2[String, Any], (String, String)] {
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
            //0. 注册累加器
            getRuntimeContext.addAccumulator("realDataCount", realDataCounter)
            //1. 初始化设备报警故障待机数据缓存
            val poolConfig = new JedisPoolConfig()
            poolConfig.setBlockWhenExhausted(true)
            poolConfig.setTestOnBorrow(true)
            poolConfig.setTestOnCreate(true)
            poolConfig.setMaxTotal(Int.MaxValue)
            val nodes = new util.HashSet[HostAndPort]()
            val hosts: Array[String] = PropertiesUtils.getValue("redis.host").split(',')
            for (host <- hosts) {
              val ipPort = host.split(':')
              nodes.add(new HostAndPort(ipPort(0), ipPort(1).toInt))
            }
            jedis = new JedisCluster(nodes, poolConfig)
            val jedisKeys: util.Set[String] = new util.HashSet[String]()
            val iter = jedis.getClusterNodes.entrySet().iterator()
            while (iter.hasNext) {
              jedisKeys.addAll(iter.next().getValue.getResource.keys("*"))
            }
            val jedisKeysIter = jedisKeys.iterator()
            //操作 redis
            while (jedisKeysIter.hasNext) {
              var deviceCode = jedisKeysIter.next()
              if (deviceCode.contains("flink-")) {
                val deviceData = JSON.parseObject(jedis.get(deviceCode))
                val deviceDataKeyIter: util.Iterator[String] = deviceData.keySet().iterator()
                deviceCode = deviceCode.replace("flink-", "")
                var deviceDataMap = deviceDataMapCache.get(deviceCode)
                if (deviceDataMap == null) {
                  deviceDataMap = new util.HashMap[String, Object]()
                  deviceDataMapCache.put(deviceCode, deviceDataMap)
                }
                while (deviceDataKeyIter.hasNext) {
                  val key: String = deviceDataKeyIter.next()
                  if (key.contains('~')) {
                    //如果key中包含字符 ~ ，表示这个数据为报警数据，否则为故障待机数据
                    deviceDataMap.put(key, deviceData.getObject(key, classOf[WarningData]))
                  } else {
                    deviceDataMap.put(key, deviceData.getObject(key, classOf[FaultStandbyData]))
                  }
                }
              }
            }
            jedis.close()
            //2. 从最开始为止读取规则配置信息主题中的所有数据，以初始化本地变量
            val pointInfoProps = new Properties()
            pointInfoProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
            pointInfoProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
            pointInfoProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
            pointInfoProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000")
            val consumer = new KafkaConsumer[String, String](pointInfoProps)
            val partitionInfoIter = consumer.partitionsFor(PropertiesUtils.getValue("kafka.rule.property.topic")).iterator()
            val topicPartitions = new util.ArrayList[TopicPartition]()
            while (partitionInfoIter.hasNext) {
              val next = partitionInfoIter.next
              topicPartitions.add(new TopicPartition(next.topic(), next.partition()))
            }
            consumer.assign(topicPartitions)
            val endOffsetIter = consumer.endOffsets(topicPartitions).entrySet().iterator()
            val beginOffsetIter = consumer.beginningOffsets(topicPartitions).entrySet().iterator()
            var endOffset: Long = 0
            var currentOffset: Long = 0
            while (endOffsetIter.hasNext) {
              endOffset = endOffsetIter.next().getValue
            }
            while (beginOffsetIter.hasNext) {
              val next = beginOffsetIter.next()
              consumer.seek(next.getKey, next.getValue)
            }
            //一直消费数据，直到消费完上面获取到的最后的offset，以用于更新本地变量
            Breaks.breakable(
              while (true) {
                val recordsIter = consumer.poll(200).iterator()
                while (recordsIter.hasNext) {
                  val next = recordsIter.next()
                  currentOffset = next.offset()
                  val key = next.key()
                  val value = next.value()
                  
                  if (Objects.equals(key, "warning_rule")) {
                    warningRuleJson = value
                  } else if (Objects.equals(key, "point_info")) {
                    pointInfoJson = value
                  } else if (Objects.equals(key, "standby_fault_info")) {
                    faultStandbyRuleJson = value
                  }
                }
                if (currentOffset == endOffset - 1) {
                  Breaks.break()
                }
              }
            )
            consumer.close()
            if (pointInfoJson != null && warningRuleJson != null && faultStandbyRuleJson != null) {
              val result = dataTypeTransform(pointInfoJson, warningRuleJson, faultStandbyRuleJson)
              pointInfo = result._1
              warningRule = result._2
              faultStandbyRule = result._3
            } else {
              val list: ListBuffer[String] = ListBuffer()
              if (pointInfoJson == null) {
                list += "测点信息"
              }
              if (warningRuleJson == null) {
                list += "报警规则"
              }
              if (faultStandbyRuleJson == null) {
                list += "待机故障信息"
              }
              val errorMessage: StringBuilder = new StringBuilder("规则配置主题中缺少")
              for (item <- list) {
                errorMessage.append(item).append("、")
              }
              errorMessage.deleteCharAt(errorMessage.length - 1).append("数据")
              MysqlUtils.insertInto("初始化规则配置数据", errorMessage.toString(), "规则配置数据", "", "")
            }
          }
          
          /**
           * 对规则配置中获取到的json字符串统一进行对象转换
           *
           * @return 三元组，对象内容分别为测点信息、报警规则信息、待机故障信息
           */
          private def dataTypeTransform(pointInfoJson: String, warningRuleJson: String, faultStandbyJson: String): (util.Map[String, String], util.Map[String, Rule], Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]]) = {
            var pointInfo: util.Map[String, String] = null
            var warningRule: util.Map[String, Rule] = null
            var standbyFault: Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]] = null
            try {
              pointInfo = JSON.parseObject(pointInfoJson, classOf[util.HashMap[String, String]])
            } catch {
              case e: Throwable => MysqlUtils.insertInto("测点信息value值json转对象", e, "规则配置数据", "point_info", pointInfoJson)
            }
            try {
              warningRule = RuleJson2Map.WarningJsonHandle(warningRuleJson)
            } catch {
              case e: Throwable => MysqlUtils.insertInto("报警规则value值json转对象", e, "规则配置数据", "warning_rule", warningRuleJson)
            }
            try {
              standbyFault = RuleJson2Map.faultStandbyJsonHandle(faultStandbyJson)
            } catch {
              case e: Throwable => MysqlUtils.insertInto("待机故障判断value值json转对象", e, "规则配置数据", "standby_fault_info", faultStandbyRuleJson)
            }
            (pointInfo, warningRule, standbyFault)
          }
          
          override def processElement(value: Tuple2[String, String], ctx: BroadcastProcessFunction[Tuple2[String, String], Tuple2[String, Any], (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = {
            val key: String = value.f0
            val data: String = value.f1
            val keys = key.split("/")
            if (keys.length != 3) { //遇到异常数据时，直接忽略
              MysqlUtils.insertInto("规则判断处理", "实时数据key值信息有误", "实时数据", key, data)
            }
            //1. 处理测点名称
            var addPointNameValue: JSONObject = null
            if (pointInfo != null) {
              try {
                addPointNameValue = RuleHandle.pointNameHandle(pointInfo, key, data)
              } catch {
                case e: Throwable => MysqlUtils.insertInto("测点名称处理", e, "实时数据", key, data)
              }
            }
            //2. 处理报警和故障待机判断
            if (warningRule != null && addPointNameValue != null) {
              try {
                //从保存了所有设备的缓存中取出当前设备的缓存数据，如果没有，则说明是第一次遇到这个设备，应该新建一个表，并设置到整个map缓存中
                var deviceDataMap: util.Map[String, Object] = deviceDataMapCache.get(keys(2))
                if (deviceDataMap == null) {
                  deviceDataMap = new util.HashMap[String, Object]()
                  deviceDataMapCache.put(keys(2), deviceDataMap)
                }
                val handleResult = RuleHandle.ruleHandle(deviceDataMap, warningRule, keys, addPointNameValue, faultStandbyRule)
                if (Objects.equals(handleResult.tag, "C")) {
                  //主流中存放处理之后的C类型数据
                  out.collect((key, handleResult.fullData))
                } else if (Objects.equals(handleResult.tag, "A")) {
                  //该侧边流存放处理之后的A类型数据
                  ctx.output(fullDataASide, handleResult.fullData)
                } else if (Objects.equals(handleResult.tag, "B")) {
                  //该侧边流存放处理之后的B类型数据
                  ctx.output(fullDataBSide, (key, handleResult.fullData))
                }
                val iter: util.Iterator[String] = handleResult.warningData.iterator()
                while (iter.hasNext) {
                  //该侧边流存放报警数据
                  ctx.output(warningSide, iter.next())
                }
                val faultStandbyDataIter = handleResult.faultStandbyData.iterator()
                while (faultStandbyDataIter.hasNext) {
                  //该侧边流存放故障待机判断数据
                  ctx.output(faultStandbySide, faultStandbyDataIter.next())
                }
                val deviceDataChangeMap = handleResult.deviceDataChangeMap
                if (deviceDataChangeMap != null) {
                  val iter = deviceDataChangeMap.entrySet().iterator()
                  while (iter.hasNext) {
                    val next = iter.next()
                    //该侧边流存放缓存更改数据
                    ctx.output(redisDataSink, ("flink-" + next.getKey, next.getValue))
                  }
                }
                realDataCounter.add(1L)
              } catch {
                case e: Throwable => MysqlUtils.insertInto("规则判断处理", e, "实时数据", key, data)
              }
            }
          }
          
          override def processBroadcastElement(value: Tuple2[String, Any], ctx: BroadcastProcessFunction[Tuple2[String, String], Tuple2[String, Any], (String, String)]#Context, out: Collector[(String, String)]): Unit = {
            val key = value.f0
            val values = value.f1
            if (Objects.equals("warning_rule", key)) {
              warningRule = values.asInstanceOf[util.Map[String, Rule]]
            } else if (Objects.equals("point_info", key)) {
              pointInfo = values.asInstanceOf[util.Map[String, String]]
            } else if (Objects.equals("standby_fault_info", key)) {
              faultStandbyRule = values.asInstanceOf[Tuple2[util.Set[String], util.Map[String, util.Map[String, util.List[FaultStandbyRule]]]]]
            }
          }
        })
        .setParallelism(PropertiesUtils.getValue("flink.kafka.rule.handle.process.parallel").toInt)
        .name("处理实时数据")
  }
  
  
  /**
   * 将数据写入kafka
   *
   * @param result 结果流
   * @param env    流执行环境
   */
  def toKafkaSink(result: DataStream[(String, String)], env: StreamExecutionEnvironment): Unit = {
    val fullDataSinkC: KeyedKafkaSink = new KeyedKafkaSink(PropertiesUtils.getValue("kafka.server"), PropertiesUtils.getValue("kafka.full.c.topic"))
    val fullDataSinkB: KeyedKafkaSink = new KeyedKafkaSink(PropertiesUtils.getValue("kafka.server"), PropertiesUtils.getValue("kafka.full.b.topic"))
    val fullDataSinkA: FlinkKafkaProducer[String] = getKafkaMessageSink(env, PropertiesUtils.getValue("kafka.full.a.topic"))
    val warningDataSink: FlinkKafkaProducer[String] = getKafkaMessageSink(env, PropertiesUtils.getValue("kafka.warning.topic"))
    val faultStandbySink: FlinkKafkaProducer[String] = getKafkaMessageSink(env, PropertiesUtils.getValue("kafka.fault.standby.topic"))
    val redisSink: RedisSink[(String, String)] = getRedisClusterSink(env)
    val durationCountSink = getKafkaMessageSink(env, "duration_count")
    //处理主流中的全量数据
    result.addSink(fullDataSinkC).setParallelism(PropertiesUtils.getValue("flink.kafka.full.handle.data.sink.parallel").toInt).name("全量C类型数据")
    //处理A类型数据侧边流中的数据
    result
        .getSideOutput(fullDataASide)
        .addSink(fullDataSinkA)
        .name("全量A类型数据")
    //处理B类型数据侧边流中的数据
    result
        .getSideOutput(fullDataBSide)
        .addSink(fullDataSinkB)
        .name("全量B类型数据")
    //处理报警侧边流中的数据
    result
        .getSideOutput(warningSide)
        .addSink(warningDataSink)
        .name("报警数据")
    //处理故障待机侧边流中的数据
    result.getSideOutput(faultStandbySide)
        .addSink(faultStandbySink)
        .name("故障待机数据")
    //处理缓存更改侧边流中的数据
    result.getSideOutput(redisDataSink)
        .addSink(redisSink)
        .setParallelism(PropertiesUtils.getValue("flink.kafka.full.handle.data.sink.parallel").toInt)
        .name("缓存更改数据")
  }
  
}
