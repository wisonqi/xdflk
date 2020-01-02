package cn.com.bonc

import java.util.regex.Pattern
import java.util.{Collections, Properties, UUID}
import java.{lang, util}

import cn.com.bonc.core.operation.flink.RuleJson2Map
import cn.com.bonc.entry.result.warning.WarningData
import cn.com.bonc.entry.rule.warning.Rule
import cn.com.bonc.utils.{MysqlUtils, PropertiesUtils, StringUtils}
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema
import org.apache.flink.util.ExceptionUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.Test
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.mutable
import scala.util.Random

/**
 *
 * @author wzq
 * @date 2019-09-05
 **/
class AppTestScala {
  
  @Test
  def test1() {
    println(classOf[StringDeserializer].getName)
    println(classOf[StringDeserializer].getCanonicalName)
  }
  
  /**
   * 测试json字符串类型的报警规则数据转换结果
   */
  @Test
  def test2() {
    val ruleData: String = "[{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-05 04:00:00:000\",\"AL_VALUE\": [],\"POINTCODE\": \"DB_ATTR_01\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"XAgcy1h8wwPZCIda\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"XAgcy1h8wwPZCIda\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"1\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 9,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_01\"},{\"PRODTYPE\": \"2\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-05 09:41:36:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"1600\",\"VALUE_MIN\": \"-100000000\",\"VALUE_CODE\": \"17w8JQ1Dh2kFlXpozqIpub\",\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"_ROWNUM_\": 0,\"_PK_\": \"17w8JQ1Dh2kFlXpozqIpub\",\"WARN_STYLE\": \"2\",\"WARN_TYPE\": \"1\",\"SEND_TO_PEOPLE\": \"王富贵\",\"SEND_rule\": \"RPUB1\",\"VALUE_NAME\": \"A段值域\",\"SEND_TO_DEPT\": \"99\"},{\"VALUE_MAX\": \"1600\",\"VALUE_MIN\": \"1\",\"VALUE_CODE\": \"1ocPfRE5x8Ga4rqEBzxYwW\",\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"_ROWNUM_\": 1,\"_PK_\": \"1ocPfRE5x8Ga4rqEBzxYwW\",\"WARN_STYLE\": \"3\",\"WARN_TYPE\": \"1\",\"SEND_TO_PEOPLE\": \"何富贵\",\"SEND_rule\": \"RPUB1\",\"VALUE_NAME\": \"c段值域\",\"SEND_TO_DEPT\": \"99\"},{\"VALUE_MAX\": \"1600\",\"VALUE_MIN\": \"1\",\"VALUE_CODE\": \"3UP7WLlWtfZpH1mn3G191r\",\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"_ROWNUM_\": 2,\"_PK_\": \"3UP7WLlWtfZpH1mn3G191r\",\"WARN_STYLE\": \"1\",\"WARN_TYPE\": \"1\",\"SEND_TO_PEOPLE\": \"朱富贵\",\"SEND_rule\": \"RPUB1\",\"VALUE_NAME\": \"B段值域\",\"SEND_TO_DEPT\": \"99\"}],\"POINTCODE\": \"device002\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [{\"SEND_TO_CAPTAIN\": \"领导A\",\"WARN_TIME_NAME\": \"aa\",\"SEND_TO_rule\": \"RPUB1\",\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"_ROWNUM_\": 0,\"_PK_\": \"1KmLfXxWB2ZHzWPgWqdZbD\",\"WARN_TIME_CODE\": \"1KmLfXxWB2ZHzWPgWqdZbD\",\"SEND_TO_PEOPLE\": \"王富贵\",\"SEND_TO_DEPT\": \"99\",\"WARN_TIME\": \"2\"},{\"SEND_TO_CAPTAIN\": \"\",\"WARN_TIME_NAME\": \"111\",\"SEND_TO_rule\": \"RPUB\",\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"_ROWNUM_\": 1,\"_PK_\": \"ruYFaDiECxxQCcto\",\"WARN_TIME_CODE\": \"ruYFaDiECxxQCcto\",\"SEND_TO_PEOPLE\": \"\",\"SEND_TO_DEPT\": \"\",\"WARN_TIME\": \"10\"}],\"WARN_CODE\": \"1824KhQZ93QayMXpFyUX6s\",\"PROCEDURE_ID\": \"1101\",\"DEVICENUM\": \"\",\"_PK_\": \"1824KhQZ93QayMXpFyUX6s\",\"PRODCODE\": \"GF-TMP-004\",\"IS_WARN_POINT\": \"1\",\"DESCRIBES\": \"1\",\"_ROWNUM_\": 1,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"1\",\"POINTNAME\": \"device002\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-08-27 00:00:00:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"\",\"VALUE_MIN\": \"\",\"VALUE_CODE\": \"3333333\",\"WARN_CODE\": \"TWO02\",\"_ROWNUM_\": 0,\"_PK_\": \"3333333\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"},{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"S56Dvag9ChOboevc\",\"WARN_CODE\": \"TWO02\",\"_ROWNUM_\": 1,\"_PK_\": \"S56Dvag9ChOboevc\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"},{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"1\",\"VALUE_CODE\": \"_USbBthtBjaaIXlI\",\"WARN_CODE\": \"TWO02\",\"_ROWNUM_\": 2,\"_PK_\": \"_USbBthtBjaaIXlI\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_02\",\"DEVICECODE\": \"\",\"_MSG_\": \"OK,修改保存成功。\",\"DEVICENAME\": \"PPP888\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [{\"SEND_TO_CAPTAIN\": \"\",\"WARN_TIME_NAME\": \"33\",\"SEND_TO_rule\": \"\",\"WARN_CODE\": \"TWO02\",\"_ROWNUM_\": 0,\"_PK_\": \"Q4IIdTiLyamuDhN0\",\"WARN_TIME_CODE\": \"Q4IIdTiLyamuDhN0\",\"SEND_TO_PEOPLE\": \"\",\"SEND_TO_DEPT\": \"\",\"WARN_TIME\": \"\"}],\"WARN_CODE\": \"TWO02\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"TWO02\",\"S_MTIME\": \"2019-09-05 16:04:16:126\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"1\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"2\",\"_ROWNUM_\": 3,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_02\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-05 04:00:00:000\",\"AL_VALUE\": [],\"POINTCODE\": \"DB_ATTR_01\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"XAgcy1h8wwPZCIda\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"XAgcy1h8wwPZCIda\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"1\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 9,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_01\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-08-27 00:04:00:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"9EJ6Mcj0w9x0TB4N\",\"WARN_CODE\": \"THREE03\",\"_ROWNUM_\": 0,\"_PK_\": \"9EJ6Mcj0w9x0TB4N\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_03\",\"DEVICECODE\": \"\",\"_MSG_\": \"OK,修改保存成功。\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"THREE03\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"THREE03\",\"S_MTIME\": \"2019-09-05 15:54:48:135\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"1\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"33333\",\"_ROWNUM_\": 4,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_03\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-18 04:04:03:000\",\"AL_VALUE\": [],\"POINTCODE\": \"eeee\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"ZICJttgfzqvdqgv-\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"ZICJttgfzqvdqgv-\",\"PRODCODE\": \"bismark-carry\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 10,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"eee\"},{\"PRODTYPE\": \"1\",\"REGISTER\": \"1\",\"REGISTER_TIME\": \"2019-09-04 09:39:39:000\",\"AL_VALUE\": [],\"POINTCODE\": \"1\",\"DEVICECODE\": \"1\",\"DEVICENAME\": \"1\",\"SEND_PEOPLE\": \"1\",\"AL_TIME\": [],\"WARN_CODE\": \"1\",\"PROCEDURE_ID\": \"1\",\"DEVICENUM\": \"1\",\"_PK_\": \"1\",\"PRODCODE\": \"ptsbdianbiao\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"1\",\"_ROWNUM_\": 6,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"1\",\"POINTNAME\": \"1\"},{\"PRODTYPE\": \"\",\"REGISTER_TIME\": \"2019-09-05 00:00:00\",\"AL_VALUE\": [],\"POINTCODE\": \"1\",\"DEVICECODE\": \"\",\"_APP_\": true,\"_ADD_\": true,\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"PRODCODE\": \"0110672_mb\",\"act\": \"save\",\"DESCRIBES\": \"\",\"WARNTYPE\": \"\",\"POINTNAME\": \"1\",\"_TRANS_\": false,\"REGISTER\": \"\",\"_MSG_\": \"OK,添加的数据保存成功。\",\"serv\": \"AL_SETTING\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"gZemCoivEwcwSp6Y\",\"_PK_\": \"gZemCoivEwcwSp6Y\",\"_TIME_\": \"0.001\",\"IS_WARN_POINT\": \"2\",\"ON_OR_DOWN\": \"2\"},{\"PRODTYPE\": \"2\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-04 16:05:55:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"5\",\"VALUE_MIN\": \"4\",\"VALUE_CODE\": \"0e6qlxcbl6UGn7CXCaReJdA\",\"WARN_CODE\": \"2BvBgVdzJB16oTJlDr9WISh\",\"_ROWNUM_\": 0,\"_PK_\": \"0e6qlxcbl6UGn7CXCaReJdA\",\"WARN_STYLE\": \"1\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"王富贵\",\"SEND_rule\": \"RPUB\",\"VALUE_NAME\": \"A段值域\",\"SEND_TO_DEPT\": \"99\"},{\"VALUE_MAX\": \"7\",\"VALUE_MIN\": \"6\",\"VALUE_CODE\": \"1IaGK644l0d9V4RPSfD3JMv\",\"WARN_CODE\": \"2BvBgVdzJB16oTJlDr9WISh\",\"_ROWNUM_\": 1,\"_PK_\": \"1IaGK644l0d9V4RPSfD3JMv\",\"WARN_STYLE\": \"2\",\"WARN_TYPE\": \"1\",\"SEND_TO_PEOPLE\": \"朱富贵\",\"SEND_rule\": \"RPUB1\",\"VALUE_NAME\": \"B段值域\",\"SEND_TO_DEPT\": \"99\"},{\"VALUE_MAX\": \"\",\"VALUE_MIN\": \"41\",\"VALUE_CODE\": \"3VNdOQKDl42rhHSiuYIikB\",\"WARN_CODE\": \"2BvBgVdzJB16oTJlDr9WISh\",\"_ROWNUM_\": 2,\"_PK_\": \"3VNdOQKDl42rhHSiuYIikB\",\"WARN_STYLE\": \"3\",\"WARN_TYPE\": \"1\",\"SEND_TO_PEOPLE\": \"何富贵\",\"SEND_rule\": \"RPUB2\",\"VALUE_NAME\": \"c段值域\",\"SEND_TO_DEPT\": \"99\"}],\"POINTCODE\": \"device002_6002_0-1.0-0-6\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"lililil\",\"AL_TIME\": [{\"SEND_TO_CAPTAIN\": \"领导A\",\"WARN_TIME_NAME\": \"aa\",\"SEND_TO_rule\": \"RPUB1\",\"WARN_CODE\": \"2BvBgVdzJB16oTJlDr9WISh\",\"_ROWNUM_\": 0,\"_PK_\": \"1XGGv78cBemVgCOvuMvq37\",\"WARN_TIME_CODE\": \"1XGGv78cBemVgCOvuMvq37\",\"SEND_TO_PEOPLE\": \"王富贵\",\"SEND_TO_DEPT\": \"99\",\"WARN_TIME\": \"2\"}],\"WARN_CODE\": \"2BvBgVdzJB16oTJlDr9WISh\",\"PROCEDURE_ID\": \"1102\",\"DEVICENUM\": \"\",\"_PK_\": \"2BvBgVdzJB16oTJlDr9WISh\",\"PRODCODE\": \"GF-TMP-004\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"1\",\"_ROWNUM_\": 0,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"1\",\"POINTNAME\": \"device002_6002_0-1.0-0-6\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-08-27 00:00:05:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"FDEmx8j5Ej_MqS0L\",\"WARN_CODE\": \"SEVEN07\",\"_ROWNUM_\": 0,\"_PK_\": \"FDEmx8j5Ej_MqS0L\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_07\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"SEVEN07\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"SEVEN07\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 8,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_07\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-09-05 11:24:42:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"ONE01_VALUE\",\"WARN_CODE\": \"ONE01\",\"_ROWNUM_\": 0,\"_PK_\": \"ONE01_VALUE\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_018\",\"DEVICECODE\": \"019WG\",\"_MSG_\": \"OK,修改保存成功。\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"ONE01\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"ONE01\",\"S_MTIME\": \"2019-09-05 14:33:30:908\",\"PRODCODE\": \"ptsbdianbiao\",\"IS_WARN_POINT\": \"1\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 2,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_01\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-08-27 00:05:00:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"EUEMzoiIxSiV9HRg\",\"WARN_CODE\": \"SIX06\",\"_ROWNUM_\": 0,\"_PK_\": \"EUEMzoiIxSiV9HRg\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_06\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"SIX06\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"SIX06\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 7,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_06\"},{\"PRODTYPE\": \"\",\"REGISTER\": \"\",\"REGISTER_TIME\": \"2019-08-27 16:59:23:000\",\"AL_VALUE\": [{\"VALUE_MAX\": \"10\",\"VALUE_MIN\": \"5\",\"VALUE_CODE\": \"ZC3Cv1hYzMzjOoKU\",\"WARN_CODE\": \"FOUR04\",\"_ROWNUM_\": 0,\"_PK_\": \"ZC3Cv1hYzMzjOoKU\",\"WARN_STYLE\": \"\",\"WARN_TYPE\": \"\",\"SEND_TO_PEOPLE\": \"\",\"SEND_rule\": \"\",\"VALUE_NAME\": \"\",\"SEND_TO_DEPT\": \"\"}],\"POINTCODE\": \"DB_ATTR_04\",\"DEVICECODE\": \"\",\"DEVICENAME\": \"\",\"SEND_PEOPLE\": \"\",\"AL_TIME\": [],\"WARN_CODE\": \"FOUR04\",\"PROCEDURE_ID\": \"\",\"DEVICENUM\": \"\",\"_PK_\": \"FOUR04\",\"PRODCODE\": \"ptsbdianbiao\",\"ISSHOW\": \"\",\"IS_WARN_POINT\": \"2\",\"DESCRIBES\": \"\",\"_ROWNUM_\": 5,\"ON_OR_DOWN\": \"2\",\"WARNTYPE\": \"\",\"POINTNAME\": \"DB_ATTR_04\"}]"
    val map: util.Map[String, Rule] = RuleJson2Map.WarningJsonHandle(ruleData)
    println(s"map.size = ${map.size}")
    val iter = map.entrySet().iterator()
    while (iter.hasNext) {
      val elem = iter.next()
      println(elem)
    }
  }
  
  
  /**
   * 测试scala集合排序
   */
  @Test
  def test3() {
    val s1 = new Student("a", 55)
    val s2 = new Student("t", 22)
    val s3 = new Student("b", 99)
    //注意sortBy方法将产生一个新的集合
    val list1 = mutable.ListBuffer[Student]()
    list1.append(s1)
    list1.append(s2)
    list1.append(s3)
    val list2 = mutable.ListBuffer[Student]()
    list2.append(s1)
    list2.append(s2)
    list2.append(s3)
    list1.sortBy(_.name)(Ordering.String).foreach(println)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    list2.sortBy(_.age)(Ordering.Int.reverse).foreach(println)
  }
  
  
  /**
   * 测试规则数据读取代码flink提供
   */
  @Test
  def test6() {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceProps = new Properties()
    sourceProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
    sourceProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    sourceProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, PropertiesUtils.getValue("kafka.rule.groupId"))
    //报警规则数据kafka-source（value类型）
    val realDataSource: DataStream[String] = environment.addSource(new FlinkKafkaConsumer[String](PropertiesUtils.getValue("kafka.rule.topic"), new SimpleStringSchema(), sourceProps))
    realDataSource.print()
    environment.execute()
  }
  
  /**
   * 测试规则数据读取代码自定义
   */
  @Test
  def test7() {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils.getValue("kafka.server"))
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, PropertiesUtils.getValue("kafka.rule.groupId"))
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
    val topics = new util.ArrayList[String]()
    topics.add(PropertiesUtils.getValue("kafka.rule.topic"))
    val topicPartitions = new util.ArrayList[TopicPartition]()
    topicPartitions.add(new TopicPartition(PropertiesUtils.getValue("kafka.rule.topic"), 0))
    consumer.assign(topicPartitions)
    val iter: util.Iterator[util.Map.Entry[TopicPartition, lang.Long]] = consumer.endOffsets(topicPartitions).entrySet().iterator()
    while (iter.hasNext) {
      val entry: util.Map.Entry[TopicPartition, lang.Long] = iter.next()
      consumer.seek(entry.getKey, entry.getValue - 1)
    }
    //设置主题和分区消费为最后一条数据
    while (true) {
      val iter: util.Iterator[ConsumerRecord[String, String]] = consumer.poll(100).iterator()
      while (iter.hasNext) {
        println("接收到数据：" + iter.next().value())
      }
    }
  }
  
  @Test
  def test9() {
    val regex = "^(\\d+\\.?\\d+)|(\\d+)$"
    val s1 = "1.2233454"
    val s2 = "1..23"
    val s3 = ".23"
    val s4 = "1.23s"
    val s5 = "0"
    println(s1.matches(regex))
    println(s2.matches(regex))
    println(s3.matches(regex))
    println(s4.matches(regex))
    println(s5.matches(regex))
  }
  
  @Test
  def test10() {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val realProperties = new Properties()
    realProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev.cloudiip.bonc.local:9092")
    realProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xingda-warning-handle-21")
    realProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    realProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    realProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    realProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "real-source-" + Random.nextInt(10000))
    val source = new FlinkKafkaConsumer[Tuple2[String, String]](
      "iot_v1_property_up",
      new TypeInformationKeyValueSerializationSchema[String, String](BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, environment.getConfig),
      realProperties)
    val data: DataStream[Tuple2[String, String]] = environment.addSource(source)
    data.print("接收到数据：")
    environment.execute()
  }
  
  @Test
  def test11() {
    val pointInfo: String = "{\n  \"u8n2xqhKGvJsTI2C~S4_BJ_22\": \"变频器保护\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_20\": \"刹车释放\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_27\": \"机床故障字备用\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_24\": \"伺服计数故障\",\n  \"QGwjsRg9AGkLZohg~333\": \"\",\n  \"aaaaaaaaaaaaaaa~aaa0001name\": \"aaa0001des\",\n  \"n8CpySiPKB9nyWuE~N1_XS_01\": \"收线长度系数\",\n  \"caesar1009~sipo0000\": \"\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_12\": \"收线电机保护\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_11\": \"收线断丝保护\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_10\": \"收线风机保护\",\n  \"mb0822~0822-2mc\": \"44\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_15\": \"紧急停车\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_02\": \"请按计米清零\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_01\": \"计米故障\",\n  \"0110672_mb~5\": \"444\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_07\": \"排线左限位故障\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_04\": \"计米定长到\",\n  \"Maq7nyivCHoetWWI~N2_BJ_01\": \"主机通讯故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_02\": \"一键调试结束\",\n  \"Maq7nyivCHoetWWI~N2_BJ_03\": \"断丝\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_08\": \"排线右限位故障\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_14\": \"通讯故障\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_11\": \"收线断丝\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_18\": \"提前降速\",\n  \"u8n2xqhKGvJsTI2C~S4_SZ_02\": \"主机速度设定\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_17\": \"防弹簧保护\",\n  \"u8n2xqhKGvJsTI2C~S4_SZ_01\": \"收线长度设定\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_16\": \"防跑头保护\",\n  \"u8n2xqhKGvJsTI2C~S4_SZ_04\": \"远程主机速度设定\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_09\": \"设备处于调试模式\",\n  \"u8n2xqhKGvJsTI2C~S4_SZ_03\": \"远程长度设定\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_04\": \"机床有报警停机\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_05\": \"定长到达\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_02\": \"机床运行中\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_03\": \"机床无报警停机\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_01\": \"休眠或未上电\",\n  \"aaa~name\": \"desc\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_20\": \"设置的米长到达\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_19\": \"速度偏差报警\",\n  \"mb0822~0822-1mc\": \"3\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_25\": \"请检查防弹簧\",\n  \"test3~M02\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_23\": \"柜内风机故障\",\n  \"test3~M03\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_22\": \"变频器故障\",\n  \"XP2QrtjyFsH_l3TX~T1\": \"\",\n  \"XP2QrtjyFsH_l3TX~T2\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_28\": \"请检查收线断丝\",\n  \"XP2QrtjyFsH_l3TX~T3\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_BJ_27\": \"机床故障字备用\",\n  \"XP2QrtjyFsH_l3TX~T4\": \"\",\n  \"XP2QrtjyFsH_l3TX~T5\": \"\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_15\": \"机床备用状态2\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_16\": \"机床备用状态3\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_13\": \"联网写入备用3\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_14\": \"机床备用状态1\",\n  \"~\": \"0\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_11\": \"联网写入备用1\",\n  \"H73ZNxhDJRokRBfE~N9_ZT_12\": \"联网写入备用2\",\n  \"2019090703~code_for_name\": \"name_for_desc\",\n  \"Maq7nyivCHoetWWI~N2_XS_01\": \"收线长度系数\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_22\": \"钥匙开关打开\",\n  \"caesar10090~caesar002\": \"\",\n  \"X6png0h4FNRZEzBw~温度\": \"\",\n  \"B3xRaxguC4r4L8hC~N10_AN_01\": \"远程锁机\",\n  \"B3xRaxguC4r4L8hC~N10_AN_03\": \"停止按钮\",\n  \"B3xRaxguC4r4L8hC~N10_AN_02\": \"远程停机\",\n  \"B3xRaxguC4r4L8hC~N10_SS_04\": \"过捻转速\",\n  \"B3xRaxguC4r4L8hC~N10_SS_03\": \"主机飞轮转速\",\n  \"B3xRaxguC4r4L8hC~N10_SS_06\": \"收线主计米当前长度\",\n  \"B3xRaxguC4r4L8hC~N10_SS_02\": \"系统版本号\",\n  \"B3xRaxguC4r4L8hC~N10_SS_01\": \"机床号\",\n  \"0110672_mb~0110672_mb_00002\": \"1\",\n  \"H73ZNxhDJRokRBfE~N9_AN_02\": \"远程停机\",\n  \"H73ZNxhDJRokRBfE~N9_AN_01\": \"远程锁机\",\n  \"0110672_mb~0110672_mb_00001\": \"123\",\n  \"0110672_mb~0110672_mb_00003\": \"1\",\n  \"H73ZNxhDJRokRBfE~N9_SS_06\": \"收线主计米当前长度\",\n  \"H73ZNxhDJRokRBfE~N9_SS_01\": \"机床号\",\n  \"H73ZNxhDJRokRBfE~N9_XS_01\": \"收线长度系数\",\n  \"X-BRyli9KyQDD7vw~S2_XS_01\": \"收线长度系数\",\n  \"H73ZNxhDJRokRBfE~N9_SS_04\": \"过捻转速\",\n  \"H73ZNxhDJRokRBfE~N9_SS_03\": \"主机飞轮转速\",\n  \"H73ZNxhDJRokRBfE~N9_SS_02\": \"系统版本号\",\n  \"Maq7nyivCHoetWWI~N2_ZT_22\": \"钥匙开关打开\",\n  \"device01~动态属性1mm123\": \"\",\n  \"B3xRaxguC4r4L8hC~N10_SZ_07\": \"远程长度设定\",\n  \"B3xRaxguC4r4L8hC~N10_SZ_09\": \"飞轮转速设定\",\n  \"n8CpySiPKB9nyWuE~N1_AN_01\": \"远程锁机\",\n  \"n8CpySiPKB9nyWuE~N1_AN_02\": \"远程停机\",\n  \"B3xRaxguC4r4L8hC~N10_SZ_03\": \"远程收线转速设定\",\n  \"B3xRaxguC4r4L8hC~N10_SZ_04\": \"收线长度设定\",\n  \"Maq7nyivCHoetWWI~N2_SZ_08\": \"远程主机转速设定\",\n  \"Maq7nyivCHoetWWI~N2_SZ_07\": \"远程长度设定\",\n  \"test_shen~测试属性1\": \"1\",\n  \"test_shen~测试属性2\": \"1\",\n  \"test_shen~测试属性4\": \"1\",\n  \"2019090701~code_for_name\": \"name_for_desc\",\n  \"LmXLAijnLXBsDjQl~S1_AN_01\": \"远程锁机\",\n  \"LmXLAijnLXBsDjQl~S1_AN_02\": \"远程停机\",\n  \"muzd001~11111\": \"1\",\n  \"n8CpySiPKB9nyWuE~N1_SZ_01\": \"主机转速设定\",\n  \"a7D8VU40~1\": \"1\",\n  \"n8CpySiPKB9nyWuE~N1_SZ_07\": \"远程长度设定\",\n  \"n8CpySiPKB9nyWuE~N1_SZ_04\": \"收线长度设定\",\n  \"iZxZ8ng1CBBQdra4~asd\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_SZ_01\": \"收线长度设定\",\n  \"test2~S2_P001_002\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_SZ_02\": \"主机速度设定\",\n  \"test2~S2_P001_001\": \"\",\n  \"n8CpySiPKB9nyWuE~N1_SZ_08\": \"远程主机转速设定\",\n  \"X-BRyli9KyQDD7vw~S2_SZ_03\": \"远程长度设定\",\n  \"X-BRyli9KyQDD7vw~S2_SZ_04\": \"远程主机速度设定\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_01\": \"断丝或计米故障\",\n  \"u8n2xqhKGvJsTI2C~S4_SS_04\": \"收线主机米当前长度\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_09\": \"收线定长到\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_08\": \"排线右限位故障\",\n  \"Maq7nyivCHoetWWI~N2_SZ_04\": \"收线长度设定\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_07\": \"排线左限位故障\",\n  \"u8n2xqhKGvJsTI2C~S4_SS_02\": \"系统版本号\",\n  \"u8n2xqhKGvJsTI2C~S4_SS_03\": \"机床实际速度\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_05\": \"放线断丝故障\",\n  \"Maq7nyivCHoetWWI~N2_SZ_01\": \"主机转速设定\",\n  \"u8n2xqhKGvJsTI2C~S4_BJ_03\": \"计米误差报警\",\n  \"lR0FuEjnC8FvGoKw~test \": \"\",\n  \"hA-v3jhKIIFYa2Dt~11\": \"\",\n  \"GK9tKlgPG5BabTQX~开关状态\": \"\",\n  \"LmXLAijnLXBsDjQl~S1_SZ_01\": \"收线长度设定\",\n  \"u8n2xqhKGvJsTI2C~S4_SS_01\": \"机床号\",\n  \"LmXLAijnLXBsDjQl~S1_SZ_04\": \"远程主机速度设定\",\n  \"LmXLAijnLXBsDjQl~S1_SZ_03\": \"远程长度设定\",\n  \"LmXLAijnLXBsDjQl~S1_SZ_02\": \"主机速度设定\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_74\": \"内放线长度过低\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_72\": \"扭转控制错误\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_71\": \"摆杆位置错误\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_70\": \"开关门处有异常情况\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_77\": \"SBD动作\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_76\": \"外放线长度过低\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_75\": \"芯股长度过低\",\n  \"LmXLAijnLXBsDjQl~S1_XS_01\": \"收线长度系数\",\n  \"Maq7nyivCHoetWWI~N2_AN_01\": \"远程锁机\",\n  \"Maq7nyivCHoetWWI~N2_AN_02\": \"远程停机\",\n  \"H73ZNxhDJRokRBfE~N9_SZ_09\": \"飞轮转速设定\",\n  \"H73ZNxhDJRokRBfE~N9_SZ_07\": \"远程长度设定\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_05\": \"机床有报警停机\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_04\": \"机床无报警停机\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_07\": \"定长到达\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_06\": \"机床放线防跑头动作\",\n  \"H73ZNxhDJRokRBfE~N9_SZ_03\": \"远程收线转速设定\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_01\": \"睡眠或未上电\",\n  \"H73ZNxhDJRokRBfE~N9_SZ_04\": \"收线长度设定\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_95\": \"摇篮晃动过大\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_03\": \"机床降速拉拔运行中\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_02\": \"机床运行中\",\n  \"0000000000000001~name23\": \"\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_91\": \"伺服报警，请下盘\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_90\": \"主机低速设置过低\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_01\": \"通讯错误\",\n  \"Maq7nyivCHoetWWI~N2_SS_04\": \"过捻实际转速\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_02\": \"一键调试\",\n  \"Maq7nyivCHoetWWI~N2_SS_06\": \"收线主计米当前长度\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_06\": \"牵引轮断丝\",\n  \"Maq7nyivCHoetWWI~N2_SS_01\": \"机床号\",\n  \"Maq7nyivCHoetWWI~N2_SS_02\": \"系统版本号\",\n  \"Maq7nyivCHoetWWI~N2_SS_03\": \"主机实际转速\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_09\": \"主副计米误差过大\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_08\": \"外放线断丝\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_15\": \"机床备用状态3\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_12\": \"联网写入备用3\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_11\": \"联网写入备用2\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_14\": \"机床备用状态2\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_13\": \"机床备用状态1\",\n  \"u8n2xqhKGvJsTI2C~S4_ZT_10\": \"联网写入备用1\",\n  \"aaaaaaaaaaaaaaa~aaasdfasdf\": \"asdfasdf\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_11\": \"ATC扭簧断\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_17\": \"排线故障\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_19\": \"急停\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_15\": \"机床备用状态3\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_14\": \"机床备用状态2\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_13\": \"机床备用状态1\",\n  \"B3xRaxguC4r4L8hC~N10_XS_01\": \"收线长度系数\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_12\": \"联网写入备用3\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_11\": \"联网写入备用2\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_10\": \"联网写入备用1\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_04\": \"机床无报警停机\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_03\": \"机床降速拉拔运行中\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_02\": \"机床运行中\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_01\": \"睡眠或未上电\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_09\": \"报警换模降速\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_08\": \"报警涨速拉拔中\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_07\": \"定长到达\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_06\": \"机床放线防跑头动作\",\n  \"LmXLAijnLXBsDjQl~S1_ZT_05\": \"机床有报警停机\",\n  \"YVkRtUgAI6eZ8Sqr~111\": \"\",\n  \"r4Rh_aiAzqYzVI4k~T1\": \"\",\n  \"r4Rh_aiAzqYzVI4k~T4\": \"\",\n  \"r4Rh_aiAzqYzVI4k~T5\": \"\",\n  \"r4Rh_aiAzqYzVI4k~T2\": \"\",\n  \"r4Rh_aiAzqYzVI4k~T3\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_AN_02\": \"远程停机\",\n  \"X-BRyli9KyQDD7vw~S2_AN_01\": \"远程锁机\",\n  \"QGwjsRg9AGkLZohg~1111\": \"\",\n  \"Maq7nyivCHoetWWI~N2_ZT_01\": \"休眠或者未上电\",\n  \"n8CpySiPKB9nyWuE~N1_SS_01\": \"机床号\",\n  \"n8CpySiPKB9nyWuE~N1_SS_02\": \"系统版本号\",\n  \"Maq7nyivCHoetWWI~N2_ZT_08\": \"放线计米到\",\n  \"n8CpySiPKB9nyWuE~N1_SS_03\": \"主机实际转速\",\n  \"Maq7nyivCHoetWWI~N2_ZT_07\": \"收线计米到\",\n  \"n8CpySiPKB9nyWuE~N1_SS_04\": \"过捻实际转速\",\n  \"X-BRyli9KyQDD7vw~S2_SS_04\": \"收线主机米当前长度\",\n  \"Maq7nyivCHoetWWI~N2_ZT_05\": \"定长到达\",\n  \"X-BRyli9KyQDD7vw~S2_SS_03\": \"机床实际速度\",\n  \"n8CpySiPKB9nyWuE~N1_SS_06\": \"收线主计米当前长度\",\n  \"Maq7nyivCHoetWWI~N2_ZT_04\": \"机床有报警停机\",\n  \"X-BRyli9KyQDD7vw~S2_SS_02\": \"系统版本号\",\n  \"Maq7nyivCHoetWWI~N2_ZT_03\": \"机床无报警停机\",\n  \"X-BRyli9KyQDD7vw~S2_SS_01\": \"机床号\",\n  \"Maq7nyivCHoetWWI~N2_ZT_02\": \"机床运行中\",\n  \"2019090702~属性二一\": \"\",\n  \"Maq7nyivCHoetWWI~N2_ZT_12\": \"联网写入备用2\",\n  \"Maq7nyivCHoetWWI~N2_ZT_11\": \"联网写入备用1\",\n  \"gateway01~111\": \"\",\n  \"Maq7nyivCHoetWWI~N2_ZT_16\": \"机床备用状态3\",\n  \"Maq7nyivCHoetWWI~N2_ZT_15\": \"机床备用状态2\",\n  \"Maq7nyivCHoetWWI~N2_ZT_14\": \"机床备用状态1\",\n  \"Maq7nyivCHoetWWI~N2_ZT_13\": \"联网写入备用3\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_10\": \"收线风机保护\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_20\": \"设置的米长到达\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_12\": \"收线电机保护\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_13\": \"断丝保护\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_15\": \"急停按钮报警\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_63\": \"ng102012\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_61\": \"主机刹车打开\",\n  \"u8n2xqhKGvJsTI2C~S4_XS_01\": \"收线长度系数\",\n  \"0000000000000001~name\": \"\",\n  \"X6png0h4FNRZEzBw~开关状态\": \"\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_12\": \"联网写入备用2\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_16\": \"设定过捻比超限\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_13\": \"联网写入备用3\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_14\": \"机床备用状态1\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_18\": \"计米故障\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_15\": \"机床备用状态2\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_19\": \"急停\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_16\": \"机床备用状态3\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_14\": \"过捻转速异常\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_15\": \"纠正过捻比超限\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_20\": \"刹车释放报警\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_09\": \"计米超差\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_21\": \"请检查单丝径\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_22\": \"变频器保护动作\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_71\": \"收线摆杆位置过高\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_26\": \"请检查放线镀铜\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_27\": \"机床故障字备用\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_74\": \"内放线长度过低\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_75\": \"芯股长度过低\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_72\": \"扭转控制错误\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_73\": \"摇篮安全插销未插\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_76\": \"外放线长度过低\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_77\": \"SBD动作\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_01\": \"通讯故障\",\n  \"test2~S5_P005_001\": \"\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_01\": \"主机通讯故障\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_09\": \"计米误差报警\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_02\": \"一键调试结束\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_08\": \"外放线断丝\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_03\": \"断丝\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_07\": \"成绳断丝\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_06\": \"牵引轮断丝\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_11\": \"联网写入备用2\",\n  \"ptsbdianbiao~DB_ATTR_09\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_12\": \"联网写入备用3\",\n  \"ptsbdianbiao~DB_ATTR_08\": \"\",\n  \"ptsbdianbiao~DB_ATTR_07\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_10\": \"联网写入备用1\",\n  \"ptsbdianbiao~DB_ATTR_06\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_15\": \"机床备用状态3\",\n  \"ptsbdianbiao~DB_ATTR_05\": \"\",\n  \"ptsbdianbiao~DB_ATTR_04\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_13\": \"机床备用状态1\",\n  \"ptsbdianbiao~DB_ATTR_03\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_14\": \"机床备用状态2\",\n  \"ptsbdianbiao~DB_ATTR_01\": \"\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_05\": \"定长到达\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_04\": \"机床有报警停机\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_03\": \"机床无报警停机\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_33\": \"模块通讯故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_50\": \"收线刹车按钮未关\",\n  \"Maq7nyivCHoetWWI~N2_BJ_52\": \"顶针未顶出启动\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_08\": \"放线计米到\",\n  \"Maq7nyivCHoetWWI~N2_BJ_53\": \"门罩未关\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_07\": \"收线计米到\",\n  \"Maq7nyivCHoetWWI~N2_BJ_54\": \"请打开钥匙开关\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_38\": \"ATC超过四区\",\n  \"Maq7nyivCHoetWWI~N2_BJ_55\": \"请按计米清零\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_39\": \"ATC进入死区\",\n  \"Maq7nyivCHoetWWI~N2_BJ_56\": \"请检查断保\",\n  \"Maq7nyivCHoetWWI~N2_BJ_57\": \"请检查ATC断保\",\n  \"Maq7nyivCHoetWWI~N2_BJ_58\": \"请点动17秒\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_02\": \"机床运行中\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_34\": \"过捻通讯故障\",\n  \"LmXLAijnLXBsDjQl~S1_SS_02\": \"系统版本号\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_01\": \"休眠或者未上电\",\n  \"LmXLAijnLXBsDjQl~S1_SS_01\": \"机床号\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_36\": \"收线通讯故障\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_17\": \"排线故障\",\n  \"LmXLAijnLXBsDjQl~S1_SS_04\": \"收线主机米当前长度\",\n  \"LmXLAijnLXBsDjQl~S1_SS_03\": \"机床实际速度\",\n  \"1~1\": \"1\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_01\": \"睡眠或未上电\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_10\": \"摇篮安全开关按钮合\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_01\": \"断丝或计米故障\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_11\": \"联网写入备用1\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_04\": \"机床无报警停机\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_05\": \"机床有报警停机\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_02\": \"机床运行中\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_06\": \"排线故障\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_03\": \"机床降速拉拔运行中\",\n  \"LmXLAijnLXBsDjQl~S1_BJ_09\": \"收线定长到\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_06\": \"机床放线防跑头动作\",\n  \"2019090702~属性二二\": \"\",\n  \"X-BRyli9KyQDD7vw~S2_ZT_07\": \"定长到达\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_20\": \"记录故障\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_16\": \"机床备用状态3\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_21\": \"过捻伺服故障\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_15\": \"机床备用状态2\",\n  \"Maq7nyivCHoetWWI~N2_BJ_60\": \"收线未刹车\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_22\": \"电柜风机故障\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_14\": \"机床备用状态1\",\n  \"Maq7nyivCHoetWWI~N2_BJ_61\": \"主机未刹车\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_01\": \"休眠或未上电\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_02\": \"机床运行中\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_28\": \"变频器故障\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_03\": \"机床无报警停机\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_04\": \"机床有报警停机\",\n  \"B3xRaxguC4r4L8hC~N10_ZT_05\": \"定长到达\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_13\": \"联网写入备用3\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_23\": \"主机风机故障\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_12\": \"联网写入备用2\",\n  \"n8CpySiPKB9nyWuE~N1_ZT_11\": \"联网写入备用1\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_28\": \"变频器故障\",\n  \"bismark-carry~火力\": \"\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_52\": \"顶针未顶出启动\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_53\": \"门罩未关\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_54\": \"请打开钥匙开关\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_55\": \"请按计米清零\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_28\": \"变频器故障\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_50\": \"收线刹车按钮未关\",\n  \"Maq7nyivCHoetWWI~N2_BJ_33\": \"模块通讯故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_34\": \"过捻通讯故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_36\": \"收线通讯故障\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_56\": \"请检查断保\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_57\": \"请检查ATC断保\",\n  \"Maq7nyivCHoetWWI~N2_BJ_38\": \"ATC超过四区\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_58\": \"请点动17秒\",\n  \"Maq7nyivCHoetWWI~N2_BJ_39\": \"ATC进入死区\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_41\": \"ATC钢丝断\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_41\": \"ATC断丝\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_42\": \"ATC异常请报修\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_43\": \"ATC调节失败\",\n  \"Maq7nyivCHoetWWI~N2_BJ_40\": \"ATC值无变化\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_45\": \"风机故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_41\": \"ATC断丝\",\n  \"Maq7nyivCHoetWWI~N2_BJ_42\": \"ATC异常请报修\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_37\": \"缺丝检测仪报警\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_40\": \"ATC值无变化\",\n  \"Maq7nyivCHoetWWI~N2_BJ_43\": \"ATC调节失败\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_49\": \"停止按钮卡住\",\n  \"Maq7nyivCHoetWWI~N2_BJ_46\": \"磁通量报警\",\n  \"Maq7nyivCHoetWWI~N2_BJ_47\": \"复位按钮卡住\",\n  \"Maq7nyivCHoetWWI~N2_BJ_48\": \"启动按钮卡住\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_46\": \"磁通量报警\",\n  \"Maq7nyivCHoetWWI~N2_BJ_49\": \"停止按钮卡住\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_47\": \"复位按钮卡住\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_48\": \"启动按钮卡住\",\n  \"Maq7nyivCHoetWWI~N2_BJ_09\": \"计米超差\",\n  \"device01~动态属性0002mm\": \"\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_52\": \"收线顶针打开\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_50\": \"收线刹车打开\",\n  \"2019090702~属性二\": \"\",\n  \"test1~S1_P001_002\": \"\",\n  \"test1~S1_P001_003\": \"1\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_54\": \"钥匙按钮打开\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_53\": \"安全罩未到位\",\n  \"test1~S1_P001_001\": \"\",\n  \"u8n2xqhKGvJsTI2C~S4_AN_01\": \"远程锁机\",\n  \"u8n2xqhKGvJsTI2C~S4_AN_02\": \"远程停机\",\n  \"test1~S1_P001_004\": \"\",\n  \"Maq7nyivCHoetWWI~N2_BJ_14\": \"过捻转速异常\",\n  \"Maq7nyivCHoetWWI~N2_BJ_15\": \"纠正过捻比超限\",\n  \"Maq7nyivCHoetWWI~N2_BJ_16\": \"设定过捻比超限\",\n  \"Maq7nyivCHoetWWI~N2_BJ_18\": \"计米故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_19\": \"急停\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_63\": \"摇篮翻转插销未到位\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_62\": \"设备运行中发生故障，请下盘\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_61\": \"主机刹车打开\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_52\": \"收线顶针打开\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_53\": \"安全罩打开\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_50\": \"收线刹车打开\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_51\": \"散热风机故障\",\n  \"B3xRaxguC4r4L8hC~N10_BJ_54\": \"钥匙按钮打开\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_60\": \"收线未刹车\",\n  \"n8CpySiPKB9nyWuE~N1_BJ_61\": \"主机未刹车\",\n  \"Maq7nyivCHoetWWI~N2_BJ_20\": \"记录故障\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_65\": \"主机速度设置错误\",\n  \"Maq7nyivCHoetWWI~N2_BJ_21\": \"过捻伺服故障\",\n  \"H73ZNxhDJRokRBfE~N9_BJ_64\": \"伺服速度偏差，请呼叫维修工\",\n  \"Maq7nyivCHoetWWI~N2_BJ_22\": \"电柜风机故障\",\n  \"Maq7nyivCHoetWWI~N2_BJ_23\": \"主机风机故障\",\n  \"2019090703~111\": \"\",\n  \"Maq7nyivCHoetWWI~N2_BJ_28\": \"变频器故障\"\n}"
    println(JSON.parseObject(pointInfo, classOf[util.HashMap[String, Object]]))
  }
  
  @Test
  def test12() {
    val s1 = "1"
    val s2 = "1.1"
    val s3 = "1.12"
    val s4 = "1.123"
    val s5 = "1.1234"
    println(StringUtils.truncate(s1))
    println(StringUtils.truncate(s2))
    println(StringUtils.truncate(s3))
    println(StringUtils.truncate(s4))
    println(StringUtils.truncate(s5))
  }
  
  @Test
  def test13() {
    println("开始执行")
    try {
      println("try开始执行")
      1 / 0
      println("try执行结束")
    } catch {
      case e: Exception => println("捕获到异常：\n" + ExceptionUtils.stringifyException(e))
    }
    println("结束执行")
  }
  
  @Test
  def test14() {
    MysqlUtils.insertInto("type1", new NullPointerException, "realData1", "ruleData1", "pointInfo1")
  }
  
  @Test
  def testStandbyFaultJson2Map() {
    val json = "[{\"PROPCODE\":\"DT2G_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"0p67Dtg_LzNC1CN5\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:09:12:015\",\"PRODCODE\":\"DT2G\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":0,\"PROPNAME\":\"DT2G_ZT_02\",\"ID\":\"0p67Dtg_LzNC1CN5\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:29:38:712\"},{\"PROPCODE\":\"LL_SS_04\",\"PROPVALUE\":\"0\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"3Xl-BAjyDxxD6t8p\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:14:27:489\",\"PRODCODE\":\"LianLa\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":1,\"PROPNAME\":\"LL_SS_04\",\"ID\":\"3Xl-BAjyDxxD6t8p\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:27:19:195\"},{\"PROPCODE\":\"XD6_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"6FSVYZjAwBsoK_9Q\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:14:32:812\",\"PRODCODE\":\"XD6\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":2,\"PROPNAME\":\"XD6_ZT_02\",\"ID\":\"6FSVYZjAwBsoK_9Q\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:33:09:717\"},{\"PROPCODE\":\"DTS_ZT_05\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"a3cKEgirDaJVuWm0\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:14:37:593\",\"PRODCODE\":\"DTS\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":3,\"PROPNAME\":\"DTS_ZT_05\",\"ID\":\"a3cKEgirDaJVuWm0\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:49:13:994\"},{\"PROPCODE\":\"ZL_SS_01\",\"PROPVALUE\":\"0\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"BI_qQKiKHCwR-KqH\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:14:42:265\",\"PRODCODE\":\"ZhongLa\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":4,\"PROPNAME\":\"ZL_SS_01\",\"ID\":\"BI_qQKiKHCwR-KqH\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:26:58:820\"},{\"PROPCODE\":\"CL_SS_04\",\"PROPVALUE\":\"0\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"DZKq6nicFp0rC0Td\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:09:34:771\",\"PRODCODE\":\"CuLa\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":5,\"PROPNAME\":\"CL_SS_04\",\"ID\":\"DZKq6nicFp0rC0Td\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:26:18:148\"},{\"PROPCODE\":\"SX_ZT_07\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"g5Jn_3gHCch-mAjZ\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:14:50:061\",\"PRODCODE\":\"ShuiXiang\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":6,\"PROPNAME\":\"SX_ZT_07\",\"ID\":\"g5Jn_3gHCch-mAjZ\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:34:13:722\"},{\"PROPCODE\":\"CL_SS_02\",\"PROPVALUE\":\"CL_SZ_01\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"Jq4s6LgCFTYDOQLh\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:12:17:416\",\"PRODCODE\":\"CuLa\",\"CONSTANT_TYPE\":\"VARIABLE\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":7,\"PROPNAME\":\"CL_SS_02\",\"ID\":\"Jq4s6LgCFTYDOQLh\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\">=\",\"S_ATIME\":\"2019-10-31 18:11:32:341\"},{\"PROPCODE\":\"DT3G_ZT_05\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"L-pcHbgyEi34hWTg\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:11:798\",\"PRODCODE\":\"DT3G\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":8,\"PROPNAME\":\"DT3G_ZT_05\",\"ID\":\"L-pcHbgyEi34hWTg\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:48:07:180\"},{\"PROPCODE\":\"LL_SS_02\",\"PROPVALUE\":\"LL_SS_01\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"lh1MbugCCAdvv1ei\",\"GREATER_THAN_ZERO\":\"1\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:50:27:946\",\"PRODCODE\":\"LianLa\",\"CONSTANT_TYPE\":\"VARIABLE\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":9,\"PROPNAME\":\"LL_SS_02\",\"ID\":\"lh1MbugCCAdvv1ei\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"<=\",\"S_ATIME\":\"2019-10-31 18:50:27:946\"},{\"PROPCODE\":\"DTS_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"n3pa81hTyPhINMQc\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:19:003\",\"PRODCODE\":\"DTS\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":10,\"PROPNAME\":\"DTS_ZT_02\",\"ID\":\"n3pa81hTyPhINMQc\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:30:28:514\"},{\"PROPCODE\":\"XD6_ZT_05\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"Q5hnUxggCaBGJxlr\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:24:298\",\"PRODCODE\":\"XD6\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":11,\"PROPNAME\":\"XD6_ZT_05\",\"ID\":\"Q5hnUxggCaBGJxlr\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:49:34:672\"},{\"PROPCODE\":\"DT3G_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"q8RyHwhuIDegajkw\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:30:486\",\"PRODCODE\":\"DT3G\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":12,\"PROPNAME\":\"DT3G_ZT_02\",\"ID\":\"q8RyHwhuIDegajkw\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:29:54:032\"},{\"PROPCODE\":\"SX_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"Sb7_J5itymPnovWc\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:36:340\",\"PRODCODE\":\"ShuiXiang\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":13,\"PROPNAME\":\"SX_ZT_02\",\"ID\":\"Sb7_J5itymPnovWc\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:28:54:679\"},{\"PROPCODE\":\"DTU_ZT_02\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"SQs677j8whI5GlHr\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:41:512\",\"PRODCODE\":\"DTU\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":14,\"PROPNAME\":\"DTU_ZT_02\",\"ID\":\"SQs677j8whI5GlHr\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"!=\",\"S_ATIME\":\"2019-10-31 15:30:11:373\"},{\"PROPCODE\":\"DTU_ZT_05\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"Vr_NGVgXAcddjaaX\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:46:050\",\"PRODCODE\":\"DTU\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":15,\"PROPNAME\":\"DTU_ZT_05\",\"ID\":\"Vr_NGVgXAcddjaaX\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:48:29:986\"},{\"PROPCODE\":\"DT2G_ZT_05\",\"PROPVALUE\":\"1\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"vTy9p_idGddSkBP6\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:52:414\",\"PRODCODE\":\"DT2G\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":16,\"PROPNAME\":\"DT2G_ZT_05\",\"ID\":\"vTy9p_idGddSkBP6\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:46:01:613\"},{\"PROPCODE\":\"LL_SS_42\",\"PROPVALUE\":\"LL_SS_01\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"W2IlFJg2xmaDcGn_\",\"GREATER_THAN_ZERO\":\"1\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:49:57:882\",\"PRODCODE\":\"LianLa\",\"CONSTANT_TYPE\":\"VARIABLE\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":17,\"PROPNAME\":\"LL_SS_42\",\"ID\":\"W2IlFJg2xmaDcGn_\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"<=\",\"S_ATIME\":\"2019-10-31 18:49:57:882\"},{\"PROPCODE\":\"ZL_SZ_02\",\"PROPVALUE\":\"ZL_SS_02\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"wu0hxUhwCRIIFdaT\",\"GREATER_THAN_ZERO\":\"1\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:47:53:663\",\"PRODCODE\":\"ZhongLa\",\"CONSTANT_TYPE\":\"VARIABLE\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":18,\"PROPNAME\":\"ZL_SZ_02\",\"ID\":\"wu0hxUhwCRIIFdaT\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"<=\",\"S_ATIME\":\"2019-10-31 18:47:53:663\"},{\"PROPCODE\":\"ZL_SZ_27\",\"PROPVALUE\":\"ZL_SS_02\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"xH5JuYgOC66czCGF\",\"GREATER_THAN_ZERO\":\"1\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:46:15:799\",\"PRODCODE\":\"ZhongLa\",\"CONSTANT_TYPE\":\"VARIABLE\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":19,\"PROPNAME\":\"ZL_SZ_27\",\"ID\":\"xH5JuYgOC66czCGF\",\"USE_TYPE\":\"STANDBY\",\"JUDGE_TYPE\":\"<=\",\"S_ATIME\":\"2019-10-31 18:46:15:799\"},{\"PROPCODE\":\"DD_SS_01\",\"PROPVALUE\":\"0\",\"KAFKA_TYPE\":\"\",\"S_FLAG\":\"1\",\"_PK_\":\"Z5BEnoh2wN7Je2py\",\"GREATER_THAN_ZERO\":\"\",\"S_MUSER\":\"\",\"S_MTIME\":\"2019-10-31 18:15:57:569\",\"PRODCODE\":\"DianDu\",\"CONSTANT_TYPE\":\"CONSTANT\",\"S_USER\":\"HvXMY1gMEm9gxAVw\",\"_ROWNUM_\":20,\"PROPNAME\":\"DD_SS_01\",\"ID\":\"Z5BEnoh2wN7Je2py\",\"USE_TYPE\":\"FAULT\",\"JUDGE_TYPE\":\"=\",\"S_ATIME\":\"2019-10-31 15:28:11:655\"}]"
    val standbyFaultRule = RuleJson2Map.faultStandbyJsonHandle(json)
    println(standbyFaultRule)
  }
  
  @Test
  def testJedis() {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setBlockWhenExhausted(true)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnCreate(true)
    poolConfig.setMaxTotal(50)
    val nodes = new util.HashSet[HostAndPort]()
    val hosts: Array[String] = PropertiesUtils.getValue("redis.host").split(',')
    for (host <- hosts) {
      val ipPort = host.split(':')
      nodes.add(new HostAndPort(ipPort(0), ipPort(1).toInt))
    }
    val jedis = new JedisCluster(nodes, poolConfig)
    val warningData = JSON.parseObject(jedis.get("ZhongLa~09330-0021~ZL_SS_02"), classOf[WarningData])
    println(warningData)
  }
  
  @Test
  def testFetchTime() {
    val topic: String = "real_data_wzq"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev.cloudiip.bonc.local:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singleton(topic))
    val pattern = Pattern.compile("\"time\":\"(\\d+)\"")
    while (true) {
      val iter = consumer.poll(1000).iterator()
      while (iter.hasNext) {
        val value = iter.next().value()
        println(value)
        val matcher = pattern.matcher(value)
        if (matcher.find()) {
          println(matcher.group(1))
        }
      }
    }
  }
  
  @Test
  def testIsNumber() {
    val s = "0"
    println(StringUtils.isNumber(s))
    println(StringUtils.truncate(s).toDouble)
  }
  
  @Test
  def testGetRedisAllData() {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setBlockWhenExhausted(true)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnCreate(true)
    poolConfig.setMaxTotal(Int.MaxValue)
    val nodes = new util.HashSet[HostAndPort]()
    val hosts: Array[String] = "172.16.22.156:8001,172.16.22.156:8002,172.16.22.156:8003,172.16.22.156:8004,172.16.22.156:8005,172.16.22.156:8006".split(',')
    for (host <- hosts) {
      val ipPort = host.split(':')
      nodes.add(new HostAndPort(ipPort(0), ipPort(1).toInt))
    }
    val jedis = new JedisCluster(nodes, poolConfig)
    var count = 1
    while (true) {
      println("\n\n" + count + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      val jedisKeys: util.Set[String] = new util.HashSet[String]()
      val iter = jedis.getClusterNodes.entrySet().iterator()
      while (iter.hasNext) {
        jedisKeys.addAll(iter.next().getValue.getResource.keys("*"))
      }
      val jedisKyesIter = jedisKeys.iterator()
      while (jedisKyesIter.hasNext) {
        val key = jedisKyesIter.next()
        println(key + ": " + jedis.get(key))
      }
      Thread.sleep(500)
      count += 1
    }
  }
}
