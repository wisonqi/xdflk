package cn.com.bonc.core.operation.flink.kafka.connector

import java.util.Properties
import java.util.concurrent.TimeUnit

import cn.com.bonc.utils.PropertiesUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * 支持写入key-value类型数据的kafka-sink
 *
 * @param server kafka URL
 * @param topic  待写入主题
 * @author wzq
 * @date 2019-09-05
 **/
class KeyedKafkaSink(val server: String, val topic: String) extends RichSinkFunction[(String, String)] {
  
  private var producer: KafkaProducer[String, String] = _
  
  /**
   * 获取kafka生产者连接<br>
   * 暂不支持正好一次语义
   *
   * @param parameters Configuration对象
   */
  override def open(parameters: Configuration): Unit = {
    val properties: Properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, PropertiesUtils.getValue("flink.kafka.transaction.timeout"))
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "full-handle-" + Random.nextInt(10000))
    producer = new KafkaProducer[String, String](properties)
  }
  
  /**
   * 将记录输入写入kafka
   *
   * @param value   待写入数据
   * @param context sink上下文
   */
  override def invoke(value: (String, String), context: SinkFunction.Context[_]): Unit = {
    val producerRecord = new ProducerRecord[String, String](topic, value._1, value._2)
    producer.send(producerRecord)
  }
  
  override def close(): Unit = {
    producer.close(3, TimeUnit.SECONDS)
  }
  
}
