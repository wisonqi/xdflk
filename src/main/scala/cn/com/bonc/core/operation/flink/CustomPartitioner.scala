package cn.com.bonc.core.operation.flink

import org.apache.flink.api.common.functions.Partitioner

import scala.collection.mutable

/**
 * 自定义轮询分区器<br>
 * 对于一个未被分区的key，对其分配下一个分区索引；对于一个已经被分区过的key，对其分配已经分配过的分区索引
 *
 * @author wzq
 * @date 2019-12-04
 **/
class CustomPartitioner extends Partitioner[String] {
  
  private val map: mutable.Map[String, Int] = mutable.HashMap[String, Int]()
  private var num: Int = 0
  
  override def partition(key: String, numPartitions: Int): Int = {
    if (map.contains(key)) {
      map.getOrElse(key, 0)
    } else {
      val partitionIndex = num
      map.put(key, num)
      num += 1
      if (num == numPartitions) {
        num = 0
      }
      partitionIndex
    }
  }
}
