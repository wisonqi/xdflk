package cn.com.bonc.core.operation.flink.mapper

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 *
 * @author wzq
 * @date 2019-12-06
 **/
class RedisSetMapper extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }
  
  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }
  
  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
  
}
