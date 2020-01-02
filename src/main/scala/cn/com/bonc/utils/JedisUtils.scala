package cn.com.bonc.utils

import java.util

import redis.clients.jedis._

/**
 * redis操作工具类<br>
 * 由于每次操作完之后需要将连接放回连接池，而放回连接池的方法权限为protected，因此需要继承JedisPool
 *
 * @author wzq
 * @date 2019-08-22
 **/
object JedisUtils extends JedisPool {
  
  private val poolConfig = new JedisPoolConfig()
  poolConfig.setBlockWhenExhausted(true)
  poolConfig.setTestOnBorrow(true)
  poolConfig.setTestOnCreate(true)
  poolConfig.setMaxTotal(50)
  val shards = new util.ArrayList[JedisShardInfo]()
  val hosts: Array[String] = PropertiesUtils.getValue("redis.host").split(',')
  for (host <- hosts) {
    val ipPort = host.split(':')
    shards.add(new JedisShardInfo(ipPort(0), ipPort(1)))
  }
  private val jedisPool: ShardedJedisPool = new ShardedJedisPool(poolConfig, shards)
  
  
  /**
   * 获取jdeis对象
   *
   * @return jdeis对象
   */
  def getJedis: ShardedJedis = {
    jedisPool.getResource
  }
  
  /**
   * 将给定的key-value设置到redis中<br>
   * 注意：如果是批量设置，则应该手动获取连接池和连接，用完之后手动将连接放回连接池
   *
   * @param key   key
   * @param value value
   */
  def setKeyValue(key: String, value: String): Unit = {
    val jedis = getJedis
    jedis.set(key, value)
    jedis.close()
  }
  
  /**
   * 获取指定key对应的value值
   *
   * @param key 指定的key
   * @return value值，如果没有找到，则返回空字符串
   */
  def getValue(key: String): String = {
    val jedis = getJedis
    val result = jedis.get(key)
    jedis.close()
    if (result != null) {
      result
    } else {
      ""
    }
  }
  
  
}
