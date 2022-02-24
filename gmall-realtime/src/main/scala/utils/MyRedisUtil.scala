package utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author ：Angus
 * @date ：Created in 2022/2/24 14:01
 * @description： 获取Redis客户端工具类
 */
object MyRedisUtil {
  // 定义一个连接对象
  private var jedisPool: JedisPool = null

  // 获取Jedis客户端
  def getJedisClient() : Jedis = {
    if (jedisPool==null){
      build()
    }
    jedisPool.getResource
  }
  // 创建jedisPool连接对象
  def build()={
    val jedisPoolConfig = new JedisPoolConfig()
    val prop = MyPropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    jedisPool = new JedisPool(jedisPoolConfig,host, port.toInt)
  }

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    println(jedis.ping())

  }
}
