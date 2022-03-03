package utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * @author ：Angus
 * @date ：Created in 2022/2/25 18:18
 * @description： 维护偏移量的工具类
 */
object OffsetManagerUtil {
  // 从Redis中获取偏移量 type:  key:  offset:topic:groupid    field:partition  value:  偏移量
    def getOffset(topic: String, groupId: String): Map[TopicPartition,Long]={
      // 获取客户端连接
      val jedis: Jedis = MyRedisUtil.getJedisClient
      // 拼接操作Redis的Key
      var offsetKey: String = "offset:" + topic + ":" + groupId
      // 获取当前消费者组消费的主题  对应的分区以及偏移量
      val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
      jedis.close()
      // 将java的Map转换成Scala的Map
      import scala.collection.JavaConverters._
      val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
        // 读取分区和偏移量
        case (partition, offset) => {
          // Map[TopicPartition, Long]
          (new TopicPartition(topic, partition.toInt), offset.toLong)
        }
      }.toMap
      kafkaOffsetMap
    }
  // 保存偏移量
  def saveOffset(topic:String,groupId: String, offsetRanges: Array[OffsetRange])={
    // 拼接存入Redis中的key
    val offsetKey: String = "offset:" + topic + ":" + groupId
    // 定义Map集合，用于存放每个分区对应的偏移量（注意是java的Map）
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()

    for (offsetRange <- offsetRanges){
      val partition: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partition.toString, untilOffset.toString)
      println("保存分区 "+partition+": "+fromOffset+"----->"+untilOffset)
    }

    if (offsetMap!=null&&offsetMap.size()>0){
      val jedis: Jedis = MyRedisUtil.getJedisClient
      jedis.hmset(offsetKey, offsetMap)
      jedis.close()
    }
  }

}
