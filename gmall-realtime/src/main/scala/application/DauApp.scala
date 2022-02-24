package application

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtil, MyRedisUtil}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ：Angus
 * @date ：Created in 2022/2/24 16:24
 * @description： 日活业务
 */
object DauApp {
    def main(args: Array[String]): Unit = {
      // 创建SparkStreaming环境
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      val topic:String = "gmall_start"
      val group:String = "mall_dau"
      // 通过SparkStreaming程序从Kafka中读取数据
      val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, group)
      // 从kafka中读取Json
      val jsonDS: DStream[JSONObject] = kafkaDS.map(
        (record: ConsumerRecord[String, String]) => {
          // 从value中获取json字符串
          val jsonStr: String = record.value()
          val jSONObject: JSONObject = JSON.parseObject(jsonStr)
          // 从json对象中获取时间戳数据
          val ts: Long = jSONObject.getLong("ts")
          // 将Long类型的ts转换成特定时间格式 2022-2-24 10
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timeStamp: String = sdf.format(new Date(ts))
          val tsArray: Array[String] = timeStamp.split(" ")
          val date: String = tsArray(0)
          val hour: String = tsArray(1)
          jSONObject.put("date", date)
          jSONObject.put("hour", hour)
          jSONObject
        }
      )

      // 通过Redis对采集到的启动日志进行去重操作   Set:    key:
      val filterDS: DStream[JSONObject] = jsonDS.filter {
        jsonObject: JSONObject => {
          // 获取日期
          val dateStr: String = jsonObject.getString("date")
          // 获取登录设备id
          val midStr: String = jsonObject.getJSONObject("common").getString("mid")
          // 拼接保存到Redis中的key
          val key: String = "dau:" + dateStr
          // 获取Redis客户端
          val jedis: Jedis = MyRedisUtil.getJedisClient()
          // 从redis判断是否重复
          val isFirst: lang.Long = jedis.sadd(key, midStr)

          // 设置过期时间
          // 例如今天是24日，计算25日0点的时间戳和ts相减
          val ts: lang.Long = jsonObject.getLong("ts")
          if (jedis.ttl(key) == -1) {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val time: Long = sdf.parse(dateStr).getTime
            val timeTo: Long = time + 86400 * 1000
            val l: Long = (timeTo - ts) / 1000
            val seconds: Int = l.toInt
            jedis.expire(key, seconds)
          }
          jedis.close()
          if (isFirst == 1) {
            true
          } else {
            false
          }
        }
      }
      filterDS.count().print()
      ssc.start()
      ssc.awaitTermination()


    }
}
