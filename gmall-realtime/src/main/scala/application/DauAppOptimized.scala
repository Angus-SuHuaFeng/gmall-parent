package application

import bean.DauInfo
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

    /**
 * @author ：Angus
 * @date ：Created in 2022/2/25 10:08
 * @description： 实现精准一次性消费: 手动提交偏移量 + 利用ES的幂等性
 */
object DauAppOptimized {
  def main(args: Array[String]): Unit = {
    // 创建SparkStreaming环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topic:String = "gmall_start"
    val group:String = "gmall_dau"
    var recordDS: InputDStream[ConsumerRecord[String, String]] = null
    // 从Redis中获取偏移量
    val offSetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,group)
    if (offSetMap!=null&&offSetMap.nonEmpty){
      // 如果存在当前消费者组对该主题的偏移量信息，那么从偏移量的位置开始消费
       recordDS = MyKafkaUtil.getKafkaStream(topic, ssc, offSetMap, group)
    }else {
      // 如果Redis中不存在当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新的位置开始消费
      recordDS = MyKafkaUtil.getKafkaStream(topic,ssc,group)
    }

    // 获取当前采集周期从Kafka中消费的数据的起始偏移量及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangesDS: DStream[ConsumerRecord[String, String]] = recordDS.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        // 因为recordDStream底层封装的是kafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 从kafka中读取Json
    val jsonDS: DStream[JSONObject] = offsetRangesDS.map(
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
        val time: String = tsArray(1)
        jSONObject.put("date", date)
        val timeArr: Array[String] = time.split(":")
        val hour: String = timeArr(0)
        val min: String = timeArr(1)
        val sec: String = timeArr(2)
        jSONObject.put("hour", hour)
        jSONObject.put("min", min)
        jSONObject.put("sec", sec)
        jSONObject
      }
    )
    // 使用Redis对jsonDs去重
    val mapParDS: DStream[JSONObject] = jsonDS.mapPartitions {
      jsonObjectIter: Iterator[JSONObject] => {
        val jedis: Jedis = MyRedisUtil.getJedisClient
        val filterIter: Iterator[JSONObject] = jsonObjectIter.filter {
          jsonObject: JSONObject => {
            // 获取日期
            val dateStr: String = jsonObject.getString("date")
            // 获取登录设备id
            val midStr: String = jsonObject.getJSONObject("common").getString("mid")
            // 拼接保存到Redis中的key
            val key: String = "dau:" + dateStr
            // 获取Redis客户端
            val jedis: Jedis = MyRedisUtil.getJedisClient
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
        jedis.close()
        filterIter
      }
    }
    // 将数据批量保存到ES中
    mapParDS.foreachRDD{
      rdd: RDD[JSONObject] => {
        // 以分区为单位进行数据处理
        rdd.foreachPartition{
          jsonObjItr: Iterator[JSONObject] => {
            val dauList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj: JSONObject => {
                val jSONObject: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  jSONObject.getString("mid"),
                  jSONObject.getString("uid"),
                  jSONObject.getString("ar"),
                  jSONObject.getString("ch"),
                  jSONObject.getString("vc"),
                  jsonObj.getString("date"),
                  jsonObj.getString("hour"),
                  jsonObj.getString("min"),
                  jsonObj.getString("sec"),
                  jsonObj.getLong("ts")
                )
                println(dauInfo)
                (dauInfo.mid, dauInfo)
              }
            }.toList
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauList, "gmall_dau_info_" + dt)
          }
        }
        // 分批次提交偏移量
        OffsetManagerUtil.saveOffset(topic,group,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
