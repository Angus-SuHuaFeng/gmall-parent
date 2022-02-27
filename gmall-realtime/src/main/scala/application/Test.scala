package application

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import utils.MyKafkaUtil

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ：Angus
 * @date ：Created in 2022/2/26 18:07
 * @description：
 */
object Test {
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
      jsonDS.print(100)


      ssc.start()
      ssc.awaitTermination()
    }
}
object Test2 extends App{
  val long = 1645870377732L
  private val date = new Date(long)
  println(date)
}