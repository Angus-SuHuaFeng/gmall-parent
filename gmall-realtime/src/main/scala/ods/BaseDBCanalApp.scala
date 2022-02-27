package ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, OffsetManagerUtil}

  /**
 * @author ：Angus
 * @date ：Created in 2022/2/27 14:39
 * @description： 将Canal采集到Kafka中的数据进行分流
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BaseDBCanalApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topic = "gmall_db_c"
    val groupId = "base_db_canal_group"

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    //    从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.nonEmpty){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else {
      // 没有偏移量则从最新的位置消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    // 获取当前批次读取的Kafka主题中的偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // 对接收到的数据进行格式转换
    val jsonObjectDStream: DStream[JSONObject] = offsetDStream.map {
      record: ConsumerRecord[String, String] => {
        val jsonStr: String = record.value()
        val jSONObject: JSONObject = JSON.parseObject(jsonStr)
        jSONObject
      }
    }

    // 分流，根据不同的表名，将数据发送到不同的kafka主题中去
    jsonObjectDStream.foreachRDD{
      rdd: RDD[JSONObject] => {
        rdd.foreach{
          jsonObj: JSONObject => {
            // 获取更新操作类型
            val operationType: String = jsonObj.getString("type")
            if ("INSERT".equals(operationType)) {
              // 获取更新的表名
              val tableName: String = jsonObj.getString("table")
              // 获取更新的数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              // 拼接topic名称
              val topic: String = "ods_" + tableName
              // 根据表明将数据发送到不同的topic
              // dataArr是java类型的数组，所以需要转换成scala类型
              import scala.collection.JavaConverters._
              for (data <- dataArr.asScala) {
                val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                MyKafkaUtil.send(producer, topic, data.toString)
              }
            }
          }
        }
        // 提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
