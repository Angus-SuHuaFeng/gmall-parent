package ods

import com.alibaba.fastjson.{JSON, JSONObject}
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
 * @date ：Created in 2022/2/27 22:08
 * @description： 通过Maxwell将数据导入Kafka再进行分流处理
 */
object BaseDBMaxwellApp {
    def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BaseDBMaxwellApp")
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      val topic = "gmall_db_maxwell"
      val groupId = "base_db_maxwell_group"
      // 从Redis中获取偏移量
      var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
      val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
      if (offsetMap != null && offsetMap.nonEmpty){
        recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
      }else {
        recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
      }
      // 获取当前批次读取的Kafka主题中的偏移量信息
      var offsetRangesArray: Array[OffsetRange] = null
      val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
        rdd: RDD[ConsumerRecord[String, String]] => {
          offsetRangesArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
      // 处理kafka数据，进行格式转换
      val jsonDStream: DStream[JSONObject] = offsetDStream.map {
        record: ConsumerRecord[String, String] => {
          val jsonStr: String = record.value()
          val jSONObject: JSONObject = JSON.parseObject(jsonStr)
          jSONObject
        }
      }
      // 对数据进行分流
      jsonDStream.foreachRDD{
        rdd: RDD[JSONObject] => {
          rdd.foreach{
            jsonObj: JSONObject => {
              val operationType: String = jsonObj.getString("type")
              val tableName: String = jsonObj.getString("table")
              val dataJSONObject: JSONObject = jsonObj.getJSONObject("data")
//              if(dataJSONObject!=null && !dataJSONObject.isEmpty) {
//                if (
//                  ("order_info".equals(tableName) && "bootstrap-insert".equals(operationType))
//                    || (tableName.equals("order_detail") && "bootstrap-insert".equals(operationType))
//                    || tableName.equals("base_province")
//                    || tableName.equals("user_info")
//                    || tableName.equals("sku_info")
//                    || tableName.equals("base_trademark")
//                    || tableName.equals("base_category3")
//                    || tableName.equals("spu_info")
//                )
//              }
              // maxwell中为小写
              if ("bootstrap-insert".equals(operationType) || "update".equals(operationType) ||
                  "insert".equals(operationType) && !dataJSONObject.isEmpty && dataJSONObject !=null){
                // 拼接要发送的主题
                val topicName: String = "ods_" + tableName
                val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                MyKafkaUtil.send(producer, topicName, dataJSONObject.toString())
              }
            }
          }
          OffsetManagerUtil.saveOffset(topic,groupId,offsetRangesArray)
        }
      }
      ssc.start()
      ssc.awaitTermination()
    }
}
