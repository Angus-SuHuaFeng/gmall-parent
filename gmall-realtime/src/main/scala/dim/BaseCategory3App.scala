package dim

import bean.BaseCategory3
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, OffsetManagerUtil}

  /**
 * @author ：Angus
 * @date ：Created in 2022/3/3 17:25
 * @description：   读取商品分类维度数据到 Hbase
 */
object BaseCategory3App {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val topic = "ods_base_category3"
    val group = "dim_base_category3_group"
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    // =======================1.从Kafka中读取数据=========================
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, group)
    if (offsetMap!=null && offsetMap.nonEmpty) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, group)
    }else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,group)
    }
    var offsetRangesArray: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        offsetRangesArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // =======================2.保存数据到phoenix==========================
    import org.apache.phoenix.spark._
    offsetDStream.foreachRDD{
      rdd: RDD[ConsumerRecord[String, String]] => {
        val BaseCategory3InfoRDD: RDD[BaseCategory3] = rdd.map {
          record: ConsumerRecord[String, String] => {
            val jsonStr: String = record.value()
            val BaseCategory3Info: BaseCategory3 = JSON.parseObject(jsonStr, classOf[BaseCategory3])
            BaseCategory3Info
          }
        }
        BaseCategory3InfoRDD.saveToPhoenix(
          "GMALL_BASE_CATEGORY3",
          Seq("ID", "NAME", "CATEGORY2_ID"),
          new Configuration(),
          Some("BigData1,BigData2,BigData3:2181")
        )
        OffsetManagerUtil.saveOffset(topic,group,offsetRangesArray)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
