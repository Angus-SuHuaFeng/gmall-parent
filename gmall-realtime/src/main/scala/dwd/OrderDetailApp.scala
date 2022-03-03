package dwd

import bean.OrderDetail
import com.alibaba.fastjson.JSON
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
 * @date ：Created in 2022/3/3 16:40
 * @description： 创建读取订单明细数据的类 OrderDetailApp
 */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val topic = "ods_order_detail"
    val groupId = "order_detail_group"

    // 1.读取流/加载偏移量
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap!=null && offsetMap.nonEmpty) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    // 2.获取偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 3.读取数据
    val orderDetailDStream: DStream[OrderDetail] = offsetDStream.map {
      record: ConsumerRecord[String, String] => {
        val orderDetailJson: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
        orderDetail
      }
    }

    // 4.关联商评维度

    orderDetailDStream.print(100)







    ssc.start()
    ssc.awaitTermination()
  }

}
