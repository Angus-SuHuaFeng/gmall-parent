package dws

import bean.{OrderDetail, OrderInfo, OrderWide}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.lang
import scala.collection.mutable.ListBuffer

/**
 * @author ：Angus
 * @date ：Created in 2022/3/3 21:51
 * @description： 从Kafka的DWD层读取订单和订单明细
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroup = "dwd_order_info_group"

    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    // =======================1.从Kafka中读取数据(order_info)=========================
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroup)
    if (orderInfoOffsetMap!=null && orderInfoOffsetMap.nonEmpty) {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, orderInfoGroup)
    }else {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoGroup)
    }

    var orderInfoOffsetRangesArray: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        orderInfoOffsetRangesArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // =======================1.从Kafka中读取数据(order_detail)=========================
    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroup = "dwd_order_detail_group"
    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroup)
    if (orderDetailOffsetMap!=null && orderDetailOffsetMap.nonEmpty) {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, orderDetailGroup)
    }else {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailGroup)
    }
    var orderDetailOffsetRangesArray: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        orderDetailOffsetRangesArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // 格式转换
    val orderInfoDStream: DStream[(Long, OrderInfo)] = orderInfoOffsetDStream.map {
      record: ConsumerRecord[String, String] => {
        val orderInfoJsonStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
      }
    }

    val orderDetailDStream: DStream[(Long, OrderDetail)] = orderDetailOffsetDStream.map {
      record: ConsumerRecord[String, String] => {
        val orderDetailJsonStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      }
    }
    // 开窗
    val orderInfoWindowDStream: DStream[(Long, OrderInfo)] = orderInfoDStream.window(Seconds(20), Seconds(8))
    val orderDetailWindowDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.window(Seconds(20), Seconds(8))

    // 双流Join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowDStream.join(orderDetailWindowDStream)

    // 去重(使用Redis进行去重)   key: order_join:[orderId]     value:orderDetailId        expire: 120S
    // 这里使用mapPartitions减少连接Redis的次数，一个分区连接一次
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr: Iterator[(Long, (OrderInfo, OrderDetail))] => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        // 获取Jedis客户端
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join:" + orderId
          // 返回1说明不存在
          val isNotExists: lang.Long = jedisClient.sadd(orderKey, orderDetail.id.toString)
          if (isNotExists == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedisClient.close()
        orderWideList.toIterator
      }
    }
    orderWideDStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
