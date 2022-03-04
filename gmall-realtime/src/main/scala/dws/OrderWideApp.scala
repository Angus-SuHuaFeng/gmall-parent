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
    // =======================2.双流Join=========================
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
    // =======================3.实付分摊=========================
    val orderWideDetailDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr: Iterator[OrderWide] => {
        val orderWideList: List[OrderWide] = orderWideItr.toList
        // 获取Redis连接
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        for (orderWide <- orderWideList) {
          // 从redis中获取明细累加和
          val order_origin_sum_key: String = "order_origin_sum:" + orderWide.order_id
          var order_origin_sum: Double = 0D
          val orderOriginSum: String = jedisClient.get(order_origin_sum_key)
          // Redis中获取字符串必须要进行非空判断
          if (orderOriginSum != null && orderOriginSum.nonEmpty) {
            order_origin_sum = orderOriginSum.toDouble
          }
          // 从Redis中获取实付分摊累加和
          val order_detail_sum_key: String = "order_split_sum:" + orderWide.order_id
          var order_detail_sum: Double = 0D
          val orderDetailSum: String = jedisClient.get(order_detail_sum_key)
          if (orderDetailSum != null && orderDetailSum.nonEmpty) {
            order_detail_sum = orderDetailSum.toDouble
          }

          val OriginAmount: Double = orderWide.sku_price * orderWide.sku_num
          // 判断是否最后一条明细并进行实付分摊
          if (OriginAmount == orderWide.original_total_amount - order_origin_sum) {
            // 最后一条明细
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - order_detail_sum) * 100D) / 100D
          } else {
            orderWide.final_detail_amount = orderWide.final_total_amount * OriginAmount / orderWide.original_total_amount
          }
          // 更新Redis中的值
          val newOriginAmount: Double = order_origin_sum + OriginAmount
          val newDetailSum: Double = order_detail_sum + orderWide.final_detail_amount
          jedisClient.setex(order_origin_sum_key, 600, newOriginAmount.toString)
          jedisClient.setex(order_detail_sum_key, 600, newDetailSum.toString)
        }
        jedisClient.close()
        orderWideList.toIterator
      }
    }

    orderWideDetailDStream.print(100)



    ssc.start()
    ssc.awaitTermination()
  }
}
