package dwd

import bean.{OrderDetail, SkuInfo}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, MyPhoenixUtil, OffsetManagerUtil}

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

    // 4.关联商品维度
    val orderDetailParDStream: DStream[OrderDetail] = orderDetailDStream.mapPartitions {
      orderDetailItr: Iterator[OrderDetail] => {
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        // 从订单明细中获取商品ID
        val skuIDList: List[Long] = orderDetailList.map(_.sku_id)
        // 根据商品ID到Phoenix中查询所有的商品
        val sql = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name from gmall_sku_info where id in ('${skuIDList.mkString("','")}')"
        val skuJsonObjList: List[JSONObject] = MyPhoenixUtil.queryList(sql)
        val skuInfoMap: Map[String, SkuInfo] = skuJsonObjList.map {
          (skuJsonObj: JSONObject) => {
            val skuInfo: SkuInfo = JSON.toJavaObject(skuJsonObj, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }
        }.toMap
        // 关联
        for (orderDetail <- orderDetailList) {
          val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)
          if (skuInfo != null) {
            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.category3_name = skuInfo.category3_name
            orderDetail.tm_id = skuInfo.tm_id.toLong
            orderDetail.tm_name = skuInfo.tm_name
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.spu_name = skuInfo.spu_name
          }
        }
        orderDetailList.toIterator
      }
    }

    orderDetailParDStream.print(100)
    // 将订单明细数据写回到Kafka的DWD层
    orderDetailParDStream.foreachRDD{
      rdd: RDD[OrderDetail] => {
        rdd.foreach{
          orderDetail: OrderDetail => {
            val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
            MyKafkaUtil.send(producer,"dwd_order_detail",JSON.toJSONString(orderDetail,new SerializeConfig(true)))
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
