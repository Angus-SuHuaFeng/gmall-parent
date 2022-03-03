package dwd

import bean.{OrderInfo, ProvinceInfo, UserStatus}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, MyPhoenixUtil, OffsetManagerUtil}

/**
 * @author ：Angus
 * @date ：Created in 2022/2/28 19:35
 * @description： 从kafka中读取订单数据，并对其进行处理
 */
object OrderInfoApp {
    def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderInfoApp")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      val topic = "ods_order_info"
      val groupId = "order_info_group"

      // 从Redis中获取偏移量
      val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
      var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
      if (offsetMap != null && offsetMap.nonEmpty) {
        recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
      }else {
        recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
      }

      // 获取偏移量
      var offsetRanges: Array[OffsetRange] = null
      val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
        rdd: RDD[ConsumerRecord[String, String]] => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
      // 数据转换
      val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
        record: ConsumerRecord[String, String] => {
          val jsonStr: String = record.value()
          val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
          val create_time: String = orderInfo.create_time
          val createTimeArr: Array[String] = create_time.split(" ")
          val create_date: String = createTimeArr(0)
          val create_hour: String = createTimeArr(1)
          orderInfo.create_date = create_date
          orderInfo.create_hour = create_hour
          orderInfo
        }
      }

      // ===========================判断是否为首单 方案1==========================
      // 每一条订单数据都要执行一个SQL语句，性能消耗太大
      val firstOrderFlagDStream: DStream[Unit] = orderInfoDStream.map {
        orderInfo: OrderInfo => {
          // 获取用户ID
          val user_id: Long = orderInfo.user_id
          // 根据用户id通过Phoenix查询Hbase中的数据，判断是否下过单
          val sql = s"select user_id , if_consumed from user_status where userid = '${user_id}' "
          val userStatusInfo: List[JSONObject] = MyPhoenixUtil.queryList(sql)
          if (userStatusInfo != null && userStatusInfo.nonEmpty) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
      }

      // ===========================首单  方案二=================================
      // 以分区为单位，将整个分区的数据拼接成一条SQL进行查询
      val firstOrderDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
        orderInfoItr: Iterator[OrderInfo] => {
          // 当前分区的所有订单
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          // 获取当前分区中下单的用户
          val user_id_list: List[Long] = orderInfoList.map(_.user_id)
          // 根据用户id的集合通过phoenix到Hbase查询是否首单     SQL拼接时注意，集合中需要String类型
          val sql = s"select USER_ID, IF_CONSUMED from user_status where user_id in ('${user_id_list.mkString("','")}')"
          val userStatusInfo: List[JSONObject] = MyPhoenixUtil.queryList(sql)
          // 获取消费过的用户的ID    坑: 在phoenix中字段名都为大写
          val consumedUserIDList: List[String] = userStatusInfo.map(_.getString("USER_ID"))
          for (orderInfo <- orderInfoList) {
            // orderInfo.user_id 是Long类型， 需要先转换成String
            if (consumedUserIDList.contains(orderInfo.user_id.toString)) {
              orderInfo.if_first_order = "0"
            } else {
              orderInfo.if_first_order = "1"
            }
          }
          orderInfoList.toIterator
        }
      }
      /*一个采集周期状态修正：
           应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
           ◼ 同一采集周期的同一用户-----按用户分组（groupByKey）
           ◼ 最早的订单-----排序，取最早（sortwith）
           ◼ 标记为首单-----具体业务代码
       */
      // 对待处理数据进行数据转换orderInfo ======> (user_id,orderInfo)
      val mapDStream: DStream[(Long, OrderInfo)] = firstOrderDStream.map((orderInfo: OrderInfo) => (orderInfo.user_id, orderInfo))
      //根据用户id对数据进行分组
      val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()
      val correctionDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
        case (user_id, orderInfoItr) => {
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          // 判断在一个采集周期中,用户是否下了多单
          if (orderInfoList != null && orderInfoList.size > 1) {
            // 对于下了多个订单,按照下单时间进行排序
            val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
              (orderInfo1: OrderInfo, orderInfo2: OrderInfo) => {
                orderInfo1.create_time < orderInfo2.create_time
              }
            }
            // 取出集合中第一个元素
            if (sortedOrderInfoList.head.if_first_order == "1") {
              // 时间最早的首单状态为1,其他都设置为0
              for (i <- 1 until sortedOrderInfoList.size) {
                sortedOrderInfoList(i).if_first_order = "0"
              }
            }
            sortedOrderInfoList
          } else {
            orderInfoItr.toList
          }

        }
      }

      // ==========================维护首单用户状态===========================
      // 如果当前用户为首单用户，那么我们应该将其标记后保存到HBase中，这样下次下单时就不是首单
      import org.apache.phoenix.spark._
      correctionDStream.foreachRDD {
        rdd: RDD[OrderInfo] => {
          // 过滤非首单的用户
          val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
          // 使用saveToPhoenix方法要求RDD的属性个数和Phoenix中的字段必须一致
          val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map {
            orderInfo: OrderInfo => {
              UserStatus(orderInfo.user_id.toString, "1")
            }
          }
          userStatusRDD.saveToPhoenix(
            "user_status",
            Seq("USER_ID", "IF_CONSUMED"),
            // Hadoop的配置
            new Configuration,
            Some("BigData1,BigData2,BigData3:2181")
          )
          OffsetManagerUtil.saveOffset(topic, groupId,offsetRanges)
        }
      }

      // ======================和省份维度表进行关联(方案一)=======================
      // 以分区为单位对订单进行处理，和Phoenix中的订单进行关联
//      val orderInfoWithProvinceDStream: DStream[OrderInfo] = correctionDStream.mapPartitions {
//        orderInfoItr: Iterator[OrderInfo] => {
//          // 转换成List
//          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
//          // 获取当前分区中订单对应的省份id
//          val provinceIdList: List[Long] = orderInfoList.map((_: OrderInfo).province_id)
//          // 根据省份id到phoenix中查询对应省份
//          var sql: String = s"select ID,NAME,AREA_CODE,ISO_CODE from gmall_province_info where ID in ('${provinceIdList.mkString("','")}')"
//          val provinceInfoList: List[JSONObject] = MyPhoenixUtil.queryList(sql)
//          val provinceMap: Map[String, ProvinceInfo] = provinceInfoList.map(
//            (provinceJsonObj: JSONObject) => {
//              // 将json对象转换成样例类对象
//              val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
//              (provinceInfo.id, provinceInfo)
//            }
//          ).toMap
//          for (order <- orderInfoList) {
//            val provinceInfo: ProvinceInfo = provinceMap.getOrElse(order.province_id.toString, null)
//            if (provinceInfo != null) {
//              order.province_name = provinceInfo.name
//              order.province_iso_code = provinceInfo.iso_code
//              order.province_area_code = provinceInfo.area_code
//            }
//          }
//          orderInfoList.toIterator
//        }
//      }

      // ======================和省份维度表进行关联(方案二)=======================
      val orderInfoWithProvinceDStream: DStream[OrderInfo] = correctionDStream.transform {
        rdd: RDD[OrderInfo] => {
          // 从Phoenix中查询所有的省份数据
          val sql = "select ID,NAME,AREA_CODE,ISO_CODE from gmall_province_info"
          val provinceInfoList: List[JSONObject] = MyPhoenixUtil.queryList(sql)
          val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
            provinceInf: JSONObject => {
              // 将json对象转换为省份样例类对象
              val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceInf, classOf[ProvinceInfo])
              (provinceInfo.id, provinceInfo)
            }
          }.toMap
          // 定义广播变量
          val provinceInfBDMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)
          rdd.map {
            orderInfo: OrderInfo => {
              val provinceInfo: ProvinceInfo = provinceInfBDMap.value.getOrElse(orderInfo.province_id.toString, null)
              if (provinceInfo != null) {
                orderInfo.province_name = provinceInfo.name
                orderInfo.province_area_code = provinceInfo.area_code
                orderInfo.province_iso_code = provinceInfo.iso_code
              }
              orderInfo
            }
          }
        }
      }
      orderInfoWithProvinceDStream.print(100)



//      orderInfoWithProvinceDStream.print(100)
      ssc.start()
      ssc.awaitTermination()

    }
}
