package dim

import bean.UserInfo
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

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ：Angus
 * @date ：Created in 2022/3/2 18:15
 * @description： 从kafka中读取数据，通过phoenix保存到HBase
 */
object UserInfoApp {
   def main(args: Array[String]): Unit = {
     //1 基本环境
     val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")
     val ssc = new StreamingContext(sparkConf, Seconds(3))
     val topic = "ods_user_info"
     val groupId = "gmall_user_info_group"
     //2 获取偏移量
     var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
     val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
     if (offsetMap!=null && offsetMap.nonEmpty) {
       recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
     }else{
       recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
     }
     //3 获取本次消费数据的偏移量情况
     var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
     val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
       rdd: RDD[ConsumerRecord[String, String]] => {
         offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
         rdd
       }
     }
     //4 对从kafka得到的数据进行结构转换  record(kv) => UserInfo
     val userInfoDStream: DStream[UserInfo] = offsetDStream.map {
       record: ConsumerRecord[String, String] => {
         val jsonStr: String = record.value()
         val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
         val sdf = new SimpleDateFormat("yyyy-MM-dd")
         val date: Date = sdf.parse(userInfo.birthday)
         val curTS: Long = System.currentTimeMillis()
         val betweenMs: Long = curTS - date.getTime
         val age = betweenMs/1000L/60L/60L/24L/365L
         if (age<20){
           userInfo.age_group = "20岁以下"
         }else if (age > 30) {
           userInfo.age_group = "30岁以上"
         }else {
           userInfo.age_group = "20岁-30岁"
         }
         if (userInfo.gender == "M") {
           userInfo.gender_name = "男"
         }else {
           userInfo.gender_name = "女"
         }
         userInfo
       }
     }
     //5 保存到Phoenix中并提交偏移量
     import org.apache.phoenix.spark._
     userInfoDStream.foreachRDD{
       rdd: RDD[UserInfo] => {
         rdd.saveToPhoenix(
           "GMALL_USER_INFO",
           Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
           new Configuration(),
           Some("BigData1,BigData2,BigData3:2181")
         )
         OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
       }
     }
     ssc.start()
     ssc.awaitTermination()
   }
}
