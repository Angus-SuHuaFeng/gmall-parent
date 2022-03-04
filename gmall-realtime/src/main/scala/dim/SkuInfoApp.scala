package dim

import bean.SkuInfo
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
 * @date ：Created in 2022/3/3 17:27
 * @description：  读取商品维度数据，并关联品牌、分类、Spu，保存到 Hbase
 */
object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SupInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val topic = "ods_sku_info"
    val group = "dim_sku_info_group"
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

    // 结构转换
    val skuInfoDStream: DStream[SkuInfo] = offsetDStream.map {
      record: ConsumerRecord[String, String] => {
        val jsonStr: String = record.value()
        val skuInfo: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        skuInfo
      }
    }

    // 商品和品牌, 分类, Spu先进行关联
    val SkuInfoDStream: DStream[SkuInfo] = skuInfoDStream.transform {
      rdd: RDD[SkuInfo] => {
        rdd.cache()
        // tm_name(品牌)
        val tmSql = "select id, tm_name from gmall_base_trademark"
        val tmList: List[JSONObject] = MyPhoenixUtil.queryList(tmSql)
        val tmMap: Map[String, JSONObject] = tmList.map { tmJson: JSONObject => (tmJson.getString("ID"), tmJson) }.toMap

        // category3(分类)
        val cgSql = "select id, name, category2_id from gmall_base_category3"
        val cgList: List[JSONObject] = MyPhoenixUtil.queryList(cgSql)
        val cgMap: Map[String, JSONObject] = cgList.map { cgJson: JSONObject => {
          (cgJson.getString("ID"), cgJson)
        }
        }.toMap

        // Spu
        val spuSql = "select id, spu_name from gmall_spu_info"
        val spuList: List[JSONObject] = MyPhoenixUtil.queryList(spuSql)
        val spuMap: Map[String, JSONObject] = spuList.map { spuJson: JSONObject => {
          (spuJson.getString("ID"), spuJson)
        }
        }.toMap

        // 将三个Map放到一个List集合中,进行广播
        val BDList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](tmMap, cgMap, spuMap)
        val ListBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(BDList)

        val skuInfoRDD: RDD[SkuInfo] = rdd.map {
          skuInfo: SkuInfo => {
            // executor
            val bcList: List[Map[String, JSONObject]] = ListBC.value
            // 接收bc
            val tmMap: Map[String, JSONObject] = bcList.head
            val cgMap: Map[String, JSONObject] = bcList(1)
            val spuMap: Map[String, JSONObject] = bcList(2)

            val tmJson: JSONObject = tmMap.getOrElse(skuInfo.tm_id, null)
            if (tmJson != null) {
              skuInfo.tm_name = tmJson.getString("TM_NAME")
            }
            val cgJson: JSONObject = cgMap.getOrElse(skuInfo.category3_id, null)
            if (cgJson != null) {
              skuInfo.category3_name = cgJson.getString("NAME")
            }
            val spuJson: JSONObject = spuMap.getOrElse(skuInfo.spu_id, null)
            if (spuJson != null) {
              skuInfo.spu_name = spuJson.getString("SPU_NAME")
            }
            skuInfo
          }
        }
        skuInfoRDD
      }
    }

    // =======================2.保存数据到phoenix==========================
    import org.apache.phoenix.spark._
    SkuInfoDStream.foreachRDD{
      rdd: RDD[SkuInfo] => {
        rdd.saveToPhoenix(
          "GMALL_SKU_INFO",
          Seq("ID","SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME"),
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
