package bean

/**
 * @author ：Angus
 * @date ：Created in 2022/3/3 17:13
 * @description：
 */
case class SkuInfo(
                    id:String ,
                    spu_id:String ,
                    price:String ,
                    sku_name:String ,
                    tm_id:String ,
                    category3_id:String ,
                    create_time:String,
                    // 关联数据
                    var category3_name:String,
                    var spu_name:String,
                    var tm_name:String
                  )
