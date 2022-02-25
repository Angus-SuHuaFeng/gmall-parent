package bean

  /**
 * @author ：Angus
 * @date ：Created in 2022/2/25 11:36
 * @description： 日志数据样例类
 */
case class DauInfo(
                    mid:String,//设备 id
                    uid:String,//用户 id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    ts:Long //时间戳
                  ){}
