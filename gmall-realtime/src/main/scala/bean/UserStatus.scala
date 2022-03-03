package bean

/**
 * @author ：Angus
 * @date ：Created in 2022/2/28 17:36
 * @description：
 */
case class UserStatus(
                     userid:String,         // 用户ID
                     jspIdConsumed:String   // 是否消费过， 1首单  0 非首单
                     )
