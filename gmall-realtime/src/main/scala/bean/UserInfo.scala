package bean

/**
 * @author ：Angus
 * @date ：Created in 2022/3/1 22:38
 * @description：
 */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段
                     var gender_name:String//性别
                   )
