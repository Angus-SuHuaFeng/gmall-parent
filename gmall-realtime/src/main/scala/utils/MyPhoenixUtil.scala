package utils

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import scala.collection.mutable.ListBuffer

/**
 * @author ：Angus
 * @date ：Created in 2022/2/28 17:38
 * @description：
 */
object MyPhoenixUtil {


  def queryList(sql: String):List[JSONObject] ={

    val userStatusList = new ListBuffer[JSONObject]()

    // 注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    // 建立连接
    val con: Connection = DriverManager.getConnection("jdbc:phoenix:BigData1,BigData2,BigData3:2181")
    // 创建数据库操作对象
    val preparedStatement: PreparedStatement = con.prepareStatement(sql)
    // 执行SQL语句
    val rs: ResultSet = preparedStatement.executeQuery()
    val metaData: ResultSetMetaData = rs.getMetaData
    // 处理结果集
    while (rs.next()){
      val userStatusJSONObject = new JSONObject()
      // {"user_id":"010", "if_consumed":"1"}
      for (i <- 1 to metaData.getColumnCount) {
        userStatusJSONObject.put(metaData.getColumnName(i), rs.getObject(i))
      }
      userStatusList.append(userStatusJSONObject)
    }
    // 释放资源
    rs.close()
    preparedStatement.close()
    con.close()
    userStatusList.toList
  }
}
