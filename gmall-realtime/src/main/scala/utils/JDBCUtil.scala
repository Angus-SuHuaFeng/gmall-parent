package utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource
import com.alibaba.druid.pool.DruidDataSourceFactory
object JDBCUtil {
//  初始化连接池
  private val dataSource: DataSource = init()
//  初始化连接池方法
  def init()={
    val properties = new Properties()
    val config: Properties = MyPropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }
//  获取mySql连接
  def getConnect: Connection = {
    dataSource.getConnection
  }

//  执行SQL语句, 单条插入
  def executeUpdate( connection:Connection, sql: String, params: Array[Any]) :Int = {
    var pstmt: PreparedStatement = null
    var rtn = 0
    try{
      connection.setAutoCommit(false)
      pstmt =  connection.prepareStatement(sql)

      if (params!=null && params.length > 0){
//        indices Returns:
//          a Range value from 0 to one less than the length of this sequence.
        for (i <- params.indices){
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    }catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  //执行 SQL 语句,批量数据插入
  def executeBatchUpdate(connection: Connection, sql:String, paramList: Iterator[Array[Any]]): Array[Int] = {
    var rtn:Array[Int] = null
    var pstmt: PreparedStatement = null

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramList) {
        if (params!=null && params.length > 0){
          //        indices Returns:
          //          a Range value from 0 to one less than the length of this sequence.
          for (i <- params.indices){
            pstmt.setObject(i + 1, params(i))
          }
        }
//        将prepareStatement加入批处理
        pstmt.addBatch()
      }
      rtn = pstmt.executeBatch()
      connection.commit()
      pstmt.close()
    }catch {
      case e : Exception => e.printStackTrace()
    }
    rtn
  }

  //判断一条数据是否存在
  def isExist(connection: Connection, sql:String, params:Array[Any])={
    var isExist = false
    var pstmt : PreparedStatement= null

    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      isExist = pstmt.executeQuery().next()
      pstmt.close()
    }catch {
      case e : Exception => e.printStackTrace()
    }
    isExist
  }

  //获取 MySQL 的一条数据
  def getDataFromMysql(connection: Connection, sql:String, params:Array[Any]): Long ={
    var result: Long = 0L
    var pstmt : PreparedStatement= null

    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      if (resultSet.next()){
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    }catch {
      case e : Exception => e.printStackTrace()
    }
    result
  }
}


















