package utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author ：Angus
 * @date ：Created in 2022/2/24 13:35
 * @description： 读取配置文件工具类
 */
object MyPropertiesUtil {
    def load(propertiesName: String)={
      val prop = new Properties()
//      加载指定的配置文件
      prop.load(new InputStreamReader(
//        从当前线程的类加载器中
        Thread.currentThread()
          .getContextClassLoader
          .getResourceAsStream(propertiesName))
      )
      prop
    }

  def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))

  }
}
