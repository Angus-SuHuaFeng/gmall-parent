package utils

import bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder

import java.util

/**
 * @author ：Angus
 * @date ：Created in 2022/2/23 20:58
 * @description： 使用Jest操作ES
 */
object MyESUtil {

  // 声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null

  def getJestClient(): JestClient ={
    if(jestFactory==null) {
      //创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  def build(): Unit ={
    jestFactory = new JestClientFactory()
    jestFactory.setHttpClientConfig(new HttpClientConfig
      .Builder("http://BigData1:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .build()
    )
  }
  // 向ES中插入单条数据(方法一，直接将Json数据传入)
  def putIndex(): Unit ={
    // 获取客户端连接
    val jestClient: JestClient = getJestClient()
    val source: String =
      """
        |"id":"1",
        | "name":"sea",
        | "doubanScore":"8.5",
        |   "actorList":[
        |  {"id":"1","name":"zhangyi"},
        |  {"id":"2","name":"haiqing"},
        |  {"id":"3","name":"zhang"}
        |  ]
        |""".stripMargin
    // 通过客户端对象操作
    // 创建Index类， Builder中的参数表示要插入到索引中的文档，底层会转换成Json格式的字符串，所以可以将文档封装为样例类对象
    val index: Index = new Index.Builder(source)
      .index("movie_index_idea")
      .`type`("movie")
      .id("1")
      .build()

    jestClient.execute(index)
    // 关闭连接
    jestClient.close()
  }
  // 方法二: 将插入的文档封装为样例类
  def putIndex2(): Unit ={
    val  jestClient: JestClient = getJestClient()
    // 封装样例类对象
    val actorList:util.ArrayList[util.Map[String,Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actorMap = new util.HashMap[String, Any]()
    actorMap.put("id", 66)
    actorMap.put("name","张倩倩")
    actorList.add(actorMap)
    val movie: Movie = Movie(300, "你好1988", 9.0f, actorList)
    val index: Index = new Index.Builder(movie)
      .index("movie_index_idea2")
      .`type`("movie")
      .id("2")
      .build()
    jestClient.execute(index)
    jestClient.close()
  }
  case class Movie(id:Long, name:String, doubanScore:Float, actorList: util.ArrayList[util.Map[String, Any]])

  // 根据文档ID，从ES中查询一条记录
  def queueIndexByID(): Unit ={
    val jestClient: JestClient = getJestClient()
    val get: Get = new Get.Builder("movie_index_idea2", "2").build()
    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)
    jestClient.close()
  }

  // 指定查询条件，从ES中查询多个文档 方式1
  def queryIndexByCondition1(query: String): Unit ={
    val jestClient: JestClient = getJestClient()
    // 封装search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_idea2")
      .build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val toList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(toList.mkString("\n"))
    jestClient.close()
  }

  // 指定查询条件，从ES中查询多个文档 方式2
  def queryIndexByCondition2(): Unit ={
    val jestClient: JestClient = getJestClient()
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","1988"))
    boolQueryBuilder.filter(new TermQueryBuilder("actonList","张倩倩"))
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(20)
    searchSourceBuilder.sort("doubanScore")
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString()
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_idea2")
      .build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val toList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(toList.mkString("\n"))
    jestClient.close()
  }

  /**
   *  向ES中批量插入数据
   * @param dauList 数据
   * @param indexName 索引
   */
  def bulkInsert(dauList: List[DauInfo], indexName: String): Unit = {
    if (dauList!=null && dauList.size>0){
      val jestClient: JestClient = getJestClient()
      // 创建批量操作对象
      val builder = new Bulk.Builder()
      for (source <- dauList){
        val index: Index = new Index.Builder(source)
          .index(indexName)
          .`type`("_doc")
          .build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      println("向ES中插入" + bulkResult.getItems.size() + "条数据")
      jestClient.close()
    }
  }
}
