package com.atguigu.gmall1018.dw.common.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  //batch
  //bulk
  def  executeIndexBulk(indexName:String ,list:List[Any]): Unit ={
    val jestClient: JestClient = getClient
    val bulkBuilder = new Bulk.Builder
    //把list循环加到bulk批处理中
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    for (doc <- list ) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }

    val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulkBuilder.build()).getItems
    println(s"保存ES = ${items.size()} 条")
    close(jestClient)
  }




  def main(args: Array[String]): Unit = {
    val jestClient: JestClient = getClient
    val source="{\n   \"mid\":\"mid_003\",\n   \"uid\":\"uid_004\"\n}"
    val index: Index = new Index.Builder(source).index("gmall1018_startup_log").`type`("_doc").build()
    jestClient.execute(index)
  }
}
