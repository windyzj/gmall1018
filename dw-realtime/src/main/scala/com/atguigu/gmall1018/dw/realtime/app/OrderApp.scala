package com.atguigu.gmall1018.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1018.dw.common.constant.GmallConstant
import com.atguigu.gmall1018.dw.common.util.MyEsUtil
import com.atguigu.gmall1018.dw.realtime.bean.OrderInfo
import com.atguigu.gmall1018.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_ORDER,ssc)

    val orderInfoDstream: DStream[OrderInfo] = recordDstream.map(_.value()).map { jsonString =>
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.createTime.split(" ")
      //补充时间字段
      orderInfo.createDate = createTimeArr(0)
      orderInfo.createHour = createTimeArr(1).split(":")(0)
      orderInfo.createHourMinute = createTimeArr(1).split(":")(0) + ":" + createTimeArr(1).split(":")(1)
      //脱敏
      orderInfo.consignee = orderInfo.consignee.splitAt(1)._1 + "**"
      orderInfo.consigneeTel = orderInfo.consigneeTel.splitAt(3)._1 + "********"
      orderInfo
    }

    //保存到ES
    orderInfoDstream.foreachRDD{rdd=>
      rdd.foreachPartition { orderInfoItr =>
       // println(orderInfoItr.toList.mkString("\n"))

       MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_ORDER, orderInfoItr.toList)
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
