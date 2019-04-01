package com.atguigu.gmall1018.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1018.dw.common.constant.GmallConstant
import com.atguigu.gmall1018.dw.realtime.bean.StartupLog
import com.atguigu.gmall1018.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object StartUpLogApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("startuplog").setMaster("local[*]")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(5))
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP, ssc)

    /*    recordDStream.map(_.value()).foreachRDD{rdd=>
          println(rdd.collect().mkString("\n"))
        }*/
    val startupLogDstream: DStream[StartupLog] = recordDStream.map(_.value()).map { jsonString =>
      val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
      val ts: Long = startupLog.ts

      val datetimeString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts))
      val datetimeArr: Array[String] = datetimeString.split(" ")
      startupLog.logDate = datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      startupLog.logHour = timeArr(0)
      startupLog.logHourMinute = timeArr(0) + ":" + timeArr(1)
      startupLog
    }


    //根据redis中已有的当日访问用户进行过滤
//    val startuplogFilteredDstream: DStream[StartupLog] = startupLogDstream.filter { startupLog =>
//      val jedis: Jedis = new Jedis("hadoop1", 6379)
//      val key = "dau:" + startupLog.logDate
//      !jedis.sismember(key, startupLog.mid)
//    }


    val startuplogFilteredDstream: DStream[StartupLog] = startupLogDstream.transform { rdd =>
      println(s"过滤前 = ${rdd.count()}")
      val jedis: Jedis = new Jedis("hadoop1", 6379)
      val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val key = "dau:" + today
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filterRdd: RDD[StartupLog] = rdd.filter { startupLog =>
        !dauBC.value.contains(startupLog.mid)
      }
      println(s"过滤后= ${filterRdd.count()}")

      filterRdd

    }



    //把当日访问用户写入redis
    startuplogFilteredDstream.foreachRDD { rdd =>
       rdd.foreachPartition{startupLogItr=>
         val jedis: Jedis = new Jedis("hadoop1", 6379)
         for (startupLog <- startupLogItr ) {
           val key="dau:"+startupLog.logDate
           jedis.sadd(key,startupLog.mid)
         }
         jedis.close()
       }

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
