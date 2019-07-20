package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Wmd
  * Date: 2019/7/20 11:50
  */
object RealTimeApp {
  /*def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf: SparkConf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")
    // 2. 创建 SparkContext 对象
    val sc = new SparkContext(conf)
    // 3. 创建 StreamingContext
    val ssc = new StreamingContext(sc, Seconds(1))
    // 4. 得到 DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")

    // 5. 为了方便后面的计算, 把消费到的字符串封装到对象中
    val adsInfoDStream: DStream[AdsInfo] = recordDStream.map {
      record =>
        val split: Array[String] = record.value.split(",")
        AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))

    }

    ssc.start()
    ssc.awaitTermination()


  }*/

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf: SparkConf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")
    // 2. 创建 SparkContext 对象
    val sc = new SparkContext(conf)
    // 3. 创建 StreamingContext
    val ssc = new StreamingContext(sc, Seconds(2))
    // 4. 得到 DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")

   /* val adsInfoDStream: DStream[AdsInfo] = recordDStream.map(record => {
      val msg: String = record.value()  // 取出其中的value
      val arr: Array[String] = msg.split(",")   // 切割并封装到 AdsInfo中
      AdsInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })


     需求1:
    val filteredAdsInfoDSteam: DStream[AdsInfo] = BlackListApp.filterBlackList(ssc, adsInfoDStream)
    BlackListApp.checkUserToBlackList(ssc, filteredAdsInfoDSteam)
*/
        //将DStream打印到控制台 使用map
    recordDStream.map(record => record.value()).print

    ssc.start()
    ssc.awaitTermination()

  }
}
