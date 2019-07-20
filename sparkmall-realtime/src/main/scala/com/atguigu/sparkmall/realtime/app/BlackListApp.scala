package com.atguigu.sparkmall.realtime.app

import java.util

import com.atguigu.sparkmall.common.util.RedisUtil
import com.atguigu.sparkmall.realtime.bean.AdsInfo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Author: Wmd
  * Date: 2019/7/20 11:29
  */
object BlackListApp {
  val dayUserAdsCount = "day:userId:adsId"
  val blackList = "blacklist"


  //
  def checkUserToBlackList(ssc: StreamingContext, adsInfoDStream: DStream[AdsInfo]): Unit = {
    //redis连接 ×
    adsInfoDStream.foreachRDD(rdd => {
      //redis连接  ×
      rdd.foreachPartition(adsInfoIt => {
        //redis连接  √
        val client = RedisUtil.getJedisClient
        //1.统计每天用户每广告的点击量
        adsInfoIt.foreach(adsInfo => {
          println(adsInfo.userId)
          val field = s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}"
          //返回值就是增加后的值
          val clickCount = client.hincrBy(dayUserAdsCount, field, 1)

          if (clickCount.toLong > 100) {
            client.sadd(blackList, adsInfo.userId)
          }
        })

        client.close()

      })

    })


  }

  def filterBlackList(ssc: StreamingContext, adsInfoDStream: DStream[AdsInfo]): DStream[AdsInfo] = {
    // 黑名单在实时更新, 所以获取黑名单也应该实时的获取, 一个周期获取一次黑名单
    adsInfoDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val blackBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(client.smembers(blackList))
      client.close()
      rdd.filter(adsInfo => {
        val blackListIds: util.Set[String] = blackBC.value
        !blackListIds.contains(adsInfo.userId)
      })
    })
  }

}
