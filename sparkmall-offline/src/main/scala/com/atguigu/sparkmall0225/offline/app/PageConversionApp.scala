package com.atguigu.sparkmall0225.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall0225.offline.app.PageConversionApp.calcPageConversionRate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author: Wmd
  * Date: 2019/7/19 18:36
  */
object PageConversionApp {
  def calcPageConversionRate(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],targetPages:String,taskId:String): Unit ={
    // 1. 找到目标页面, 计算出来需要计算的调转流"1->2", "2->3"...
    val pages = targetPages.split(",")
    val prePages = pages.slice(0,pages.length-1)
    val postPages = pages.slice(1,pages.length)
    //进行拉链组合
    val targetPageFlows = prePages.zip(postPages).map{
      case (p1,p2 ) => p1 +"->" +p2
    }
    // 2. 先过滤出来目标页面的点击记录
  //val targetUserVisiteActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => pages.contains(uva.page_id.toString))
    val targetUserVisiteActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => pages.contains(uva.page_id.toString))
    //3.计算分母
    val targetPageCountMap: collection.Map[Long, Long] = targetUserVisiteActionRDD.map(uva => (uva.page_id,1))
      .countByKey()

    //4.计算分子: RDD[List["1->2"..]] => RDD["1->2",...] => map["1->2" -> 100, ....]
    val allTargetFlows =  targetUserVisiteActionRDD.groupBy(_.session_id).flatMap{
      case (sessionId,uvas) =>{
        //UVA
        val sortedUVAList = uvas.toList.sortBy(_.action_time)
        val pre = sortedUVAList.slice(0,sortedUVAList.length-1)
        val post = sortedUVAList.slice(1,sortedUVAList.length)

        val prePost = pre.zip(post)
        //存储的是每个session所有的跳转
        val pageFlows = prePost.map{
          case (uva1,uva2) => uva1.page_id + "->" + uva2.page_id
        }

        // 得到每个Session中所有的目标调转流
        val allTargetFlows: List[String] = pageFlows.filter(flow => targetPageFlows.contains(flow))
        allTargetFlows

      }
    }.map((_,null)).countByKey()

    //5计算跳转率
    val formatter = new DecimalFormat("0.00%")
    val resultRate: collection.Map[String, String] = allTargetFlows.map{
      case (flow,count) => {
        val page = flow.split("->")(0).toLong
        (flow,formatter.format((count.toDouble / targetPageCountMap(page))))
      }
    }

    //6.写到mysql
    val args = resultRate.map{
      case (flow,rate) => Array[Any](taskId,flow,rate)
    }
    JDBCUtil.executeUpdate("truncate page_conversion_rate",null)
    JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?, ?, ?)",args)


  }

  def main(args: Array[String]): Unit = {
    calcPageConversionRate(null, null, "1,2,3,4,5,6,7", null)
  }

}
