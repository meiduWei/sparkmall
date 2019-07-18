package com.atguigu.sparkmall0225.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.offline.acc.MapAcc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * * @Author: Wmd
  * * @Date: 2019/7/18 16:26
  */
object CategoryTop10App {


  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction]): Unit = {

    val acc = new MapAcc
    spark.sparkContext.register(acc)
    userVisitActionRDD.foreach(action => {
      acc.add(action)
    })

    val sortList = acc.value.toList.sortBy {
      case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
    }.take(10)

    println(sortList)

  }

}
