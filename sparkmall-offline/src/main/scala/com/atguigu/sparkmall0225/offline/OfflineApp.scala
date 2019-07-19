package com.atguigu.sparkmall0225.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall0225.offline.app._
import com.atguigu.sparkmall0225.offline.util.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * * @Author: Wmd
  * * @Date: 2019/7/18 15:43
  */
object OfflineApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession.builder()
      .appName("OfflineApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("hdfs://hadoop102:9000/sparkmall")

    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readCondition)

    userVisitActionRDD.cache()
    userVisitActionRDD.checkpoint()
    //userVisitActionRDD.take(10).foreach(println)
    val taskId = UUID.randomUUID().toString

    //需求1
    // val categoryCountTop10 = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD,taskId)

    //需求2
    // CategorySessionTop10.statCategoryTop10Session(spark,categoryCountTop10,userVisitActionRDD,taskId)

    //需求3
    //  PageConversionApp.calcPageConversionRate(spark, userVisitActionRDD, readCondition.targetPageFlow, taskId)

    //需求4
    AreaProductTop3.statAreaProductTop3(spark,taskId)


  }

  def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
    var sql =
      s"""
         |select
         | v.*
         |from user_visit_action v join user_info u on v.user_id=u.user_id
         |where 1=1
             """.stripMargin

    if (isNotEmpty(condition.startDate)) {
      sql += s" and date>='${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and date<='${condition.endDate}'"
    }
    if (condition.startAge > 0) {
      sql += s" and u.age>=${condition.startAge}"
    }
    if (condition.endAge > 0) {
      sql += s" and u.age<=${condition.endAge}"
    }
    import spark.implicits._
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd

  }

  /**
    * 读取过滤的条件
    *
    * @return
    */
  def readCondition: Condition = {
    val conditionString = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(conditionString, classOf[Condition])
  }

}
