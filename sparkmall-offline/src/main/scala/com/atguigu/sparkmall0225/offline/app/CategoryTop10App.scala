package com.atguigu.sparkmall0225.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall0225.offline.acc.MapAcc
import com.atguigu.sparkmall0225.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * * @Author: Wmd
  * * @Date: 2019/7/18 16:26
  */
object CategoryTop10App {


  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {

    //计算统计前10
    val acc = new MapAcc
    spark.sparkContext.register(acc)
    userVisitActionRDD.foreach(action => {
      acc.add(action)
    })

    val sortList = acc.value.toList.sortBy {
      case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
    }.take(10)

//    println(sortList)
    //2把计算得到的数据封装到样例类对象中
    val Top10CategoryCountInfo = sortList.map{
      case(cid, (c1, c2, c3)) => CategoryCountInfo(taskId, cid,c1,c2,c3)

    }
    //写到sql中
    val sql  ="insert into category_top10 values(?, ?, ?, ?, ?)"
   val args =  Top10CategoryCountInfo.map { cci =>
      Array[Any](cci.taskId,cci.categoryId,cci.clickCount, cci.orderCount, cci.payCount)
    }
//    category_top10
    JDBCUtil.executeUpdate("truncate category_top10",null)
    JDBCUtil.executeBatchUpdate(sql,args)
    Top10CategoryCountInfo



  }

}
