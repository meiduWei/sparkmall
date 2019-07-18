package com.atguigu.sparkmall0225.offline.acc

import com.atguigu.sparkmall.common.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

/**
  * * @Author: Wmd
  * * @Date: 2019/7/18 16:31
  */
class MapAcc  extends AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]{
  // (cid, (clickCount, orderCount, payCount))
  var map: Map[String, (Long, Long, Long)] = Map[String, (Long, Long, Long)]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] = {
    val acc = new MapAcc
    acc.map = Map[String, (Long, Long, Long)]()
    acc
  }

  override def reset(): Unit = {
    map = Map[String, (Long, Long, Long)]()
  }

  //累加 核心代码
  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != -1) { // 点击行为
      val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(v.click_category_id.toString, (0L, 0L, 0L))
      map += v.click_category_id.toString -> (clickCount + 1, orderCount, payCount)

    } else if (v.order_category_ids != null) { // 下单行为   "1,3,2,4"
      val split: Array[String] = v.order_category_ids.split(",") // 这次下单所有的品类
      split.foreach(categoryId => {
        val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(categoryId, (0L, 0L, 0L))
        map += categoryId -> (clickCount, orderCount + 1, payCount)
      })

    } else if (v.pay_category_ids != null) { // 支付行为
      val split: Array[String] = v.pay_category_ids.split(",") // 这次下单所有的品类
      split.foreach(categoryId => {
        val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(categoryId, (0L, 0L, 0L))
        map += categoryId -> (clickCount, orderCount, payCount + 1)
      })
    }

  }

  //分区进行合并
  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {

    val acc = other.asInstanceOf[MapAcc]
    //other.asInstanceOf[MapAcc] 判断 other是否为MapAcc类型.如果是就直接将其转换为MapAcc类型.和isInstanceOf略有不同.
    acc.map.foreach {
      case (key, (count1, count2, count3)) =>
        val (c1, c2, c3) = this.map.getOrElse(key, (0L, 0L, 0L))
        this.map += key -> (count1 + c1, count2 + c2, count3 + c3)
    }
  }

  override def value: Map[String, (Long, Long, Long)] = map
}
