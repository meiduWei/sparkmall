package com.atguigu.sparkmall0225.offline.udf

import java.text.DecimalFormat


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * Author: Wmd
  * Date: 2019/7/19 20:09
  */
class AreaClickUDAF extends UserDefinedAggregateFunction {

  //输入数据类型 北京
  override def inputSchema: StructType = {
    StructType(StructField("city", StringType) :: Nil)
  }

  //缓冲区数据类型  Map[北京-> 100, 天津->200]  总的点击量count: Long
  override def bufferSchema: StructType = {
    StructType(StructField("city_count_map", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)
  }

  //输出数据类型
  override def dataType: DataType = StringType

  //输入一致时返回值是否一致
  override def deterministic: Boolean = true

  //缓冲的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]() //初始化缓存
    buffer(1) = 0L //初始化总的点击量

  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val cityname = input.getString(0) //获取到城市名
      val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
      //      val map = buffer.getAs[Map[String,Long]](0)   //第二种方式获取
      buffer(0) = map + (cityname -> (map.getOrElse(cityname, 0L) + 1L))
      buffer(1) = buffer.getLong(1) + 1L //更新点击量
    }
  }

  //分区间进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)) {
      val map1 = buffer1.getMap[String, Long](0)
      val map2 = buffer2.getMap[String, Long](0)
      buffer1(0) = map1.foldLeft(map2) { //把map1中的键值对的值和map2值进行合并,最后缓冲到map1中
        case (map, (city, count)) => map + (city -> (map.getOrElse(city, 0L) + count))

      }
      //总的点击量合并
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  //最终返回值
  override def evaluate(buffer: Row): Any = {
    //"北京21.2%，天津13.2%，其他65.6%"
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getAs[Long](1)
    var cityRate = map.toList.sortBy(-_._2).take(2).map{
      case (cityname,count) => CityRemark(cityname,count.toDouble / totalCount)
    }

    cityRate :+= CityRemark("其他",cityRate.foldLeft(1d)(_ - _.rate))

    cityRate.mkString(", ")
  }
}

//样例类对城市输出格式进行格式化
case class CityRemark(cityName:String,rate:Double){
  private val format = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName:${format.format(rate)}"


}
