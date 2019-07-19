package com.atguigu.sparkmall.common.util

import java.sql.PreparedStatement
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {
    
    val dataSource: DataSource = initConnection()
    
    /**
      * 初始化的连接
      */
    def initConnection() = {
        val properties = new Properties()
        val config = ConfigurationUtil("config.properties")
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
        properties.setProperty("url", config.getString("jdbc.url"))
        properties.setProperty("username", config.getString("jdbc.user"))
        properties.setProperty("password", config.getString("jdbc.password"))
        properties.setProperty("maxActive", config.getString("jdbc.maxActive"))
        DruidDataSourceFactory.createDataSource(properties)  //
    }
    /**
      * 执行单条语句
      *
      * "insert into xxx values (?,?,?)"
      */
    def executeUpdate(sql: String, args: Array[Any]) = {
        // 拿到到数据库的连接
        val conn = dataSource.getConnection
        conn.setAutoCommit(false)
        val ps: PreparedStatement = conn.prepareStatement(sql)
        if (args != null && args.length > 0) {
            (0 until args.length).foreach {
                i => ps.setObject(i + 1, args(i))
            }
        }
        ps.executeUpdate
        conn.commit()
    }
    
    /**
      * 执行批处理
      *
      * "insert into xxx values (?,?,?)"
      */
    def executeBatchUpdate(sql: String, argsList: Iterable[Array[Any]]) = {
        val conn = dataSource.getConnection
        conn.setAutoCommit(false)
        val ps: PreparedStatement = conn.prepareStatement(sql)
        argsList.foreach {
            case args: Array[Any] => {
                (0 until args.length).foreach {
                    i => ps.setObject(i + 1, args(i))
                }
                ps.addBatch()
            }
        }
        ps.executeBatch()
        conn.commit()
    }
}

