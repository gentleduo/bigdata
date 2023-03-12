package org.duo.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * @Auther:duo
 * @Date: 2023-03-12 - 03 - 12 - 13:57
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_jdbc {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("test")
      .config("spark.sql.shuffle.partitions", "1") // spark sql默认会有200的并行度，一般会调整该参数
      //      .enableHiveSupport()//开启这个选项时：spark sql on hive才支持DDL，没开启spark只有catalog
      .getOrCreate()
    val context = session.sparkContext
    context.setLogLevel("ERROR")

    val properties = new Properties()
    properties.put("url", "jdbc:mysql://server01:3306/test_db")
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    // 通过SparkSession读取数据源拿到的都说DataSet或者DataFrame
    val frame: DataFrame = session.read.jdbc(properties.get("url").toString, table = "employee", properties)
    frame.show();

    frame.createTempView("employee")
    session.sql("select count(*) from employee").show()

    // 将frame中的数据写入数据库中
    frame.write.jdbc(properties.get("url").toString, table = "employee_spark", properties)
  }
}
