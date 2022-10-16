package org.duo.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

/**
  * MySQL 的访问方式有两种: 使用本地运行, 提交到集群中运行
  *
  * 写入 MySQL 数据时, 使用本地运行, 读取的时候使用集群运行
  */
object MySQLWrite {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("mysql write")
      .getOrCreate()

    // 2. 读取数据创建 DataFrame
    //    1. 拷贝文件
    //    2. 读取
    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val df = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("hdfs://server01:8020/data/studenttab10k")

    // 3. 处理数据
    val resultDF = df.where("age < 30")

    // 4. 落地数据
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://server01:3306/test_db")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "123456")
      .mode(SaveMode.Overwrite)
      .save()
  }

}
