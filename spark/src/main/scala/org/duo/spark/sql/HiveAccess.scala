package org.duo.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object HiveAccess {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    //    1. 开启 Hive 支持
    //    2. 指定 Metastore 的位置
    //    3. 指定 Warehouse 的位置
    val spark = SparkSession.builder()
      .appName("hive access1")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://server01:9083")
      // 创建student表时，指定了/location
      .config("spark.sql.warehouse.dir", "/data/hive/student")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据
    //    1. 上传 HDFS, 因为要在集群中执行, 没办法保证程序在哪个机器中执行
    //        所以, 要把文件上传到所有的机器中, 才能读取本地文件
    //        上传到 HDFS 中就可以解决这个问题, 所有的机器都可以读取 HDFS 中的文件
    //        它是一个外部系统
    //    2. 使用 DF 读取数据

    val schema = StructType(
      List(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("birthday", StringType),
        StructField("gender", StringType)
      )
    )

    val dataframe: DataFrame = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv("hdfs:///data/student")


    //val resultDF: Dataset[Row] = dataframe.where('age > 50)

    // 3. 写入数据, 使用写入表的 API, saveAsTable
    dataframe.write.mode(SaveMode.Overwrite).saveAsTable("myhive.student")
  }
}
