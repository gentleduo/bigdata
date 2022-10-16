package org.duo.spark.structured

import org.apache.spark.sql.SparkSession

object HDFSSink {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("kafka integration")
      .getOrCreate()

    import spark.implicits._

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "server01:9092,server02:9092,server03:9092")
      .option("subscribe", "streaming-bank")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val result = source.map {
      item =>
        val arr = item.replace("\"", "").split(";")
        (arr(0).toInt, arr(1).toInt, arr(5).toInt)
    }
      .as[(Int, Int, Int)]
      .toDF("age", "job", "balance")

    result.writeStream
      .format("parquet") // 也可以是 "orc", "json", "csv" 等
      .option("path", "D:\\intellij-workspace\\bigdata\\spark\\data\\streaming\\result")
      .option("checkpointLocation", "D:\\intellij-workspace\\bigdata\\spark\\data\\checkpoint")
      .start().awaitTermination()
  }
}