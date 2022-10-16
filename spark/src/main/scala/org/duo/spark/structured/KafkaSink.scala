package org.duo.spark.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object KafkaSink {

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
      .option("subscribe", "streaming-test")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val result = source.map {
      item =>
        val arr = item.replace("\"", "").split(";")
        (arr(0).toInt, arr(1).toString, arr(5).toString)
    }
      .as[(Int, String, String)]
      .toDF("age", "job", "balance")

    result.writeStream
      .format("kafka")
      .outputMode(OutputMode.Append())
      .option("kafka.bootstrap.servers", "server01:9092,server02:9092,server03:9092")
      .option("checkpointLocation", "D:\\intellij-workspace\\bigdata\\spark\\data\\checkpoint")
      .option("topic", "streaming-bank")
      .start().awaitTermination()
  }
}
