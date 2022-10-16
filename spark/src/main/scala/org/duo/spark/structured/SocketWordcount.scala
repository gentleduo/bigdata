package org.duo.spark.structured

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

object SocketWordcount {

  def main(args: Array[String]): Unit = {

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_processor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // 2. 读取外部数据源, 并转为 Dataset[String]
    val source: Dataset[String] = spark.readStream
      .format("socket")
      .option("host", "192.168.56.111")
      .option("port", 9999)
      .load()
      .as[String]

    // 3. 统计词频
    val words = source.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    // 4. 输出结果
    words.writeStream.outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()
  }
}
