package org.duo.spark.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.IntegerType

object TiggerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_processor")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // 根据 Spark 提供的调试用的数据源 Rate 创建流式 DataFrame
    // Rate 数据源会定期提供一个由两列 timestamp, value 组成的数据, value 是一个随机数
    val source = spark.readStream
      .format("rate")
      .load()

    // 对 value 求 log10 即可得出其位数
    // 后按照位数进行分组, 最终就可以看到每个位数的数据有多少个
    val result = source.select(log10('value) cast IntegerType as 'key, 'value)
      .groupBy('key)
      .agg(count('key) as 'count)
      .select('key, 'count)
      .where('key.isNotNull)
      .sort('key.asc)

    // 通过 Trigger.ProcessingTime() 指定处理间隔
    result.writeStream.format("console").outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()
  }
}
