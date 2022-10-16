package org.duo.spark.structured

import org.apache.spark.sql.SparkSession

object KafkaSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka_source")
      .master("local[6]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "server01:9092,server02:9092,server03:9092")
      .option("subscribe", "streaming-test")
      .option("startingOffsets", "earliest")
      .load()

    import org.apache.spark.sql.streaming.OutputMode
    import org.apache.spark.sql.types._

    val eventType = new StructType().add("has_sound", BooleanType, nullable = true)
      .add("has_motion", BooleanType, nullable = true)
      .add("has_person", BooleanType, nullable = true)
      .add("start_time", DateType, nullable = true)
      .add("end_time", DateType, nullable = true)

    val camerasType = new StructType()
      .add("device_id", StringType, nullable = true)
      .add("last_event", eventType, nullable = true)

    val devicesType = new StructType()
      .add("cameras", camerasType, nullable = true)

    val schema = new StructType()
      .add("devices", devicesType, nullable = true)

    val jsonOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.sss'Z'")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 解析 Kafka 中的 JSON 数据, 这是一个重点中的重点
    // 由 Dataset 的结构可以知道 key 和 value 列的类型都是 binary 二进制, 所以要将其转为字符串, 才可进行 JSON 解析
    // source.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
    val result = source.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .select(from_json('value, schema, jsonOptions).alias("parsed_value"))
      .selectExpr("parsed_value.devices.cameras.last_event.has_person as has_person",
        "parsed_value.devices.cameras.last_event.start_time as start_time")
      .filter('has_person === true)
      .groupBy('has_person, 'start_time)
      .count()

    result.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()
  }
}
