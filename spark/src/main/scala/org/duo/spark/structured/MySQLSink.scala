package org.duo.spark.structured

import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}

import java.sql.{Connection, DriverManager, Statement}

object MySQLSink {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("MySQL integration")
      .getOrCreate()

    import spark.implicits._

    val source: Dataset[String] = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "server01:9092,server02:9092,server03:9092")
      .option("subscribe", "streaming-bank")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val result: DataFrame = source.map {
      item =>
        val arr = item.replace("\"", "").split(";")
        (arr(0).toInt, arr(1).toInt, arr(5).toInt)
    }
      .as[(Int, Int, Int)]
      .toDF("age", "job", "balance")

    class MySQLWriter extends ForeachWriter[Row] {

      val driver = "com.mysql.jdbc.Driver"
      var statement: Statement = _
      var connection: Connection = _
      val url: String = "jdbc:mysql://server01:3306/streaming-bank-result"
      val user: String = "root"
      val pwd: String = "123456"

      override def open(partitionId: Long, version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        this.statement = connection.createStatement
        true
      }

      override def process(value: Row): Unit = {
        statement.executeUpdate(s"insert into bank values(" +
          s"${value.getAs[Int]("age")}, " +
          s"${value.getAs[Int]("job")}, " +
          s"${value.getAs[Int]("balance")} )")
      }

      override def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    }

    result.writeStream
      .foreach(new MySQLWriter)
      .start()
      .awaitTermination()
  }
}