package org.duo.spark.sql

import org.apache.spark.sql.SparkSession

object UDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("window")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val source = Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra thin", "Cell phone", 5000),
      ("Very thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    ).toDF("product", "category", "revenue")

    import org.apache.spark.sql.functions._

    val toStrUDF = udf(toStr _)
    source.select('product, 'category, toStrUDF('revenue)).show()
  }

  def toStr(revenue: Long): String = {
    (revenue / 1000) + "K"
  }
}