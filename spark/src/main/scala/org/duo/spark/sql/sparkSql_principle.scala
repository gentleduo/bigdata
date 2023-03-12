package org.duo.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}


/**
 * @Auther:duo
 * @Date: 2023-03-11 - 03 - 11 - 21:50
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_principle {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("test")
      //      .enableHiveSupport()//开启这个选项时：spark sql on hive才支持DDL，没开启spark只有catalog
      .getOrCreate()
    val context = session.sparkContext
    context.setLogLevel("ERROR")

    // 数据 + 元数据 == df
    // 1 数据：RDD[Row]
    val rdd: RDD[String] = context.textFile("./spark/data/person.txt")
    //rdd.map(_.split(" ")).map(arr=>(arr(0),arr(1))).foreach(arr=>println(arr._2));
    val rddRow: RDD[Row] = rdd.map(_.split(" ")).map(arr => Row.apply(arr(0), arr(1).toInt))


    // 2 元数据：StructType
    val fields = Array(
      StructField.apply("name", DataTypes.StringType, true),
      StructField.apply("age", DataTypes.IntegerType, true)
    )
    val schema: StructType = StructType.apply(fields)

    val dataframe: DataFrame = session.createDataFrame(rddRow, schema)
    dataframe.show();
    dataframe.printSchema();
    dataframe.createTempView("person")
    session.sql("select * from person").show()

    //import session.implicits._

    val ds01: Dataset[String] = session.read.textFile("./spark/data/person.txt")
    val person: Dataset[(String, Int)] = ds01.map(line => {
      val strs: Array[String] = line.split(" ")
      (strs(0), strs(1).toInt)
    })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))
    val frame: DataFrame = person.toDF("name", "age")
    frame.show()
    frame.printSchema()
  }


}