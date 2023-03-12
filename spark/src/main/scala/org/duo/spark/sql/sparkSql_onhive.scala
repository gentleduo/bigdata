package org.duo.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @Auther:duo
 * @Date: 2023-03-12 - 03 - 12 - 15:46
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_onhive {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("test")
      .config("spark.sql.shuffle.partitions", "1") // spark sql默认会有200的并行度，一般会调整该参数
      .config("hive.metastore.uris", "thrift://server02:9083")
      .enableHiveSupport()
      .getOrCreate()

    val context = session.sparkContext
    context.setLogLevel("ERROR")

    session.catalog.listTables().show();
    session.sql("select * from buckettest").show()
    //session.sql("insert into tmp values (123,13,98,23,3,12)")

    val frame = session.sql("show tables")
    frame.show();
  }
}
