package org.duo.spark.sql

import org.apache.spark.sql.catalog.{Database, Table}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, catalog}

/**
 * @Auther:duo
 * @Date: 2023-03-11 - 03 - 11 - 21:25
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_Basic {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("test")
      //      .enableHiveSupport()//开启这个选项时：spark sql on hive才支持DDL，没开启spark只有catalog
      .getOrCreate()
    val context = session.sparkContext
    context.setLogLevel("ERROR")

    val databases: Dataset[Database] = session.catalog.listDatabases();
    databases.show()
    val tables: Dataset[Table] = session.catalog.listTables()
    tables.show()
    val functions: Dataset[catalog.Function] = session.catalog.listFunctions()
    functions.show()
    println("-----------------------------------------------------")

    val frame: DataFrame = session.read.json("./spark/data/person.json")
    frame.show()
    frame.printSchema()
    frame.createTempView("person");

    //    val frame1 = session.sql("select name from person")
    //    frame1.show();
    //    println("-----------------------------------------------------")
    //    session.catalog.listTables().show();

    import scala.io.StdIn._

    while (true) {

      val str = readLine("input your sql:")
      session.sql(str).show();
    }
  }

}
