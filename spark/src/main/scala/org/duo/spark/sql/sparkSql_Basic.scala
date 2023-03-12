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

    // Catalog
    // 在关系数据库中，Catalog是一个宽泛的概念，通常可以理解为一个容器或数据库对象命名空间中的一个层。
    // 在SparkSQL系统中，Catalog主要用于各种函数资源信息和元数据信息（数据库、数据表、数据视图、数据分区与函数等）的统一管理。
    // SparkSession默认按照in-memory的方式创建Catalog，但是如果调用SparkSession.enableHiveSupport()，会将其改为“hive”
    // 由于InMemoryCatalog会将元数据全部储存在内存之中，使用一个HashMap类型，储存数据库元信息。所以此时调用DDL语句会报错
    // session.sql("create table t_emp(name string, age int)")
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
