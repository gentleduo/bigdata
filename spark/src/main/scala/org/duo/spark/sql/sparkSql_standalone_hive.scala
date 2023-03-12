package org.duo.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalog.Table

/**
 * @Auther:duo
 * @Date: 2023-03-12 - 03 - 12 - 14:29
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_standalone_hive {

  def main(args: Array[String]): Unit = {

    // 第一次启动的时候metastore会绑定默认的数据库，默认数据库的数据存储在spark.sql.warehouse.dir指定的目录下
    // 后面可以改变spark.sql.warehouse.dir的目录，但是如果需要将新的表存储在新的warehouse目录下，则需要创建新的数据库库，并使用use databaseNm命令改变数据库之后再创建
    // 一定要有数据库的概念
    // 其实mysql也是一样，当登录进去后其实mysql软件默认为了执行了 use default命令。
    val session = SparkSession.builder().master("local").appName("test")
      .config("spark.sql.shuffle.partitions", "1") // spark sql默认会有200的并行度，一般会调整该参数
      .config("spark.sql.warehouse.dir", "d:/spark/warehouse")
      .enableHiveSupport() //开启hive支持，在没有指定metastore的情况下，在spark进程中会启动metastore(hive集成的DERBY数据库)，并设置默认的warehouse.dir，通过下面的启动日志可以看出来：
      // Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/D:/intellij-workspace/bigdata/spark-warehouse/')
      // ObjectStore: Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes="Table,StorageDescriptor,SerDeInfo,Partition,Database,Type,FieldSchema,Order"
      // Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
      // Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
      // Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
      // Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
      // Query: Reading in results for query "org.datanucleus.store.rdbms.query.SQLQuery@0" since the connection used is closing
      // MetaStoreDirectSql: Using direct SQL, underlying DB is DERBY
      .getOrCreate()
    val context = session.sparkContext
    // context.setLogLevel("ERROR")

    //session.sql("create table t_emp(name string, age int)")
    val tables: Dataset[Table] = session.catalog.listTables()
    tables.show()
  }

}
