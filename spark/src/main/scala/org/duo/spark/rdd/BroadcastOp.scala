package org.duo.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastOp {

  def main(args: Array[String]): Unit = {

    // 1. 创建SparkContext
    val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context") //本地运行
    val sc = new SparkContext(conf)

    val pws = Map("Apache Spark" -> "http://spark.apache.org/", "Scala" -> "http://www.scala-lang.org/")
    val pwsB = sc.broadcast(pws)
    val websites = sc.parallelize(Seq("Apache Spark", "Scala")).map(pwsB.value(_)).collect

    websites.foreach(println(_))
    pwsB.unpersist()
    pwsB.destroy()
    sc.stop();
  }

}
