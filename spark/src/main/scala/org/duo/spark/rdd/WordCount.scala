package org.duo.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test

object WordCount {

  def main(args: Array[String]): Unit = {

    // 1. 创建SparkContext
    //val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context") //本地运行
    val conf = new SparkConf().setAppName("word_count") // 集群运行
    val sc = new SparkContext(conf)

    // 2. 加载文件
    //val rdd1: RDD[String] = sc.textFile("D:\\intellij-workspace\\bigdata\\spark\\data\\wordcount.txt")
    //val rdd1: RDD[String] = sc.textFile("hdfs://server01:8020/data/input/wordcount.txt")
    val rdd1: RDD[String] = sc.textFile(args(0))

    // 3. 处理
    //     1. 把整句话拆分为多个单词
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" "))
    //     2. 把每个单词指定一个词频1
    val rdd3: RDD[(String, Int)] = rdd2.map(item => (item, 1))
    //     3. 聚合
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((curr, agg) => curr + agg)

    // 4. 得到结果
    //println(rdd4.toDebugString)
    //    val result: Array[(String, Int)] = rdd4.collect()
    //    result.foreach(item => println(item))
    rdd4.saveAsTextFile(args(1))

    val rddA = sc.parallelize(Seq(1, 2, 3))
    val rddB = sc.parallelize(Seq("a", "b"))
    rddA.cartesian(rddB);
    sc.stop()
  }

  //  val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
  //  val sc = new SparkContext(conf)
  //
  //  @Test
  //  def mapTest(): Unit = {
  //    // 1. 创建 RDD
  //    val rdd1 = sc.parallelize(Seq(1, 2, 3))
  //    // 2. 执行 map 操作
  //    val rdd2 = rdd1.map(item => item * 10)
  //    // 3. 得到结果
  //    val result = rdd2.collect()
  //    result.foreach(item => println(item))
  //
  //    sc.stop()
  //  }
}