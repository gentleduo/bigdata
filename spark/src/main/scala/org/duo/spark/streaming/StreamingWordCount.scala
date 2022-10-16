package org.duo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    //    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // StreamingContext在创建的时候也要用sparkConf，说明StreamingContext是基于Spark Core的
    // 在执行master的时候不能指定一个线程，因为streaming运行的时候，需要开一个新的线程来去一直监听数据的获取
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val lines = ssc.socketTextStream(
      hostname = args(0),
      port = args(1).toInt,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    // main方法执行完毕后整个程序就会退出，所以需要阻塞主线程
    ssc.awaitTermination()
  }
}