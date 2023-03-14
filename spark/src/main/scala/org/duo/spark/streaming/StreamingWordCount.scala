package org.duo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    //    if (args.length < 2) {
    //      System.err.println("Usage: NetworkWordCount <hostname> <port>")
    //      System.exit(1)
    //    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[5]")
    //    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // StreamingContext在创建的时候也要用sparkConf，说明StreamingContext是基于Spark Core的
    // 在执行master的时候不能指定一个线程，因为streaming运行的时候，需要开一个新的线程来去一直监听数据的获取
    val ssc = new StreamingContext(sparkConf, Seconds(1)) // 默认win：1s，slide：1s
    ssc.sparkContext.setLogLevel("ERROR")

    // 在linux服务器上使用nc命令，实现tcp方式监听服务器端8888：nc -l 8888
    val lines = ssc.socketTextStream(
      //      hostname = args(0),
      //      port = args(1).toInt,
      "192.168.56.110",
      8888,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER)

    val wordCountMap: DStream[(String, Int)] = lines.map(_.split(" ")).map(x => (x(0), x(1).toInt))
    val wordCountReduce = wordCountMap.reduceByKey(_ + _)
    //wordCountReduce.print()

    // 窗口；注意窗口的大小一定要是StreamingContext中定义的batch的整数倍
    // window函数中有两个参数：
    // 1,窗口大小：计算量，取多少batch
    // 2,步进：job启动间隔，即：隔多少秒之后从receiver中取数据
    // window(5,2)：每隔2秒取出历史5秒的数据计算
    // window(5,5)：每隔5秒取出历史5秒的数据计算
    // window(10,5)：每隔10秒取出历史5秒的数据计算
    //    val wordCountWindowReduce = wordCountMap.window(Duration(5000), Duration(5000)).reduceByKey(_ + _)
    //    wordCountWindowReduce.print()

    val wordCountWindowReduce = wordCountMap.reduceByKeyAndWindow(((x: Int, y: Int) => x + y), Duration(5000), Duration(5000))
    //val wordCountWindowReduce = wordCountMap.reduceByKeyAndWindow(_ + _, Duration(5000), Duration(5000))
    wordCountWindowReduce.print()

    ssc.start()
    // main方法执行完毕后整个程序就会退出，所以需要阻塞主线程
    ssc.awaitTermination()
  }
}