package org.duo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther:duo
 * @Date: 2023-03-14 - 03 - 14 - 15:40
 * @Description: org.duo.spark.streaming
 * @Version: 1.0
 */
object StreamingStateCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[5]")
    // StreamingContext在创建的时候也要用sparkConf，说明StreamingContext是基于Spark Core的
    // 在执行master的时候不能指定一个线程，因为streaming运行的时候，需要开一个新的线程来去一直监听数据的获取
    val ssc = new StreamingContext(sparkConf, Seconds(1)) // 默认win：1s，slide：1s
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.sparkContext.setCheckpointDir(".")

    // 在linux服务器上使用nc命令，实现tcp方式监听服务器端8888：nc -l 8888
    val lines = ssc.socketTextStream(
      //      hostname = args(0),
      //      port = args(1).toInt,
      "192.168.56.110",
      8888,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER)

    // 有状态计算
    // 历史的计算结果都要保存下来，当前的计算最后还要合到历史数据里
    // 持久化下来历史的数据状态
    val mapdata: DStream[(String, Int)] = lines.map(_.split(" ")).map(x => (x(0), x(1).toInt))
    val res = mapdata.updateStateByKey(
      (nv: Seq[Int], ov: Option[Int]) => {
        val count: Int = nv.count(_ > 0)
        val oldVal: Int = ov.getOrElse(0)
        Some(count + oldVal)
      })

    res.print()

    ssc.start()
    // main方法执行完毕后整个程序就会退出，所以需要阻塞主线程
    ssc.awaitTermination()
  }
}
