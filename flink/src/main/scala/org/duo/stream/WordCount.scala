package org.duo.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther:duo
 * @Date: 2023-03-16 - 03 - 16 - 13:51
 * @Description: org.duo.stream
 * @Version: 1.0
 */
object WordCount {

  /**
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * createLocalEnvironment 创建一个本地执行环境
     * createLocalEnvironmentWithWebUI 创建一个本地执行环境，同时开启Web UI的查看端口，默认是8081
     * getExecutionEnvironment 根据执行的环境创建上下文，比如：local cluster
     *
     */
    //    StreamExecutionEnvironment.createLocalEnvironment()
    //    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val initStream: DataStream[String] = environment.socketTextStream("server01", 8888)

    val wordStream = initStream.flatMap(_.split(" "))
    val pairStream = wordStream.map((_, 1))
    val keyByStream = pairStream.keyBy(0)
    val restStream = keyByStream.sum(1)
    restStream.print()

    /**
     * 2> (hello,1)
     * 4> (flink,1)
     * 1> (spark,1)
     * 2> (hello,2)
     * 4> (hadoop,1)
     * 2> (hello,3)
     *
     * 2> 代表是哪一个线程处理
     * 但是相同的数据一定是由一个线程来处理
     */
    environment.execute("first flink job")
  }
}
