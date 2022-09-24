package org.duo.stream.window

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeSlidingWindow {
  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 定义数据源  socket nc -lk 9999 [ 1,2 2,2 ]
    val socketDataStream: DataStream[String] = env.socketTextStream("server01", 9999)
    // 3. 转换数据  1,2 2,2
    val mapDataStream: DataStream[CountCar] = socketDataStream.map {
      line =>
        val strs: Array[String] = line.split(",")
        CountCar(strs(0).toInt, strs(1).toInt)
    }
    // 4. 执行统计
    // 以红绿灯进行分组
    val keyedStream: KeyedStream[CountCar, Int] = mapDataStream.keyBy(_.sen)
    // 执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为4秒，而窗口计算时间有如下两种情况
    // 1) 窗口计算时间 < 窗口时间，也就是说，每2秒钟统计一次，在这过去的4秒钟内，各个路口通过红绿灯汽车的数量。所有会有重复数据
    val countCarDataStream: DataStream[CountCar] = {
      keyedStream.timeWindow(Time.seconds(4), Time.seconds(2)).sum(1)
      // 2) 窗口计算时间 > 窗口时间，也就是说，每8秒钟统计一次，在这过去的4秒钟内，各个路口通过红绿灯汽车的数量。会出现数据丢失
      //val countCarDataStream: DataStream[CountCar] = {
      //  keyedStream.timeWindow(Time.seconds(4), Time.seconds(8)).sum(1)
    }
    // 5. 打印结果
    countCarDataStream.print()
    // 6. 执行任务
    env.execute()

  }

  /**
   *
   * @param sen    哪个红绿灯
   * @param carNum 多少辆车
   */
  case class CountCar(sen: Int, carNum: Int)
}

