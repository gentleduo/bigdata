package org.duo.stream.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object CountSlidingWindow {
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
    // 4. 执行统计，每个sensorId一个sliding窗口，窗口大小4条数据,窗口滑动为2条数据
    // 也就是说，每个路口分别统计，收到关于它的2条消息时统计在最近4条消息中，各自路口通过的汽车数量
    // 对应的key出现的次数达到二次才对相同的key的最近四条消息做一次聚合
    val keyedStream: KeyedStream[CountCar, Int] = mapDataStream.keyBy(_.sen)
    val countCarDataStream: DataStream[CountCar] =
      keyedStream.countWindow(4, 2).sum(1)
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

