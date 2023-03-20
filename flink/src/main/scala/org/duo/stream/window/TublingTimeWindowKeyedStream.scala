package org.duo.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Auther:duo
 * @Date: 2023-03-20 - 03 - 20 - 9:46
 * @Description: org.duo.stream.window
 * @Version: 1.0
 */
object TublingTimeWindowKeyedStream {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = environment.socketTextStream("server01", 8888)

    // countWindow(10)：如果是基于keyed stream之上做count window，是当相同元素的个数大于10之后触发计算
    // 即：某个key的个数大于10之后触发计算，统计完成后，窗口向前移动10
    // countWindow(10, 2)：窗口中每新增一个个数大于2的元素即触发窗口计算，并且计算的时候会基于之前的结果
    // 即：某个key的个数大于2之后触发计算，当该key的个数大于10会后清零
    // 比如；第一次输入 hello hello spark，会触发第一次计算，输出结果为 hello 2
    // 第二次输入 hello hello hive，会触发第二次计算，输出结果为 hello 4
    // 第三次输入 hello hello hello hello hello hello flink，会触发第三次计算，输出结果为 hello 10
    // 第四次输入 hello hello hbase，由于前面三次计算已到达window的大小10所以窗口会向前移动2并触发第四次计算，得到的输出结果仍然为 hello 10
    // 综上所述，countWindow函数中的第一个参数表示从个数的维度划分的窗口的大小：即对个数在size范围内的word每增加slide进行一次wordcount
    // size范围外的word不会参与计算，就像时间窗口一样，只有在该时间窗口内的才会参与计算
    // 所有：size为最终输出结果的最大值，无论是滚动窗口还是滑动窗口，最终的输出结果都不可能超过这个数。
    stream.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).countWindow(10, 2)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      }).print()

    environment.execute()
  }

}
