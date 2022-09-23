package org.duo.env

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

import java.util.Date

/**
 * local环境
 */
object BatchCollectionsEven {

  def main(args: Array[String]): Unit = {
    // 开始时间
    var start_time = new Date().getTime
    //TODO 初始化本地执行环境
    val env = ExecutionEnvironment.createCollectionsEnvironment
    val list: DataSet[String] = env.fromCollection(List("1", "2"))

    list.print()

    // 结束时间
    var end_time = new Date().getTime
    println(end_time - start_time) //单位毫秒
  }
}
