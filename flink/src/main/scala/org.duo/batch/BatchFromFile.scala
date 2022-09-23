package org.duo.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * 读取文件中的批次数据
 */
object BatchFromFile {

  def main(args: Array[String]): Unit = {

    // 创建env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 加载文件
    val textDataSet: DataSet[String] = env.readTextFile("D:\\intellij-workspace\\hadoop\\flink\\src\\main\\resources\\data.txt")

    //加载hdfs文件
    //val datas: DataSet[String] = env.readTextFile("hdfs://server01:8020/README.txt")

    // 打印
    textDataSet.print()

  }
}
