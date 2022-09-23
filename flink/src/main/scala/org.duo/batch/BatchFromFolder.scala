package org.duo.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

/**
 * 遍历目录的批次数据
 */
object BatchFromFolder {

  def main(args: Array[String]): Unit = {

    // env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 读取目录
    def params: Configuration = new Configuration()
    params.setBoolean("recursive.file.enumeration",true)
    val folderDataSet: DataSet[String] = env.readTextFile("D:\\intellij-workspace\\hadoop\\flink\\src\\main\\resources\\").withParameters(params)

    folderDataSet.print()

  }
}
