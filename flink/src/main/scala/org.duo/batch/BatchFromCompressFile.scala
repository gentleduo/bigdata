package org.duo.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 读取压缩文件的数据
 */
object BatchFromCompressFile {

  def main(args: Array[String]): Unit = {
    //初始化环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //加载数据
    val result = env.readTextFile("D:\\intellij-workspace\\hadoop\\flink\\src\\main\\resources\\wordcount.txt.gz")
    //触发程序执行
    result.print()
  }
}
