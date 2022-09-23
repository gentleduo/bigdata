package org.duo.trans

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._

/**
 * 1. 构建批处理运行环境
 * 2. 使用`env.generateSequence`创建0-100的并行数据
 * 3. 使用`fiter`过滤出来`大于8`的数字
 * 4. 使用map操作传入`RichMapFunction`，将当前子任务的ID和数字构建成一个元组
 * 在RichMapFunction中可以使用`getRuntimeContext.getIndexOfThisSubtask`获取子任务序号
 * 5. 打印测试
 */
object RebanlanceTrans {

  def main(args: Array[String]): Unit = {

    // 1. 构建批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //   2. 使用`env.generateSequence`创建0-100的并行数据
    val seqDataSet: DataSet[Long] = env.generateSequence(0, 100)

    //   3. 使用`fiter`过滤出来`大于8`的数字
    // 如果不进行rebanlance，那么数据在各分区的分布是不均匀的
    // val filterDataSet: DataSet[Long] = seqDataSet.filter(_ > 8)
    // rebanlance会使用轮询的方式将数据均匀打散，按照数据的哈希模以并行度（Parallelism），并行度未设置的情况下默认为cpu核数
    val filterDataSet: DataSet[Long] = seqDataSet.filter(_ > 8).rebalance()
    //   4. 使用map操作传入`RichMapFunction`，将当前子任务的ID和数字构建成一个元组
    // 在RichMapFunction中可以使用`getRuntimeContext.getIndexOfThisSubtask`获取子任务序号
    val mapDataSet: DataSet[(Int, Long)] = filterDataSet.map(new RichMapFunction[Long, (Int, Long)] {
      // value: 是每次遍历的数字
      override def map(value: Long): (Int, Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })

    // 5. 打印测试
    mapDataSet.print()
  }

}
