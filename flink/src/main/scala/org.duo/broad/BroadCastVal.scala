package org.duo.broad

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object BroadCastVal {

  def main(args: Array[String]): Unit = {

    // 1. 创建env
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2. 加载两个集合
    // List((1, "张三"), (2, "李四"), (3, "王五"))  => 广播变量
    // List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
    // List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
    val stuDataSet: DataSet[(Int, String)] = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))
    // 3. 遍历成绩集合
   /* 所有Flink函数类都有其Rich版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。也有意味着提供了更多的，更丰富的功能。例如：RichMapFunction
    * 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次, 而且先被调用
    * 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次, 而且是最后被调用
    * getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态. 开发人员在需要的时候自行调用获取运行时上下文对象.
    */
    // IN  OUT
    val resultDataSet: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      var stuList: List[(Int, String)] = null

      override def map(value: (Int, String, Int)): (String, String, Int) = {
        // 在map方法中 我们要进行数据的对比
        // 1. 获取学生ID
        val stuID = value._1

        // 2. 过滤出id相等数据
        val tuples: List[(Int, String)] = stuList.filter((x: (Int, String)) => stuID == x._1)

        // 3. 构建新的元组
        (tuples(0)._2, value._2, value._3)

      }

      // open方法会在map方法之前执行
      override def open(parameters: Configuration): Unit = {

        import scala.collection.JavaConverters._
        // 获取广播变量
        stuList = getRuntimeContext.getBroadcastVariable[(Int, String)]("stuDataSet").asScala.toList
      }
    }).withBroadcastSet(stuDataSet, "stuDataSet")


    // 4. 打印
    resultDataSet.print()


  }

}
