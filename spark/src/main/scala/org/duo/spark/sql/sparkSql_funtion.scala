package org.duo.spark.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}


class myAggFunc extends UserDefinedAggregateFunction {

  // inputSchema来指定调用avgUDAF函数时传入的参数类型
  override def inputSchema: StructType = {

    // false:表示不能为空
    StructType.apply(Array(StructField.apply("score", IntegerType, false)))
  }

  // bufferSchema设置UDAF在聚合过程中的缓冲区保存数据的类型，一个参数是成绩总和，一个参数是累加科目数
  override def bufferSchema: StructType = {

    // sum / count = avg
    StructType.apply(Array(
      StructField.apply("sum", IntegerType, false),
      StructField.apply("count", IntegerType, false)
    ))
  }

  // dataType设置UDAF运算结束时返回的数据类型
  override def dataType: DataType = DoubleType

  // deterministic为代表结果是否为确定性的，也就是说，相同的输入是否有相同的输出。(幂等性)
  override def deterministic: Boolean = true

  // nitialize初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  //update用于控制具体的聚合逻辑，通过update方法，将每行参与运算的列累加到聚合缓冲区的Row实例中
  //每访问一行，都会调用一次update方法。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    buffer(0) = buffer.getInt(0) + input.getInt(0) //sum
    buffer(1) = buffer.getInt(1) + 1 // count
  }

  // merge用于合并每个分区聚合缓冲区的值
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // evaluate方法用于对聚合缓冲区的数据进行最后一次运算
  override def evaluate(buffer: Row): Double = {

    buffer.getInt(0) / buffer.getInt(1)
  }
}

/**
 * @Auther:duo
 * @Date: 2023-03-12 - 03 - 12 - 21:38
 * @Description: org.duo.spark.sql
 * @Version: 1.0
 */
object sparkSql_funtion {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("function")
      .master("local")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    import session.implicits._

    val dataFrame: DataFrame = List(("A", 1, 90), ("A", 2, 70), ("B", 1, 68), ("B", 2, 89), ("C", 1, 61), ("C", 2, 90)).toDF("name", "class", "score")

    dataFrame.createTempView("users")

    // 普通udf函数
    session.udf.register("add", (x: Int) => {
      x + 1
    })
    session.sql("select add(score) from users").show()

    // 聚合udf函数
    session.udf.register("customAgg", new myAggFunc)

    session.sql("select name,customAgg(score) from users group by name").show()
  }
}
