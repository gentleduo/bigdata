package org.duo.stream.sink

import org.duo.stream.datasource.DataSource_MySql.MySql_Source
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer011}

object Sink_Kafka {

  def main(args: Array[String]): Unit = {

    //  1. 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  2. 设置并行度
    env.setParallelism(1)
    //  3. 添加自定义MySql数据源
    val mySqlDataStream: DataStream[(Long, String, String, String)] = env.addSource(new MySql_Source())
    //  4. 转换元组数据为字符串
    val mapDataStream: DataStream[String] = mySqlDataStream.map {
      line => line._1 + "," + line._2 + "" + line._3 + "" + line._4
    }
    //  5. 构建`FlinkKafkaProducer010`
    val kafkaCluster = "server01:9092,server02:9092,server03:9092"
    val flinkKafkaProducer011 = new FlinkKafkaProducer011[String](kafkaCluster,
      "test-topic-1", new SimpleStringSchema())
    //  6. 添加sink
    mapDataStream.addSink(flinkKafkaProducer011)
    //  7. 执行任务
    env.execute()
  }

}
