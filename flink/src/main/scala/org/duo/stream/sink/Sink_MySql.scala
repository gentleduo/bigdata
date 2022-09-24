package org.duo.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Sink_MySql {

  def main(args: Array[String]): Unit = {

    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. load list
    val listDataSet: DataStream[(Long, String, String, String)] = env.fromCollection(List(
      (10L, "dazhuang", "123456", "大壮"),
      (11L, "erya", "123456", "二丫"),
      (12L, "sanpang", "123456", "三胖")
    ))

    // 3. add sink
    listDataSet.addSink(new MySqlSink)

    // 4. execute
    env.execute()
  }
}

class MySqlSink extends RichSinkFunction[(Long, String, String, String)] {

  var connection: Connection = null;
  var ps: PreparedStatement = null;

  override def open(parameters: Configuration): Unit = {

    // 1. 加载MySql驱动
    Class.forName("com.mysql.jdbc.Driver")
    // 2. 创建连接
    connection = DriverManager.getConnection("jdbc:mysql://server01:3306/kgwebdb?useSSL=false", "root", "123456")
    // 3. 创建PreparedStatement
    val sql = "insert into user(id,username,password,nickname) values(?,?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def invoke(value: (Long, String, String, String)): Unit = {

    // 执行插入
    ps.setLong(1, value._1)
    ps.setString(2, value._2)
    ps.setString(3, value._3)
    ps.setString(4, value._4)

    ps.executeUpdate()
  }

  override def close(): Unit = {
    // 关闭连接
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}