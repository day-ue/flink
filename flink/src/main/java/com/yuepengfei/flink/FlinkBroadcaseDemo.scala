package com.yuepengfei.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._


/**
 * 简单的广播使用批处理的join, 没啥好说的。工作中用的并不多，因为工作中这种批处理多用sparksql去完成。
 */


object FlinkBroadcaseDemo {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBroadcast: DataSet[Int] = env.fromElements(1, 2, 3)
    val data: DataSet[String] = env.fromElements("1", "2", "5")

    val result = data.map(new RichMapFunction[String, String]() {
      var broadcastSet: Traversable[Integer] = null

      override def open(config: Configuration): Unit = {
        // 获取广播变量
        broadcastSet = getRuntimeContext().getBroadcastVariable[Integer]("broadcastSetName").asScala
      }

      def map(in: String): String = {
        //实现业务逻辑
        if (broadcastSet.toList.contains(in.toInt)) s"广播变量包含: ${in}"
        else s"广播变量不包含: ${in}"
      }
    }).withBroadcastSet(toBroadcast, "broadcastSetName") // 2. Broadcast the DataSet


    result.print("~~")

    env.execute("fuck")
  }
}
