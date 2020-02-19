package com.yuepengfei.flink

import java.util.Random

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object StreamingDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建主流
    val dataStreamSource: DataStream[String] = env.addSource(new RichParallelSourceFunction[String] {
      var isRun: Boolean = _

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val words = Array("spark", "suning", "spring", "flink", "kafka", "hadoop")
        //这个流产生1000个单词就结束了
        for (i <- 1 to 1000) {
          val random = new Random
          val message = words(random.nextInt(6))
          //println(message)
          ctx.collect(message)
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        isRun = false
      }
    }).setParallelism(1)

    dataStreamSource.map((_,1)).print("map1")
    dataStreamSource.map((_,2)).print("map2")

    env.execute("flink Streaming demo")


  }

}
