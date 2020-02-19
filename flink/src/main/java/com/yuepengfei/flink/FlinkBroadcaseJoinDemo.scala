package com.yuepengfei.flink

import java.util
import java.util.Random

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * flink的广播流join的使用。工作中很常见的一种需求
 */

object FlinkBroadcaseJoinDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //创建广播流
    val CONFIG_DESCRIPTOR = new MapStateDescriptor[String, String]("streamingConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val broadcaseStream = env.addSource(new RichParallelSourceFunction[String] {
      var isRun: Boolean = _
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val words = Array("spark", "suning")
        //这个流产生1000个单词就结束了
        for (i <- 1 to 1000) {
          val random = new Random
          val message = words(random.nextInt(2))
          ctx.collect(message)
          Thread.sleep(5000)
        }
      }
      override def cancel(): Unit = {
        isRun = false
      }
    }).setParallelism(1).broadcast(CONFIG_DESCRIPTOR)



    //创建主流
    val dataStreamSource: DataStream[String] = env.addSource(new RichParallelSourceFunction[String] {
      var isRun: Boolean = _

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val words = Array("spark", "suning", "spring", "flink", "kafka", "hadoop")
        //这个流产生1000个单词就结束了
        for (i <- 1 to 1000) {
          val random = new Random
          val message = "主流中的单词是"+words(random.nextInt(6))
          ctx.collect(message)
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        isRun = false
      }
    })
      .setParallelism(1)

    //广播流合主流的整合
    val dbs: BroadcastConnectedStream[String, String] = dataStreamSource.connect(broadcaseStream)
    dbs.process(new BroadcastProcessFunction[String, String, String] {
      private var conf = new util.HashMap[String,Int]()

      override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val conf0 = conf.asScala
        println("配置文件长度：" + conf0.size)
        for ((k0,v0) <- conf0){
          if(value.contains(k0)){
            println(s"普通流值：${value}")
          }
        }
      }

      override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        println(s"广播值: ${value}")
        conf.put(value,1)
      }
    })

    env.execute("FlinkBroadcaseJoinDemo")

  }


}
