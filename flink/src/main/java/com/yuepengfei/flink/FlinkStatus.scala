package com.yuepengfei.flink

import java.util.Random

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkStatus extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //有状态维护要开启checkpoint
  env.enableCheckpointing(1000)
  env.setStateBackend(new FsStateBackend("file:/data/flink/checkpoint"))

  val dataStreamSource: DataStream[String] = env.addSource(new RichParallelSourceFunction[String] {
    var isRun: Boolean = _

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val words = Array("spark", "suning", "spring", "flink", "kafka", "hadoop")
      //这个流产生1000个单词就结束了
      for (i <- 1 to 1000) {
        val random = new Random
        val message = words(random.nextInt(6))
        ctx.collect(message)
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      isRun = false
    }
  }).setParallelism(1)

  dataStreamSource
    .map((_, 1))
    .keyBy(_._1)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5 * 60)))
    .trigger(new FlinkStatusMyTrigger)
    .process(new FlinkStatusMyWindowsProcess)
    .setParallelism(1)
    .print("result")
    .setParallelism(1)

  env.execute("FlinkStatus")

}

class FlinkStatusMyTrigger extends Trigger[(String, Int),TimeWindow]{
  override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE //清理的话，ProcessWindowFunction类里的elements是最新的一条数据
//    TriggerResult.FIRE  //不清理，ProcessWindowFunction类里的elements是窗口内的所有数据
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }
}

class FlinkStatusMyWindowsProcess extends ProcessWindowFunction[(String, Int),(String, Int), String, TimeWindow] {

  lazy val stateDescriptor = new ValueStateDescriptor[Int]("countState",classOf[Int])
  lazy val state = getRuntimeContext.getState(stateDescriptor)

  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    val newState = state.value() + 1
    elements.foreach(println)//窗口清理则，打印最新的一条数据;不清理，打印窗口内的所有数据
    state.update(newState)
    out.collect((key, newState))
  }
}

