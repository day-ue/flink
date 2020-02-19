package com.yuepengfei.flink

import java.util.Random

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


/**
 * 需求：每5min计算今天的用户量。
 * 开1天的滚动窗口，5min触发一次计算
 */


object TriggerDemo {
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

    dataStreamSource.map(Word(_,1)).setParallelism(1)
      .keyBy(_.word)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5*60)))
      .trigger(new MyTrigger)
      //window之后process必须继承ProcessWindowFunction型的。直接继承ProcessFunction,其实内部就可以实现window和trigger功能。
      //.process(new MyProcess())
      .process(new MyProcessWin())
      .print("keyword")

    env.execute("triggerDemo")

  }
}

class MyProcess extends ProcessFunction[Word, Word]{
  override def processElement(value: Word, ctx: ProcessFunction[Word, Word]#Context, out: Collector[Word]): Unit = {

  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[Word, Word]#OnTimerContext, out: Collector[Word]): Unit = {
    super.onTimer(timestamp, ctx, out)
  }
}




case class Word(word: String, sum : Int)
class MyProcessWin extends ProcessWindowFunction[Word, Word, String, TimeWindow]{
  /** process function维持的状态  */
  lazy val stateDescriptor = new MapStateDescriptor[String, Int]("myState",classOf[String], classOf[Int])
  lazy val state: MapState[String, Int] = getRuntimeContext
    .getMapState(stateDescriptor)

  /**
   * @param key
   * @param context
   * @param elements 同一个key的数据集合
   * @param out
   */
  override def process(key: String, context: Context, elements: Iterable[Word], out: Collector[Word]): Unit = {
    elements.foreach(x =>{
      if(state.contains(key)){
        state.put(key, state.get(key) + x.sum)
      } else{
        state.put(key, x.sum)
      }
    })
    state.entries.asScala.foreach(x => {
      out.collect(Word(x.getKey, x.getValue))
    })
  }

  /**
   * 窗口结束时调用此方法
   * @param context
   */
  override def clear(context: Context): Unit = {
    context.windowState.getMapState[String, Int](stateDescriptor).clear()
  }
}

/**
 * 每来一条数据计算一次，一分钟清理一次状态
 */
class MyTrigger extends Trigger[Any, TimeWindow]{

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  var count :Int= 0
  /**
   * 数据到来，先走这里，这里并非多任务。可能是在jobmanager内完场的，因此触发一次计算，
   */
  override def onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    {
      count = count + 1
      if(count%10 == 0){
        println(s"-------------------${format.format(window.getStart)}------------------${format.format(window.getEnd)}----------------------")
        return TriggerResult.FIRE
      }else{
        //注册定时器（有默认的定时器，只是这里可以添加自己想要的定时器）（定时器的队列是set集合，因此不怕重复注册）
        ctx.registerProcessingTimeTimer(window.maxTimestamp)
        return TriggerResult.CONTINUE
      }
    }

  /**
   * 并行度为3则触发三次
   * 猜想：这个方法是窗口结束时的回调，后续任务是3，就触发三次回调。其实这段代码还在driver端
   */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println(s"--------------------------------滚动窗口触发计算-------------------------------------")
    TriggerResult.FIRE_AND_PURGE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = return TriggerResult.CONTINUE

  @throws[Exception]
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //这里清除的是定时器
    ctx.deleteProcessingTimeTimer(window.maxTimestamp)
  }

  override def canMerge = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = { // only register a timer if the time is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the time is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime) ctx.registerProcessingTimeTimer(windowMaxTimestamp)
  }

  override def toString = "YUEPENGFEI"
}