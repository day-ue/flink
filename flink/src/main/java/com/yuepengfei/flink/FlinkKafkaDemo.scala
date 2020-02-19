package com.yuepengfei.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.matching.Regex


object FlinkKafkaDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties
    props.put("bootstrap.servers", "192.168.240.131:9092")
    props.put("zookeeper.connect", "192.168.240.131:2181")
    props.put("group.id", "fuck")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //value 反序列化
    props.put("auto.offset.reset", "latest")

    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props))
    val regexWord: Regex = """[a-z]+""".r()
    val sumSource = dataStreamSource
      .map(_.toLowerCase)
      .flatMap(regexWord.findAllIn(_))
      .filter(word => word != "" && word != "\n")
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1).setParallelism(1)

      sumSource
      .map(param => WordWithCount(param._1, param._2))
      .keyBy(_.word)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new WordCountWindow())
      /*.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new WordCountAllWindow())*/
      .setParallelism(1)
      .print()
      .setParallelism(1)

    env.execute("Flink add data source")
  }
}
class WordCountAllWindow extends ProcessAllWindowFunction[WordWithCount, WordWithCount, TimeWindow]{
  override def process(context: Context, elements: Iterable[WordWithCount], out: Collector[WordWithCount]): Unit = {
    println(s"++++++++++++++++++++++${context.window}++++++++++++++++++++++++++++++++++")
    val set = new mutable.HashSet[WordWithCount] {}
    for (wordCount <- elements) {
      set.add(wordCount)
    }
    val sortSet = set.toList.sortWith((a, b) => a.count.compareTo(b.count) > 0)
    for (wordCount <- sortSet) {
      out.collect(wordCount)
    }
  }
}



class WordCountWindow extends ProcessWindowFunction[WordWithCount, WordWithCount, String, TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[WordWithCount], out: Collector[WordWithCount]): Unit = {
    val set = new mutable.HashSet[WordWithCount] {}
    for (wordCount <- elements) {
      set.add(wordCount)
    }
    val sortSet = set.toList.sortWith((a, b) => a.count.compareTo(b.count) > 0)
    for (wordCount <- sortSet) {
      out.collect(wordCount)
    }
  }
}



case class WordWithCount(word: String, count: Long)



