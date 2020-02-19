package com.yuepengfei.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object FlinkTableWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //开启事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //开启checkpoint(实际生产中，考虑性能问题，一般时长为分钟级别)
    env.enableCheckpointing(5*60*1000)
    //env.setStateBackend(new FsStateBackend("file://./data/flink/checkpoint"))

    val tEnv = StreamTableEnvironment.create(env)

    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "test")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//value 反序列化
    props.put("auto.offset.reset", "earliest")

    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props))


    val df: DataStream[(String, Int)] = dataStreamSource
      .flatMap(_.split(" "))
      .map(_.trim.replace("\t", "").replace(" ", "").replace(".", ""))
      .filter(word => word != "" && word != "\n")
      .map((_, 1))

      //抽取时间戳
      .assignAscendingTimestamps(x => System.currentTimeMillis())
      //抽取时间戳并且设定watermark
      /*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[WordCount] {

        private val maxOutOfOrderness = 3500
        private var currentMaxTimestamp = 0L

        override def extractTimestamp(element: WordCount, previousElementTimestamp: Long): Long = {
          val timestamp = System.currentTimeMillis()
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          return timestamp
        }

        override def getCurrentWatermark: Watermark = {
          return new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        }
      })*/

    tEnv.registerDataStream("word_count", df, 'word, 'num, 'rowtime.rowtime)

    //没有窗口直接查询
    /*val result: Table = tEnv.sqlQuery(
      """
        |select word, num as num from word_count
        |""".stripMargin)*/

    //开窗计算
    val result: Table = tEnv.sqlQuery(
      """
        |select word, sum(num) as num from word_count
        |group by TUMBLE(rowtime, INTERVAL '1' MINUTE), word
        |""".stripMargin)



   /* tEnv.scan("word_count").window(Over
      .partitionBy("a")
      .orderBy("rowtime")
      .preceding("UNBOUNDED_RANGE")
      .following("CURRENT_RANGE")
      .as("w"))*/

    tEnv.toRetractStream[WordCount](result).print()

    env.execute("FlinkTableWordCount")

  }
}

case class WordCount(word: String, num: Int)
