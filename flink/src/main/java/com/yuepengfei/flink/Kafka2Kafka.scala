package com.yuepengfei.flink

import java.util.Properties
import java.util.concurrent.Executors

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object Kafka2Kafka {
  def main(args: Array[String]): Unit = {

    var conf: String = null
    val service = Executors.newFixedThreadPool(1)
    service.submit(new Runnable {
      override def run(): Unit = {
        while (true){
          conf = System.currentTimeMillis().toString
          Thread.sleep(2000)
        }
      }
    })


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:/data/flink/checkpoint"))

    val props = new Properties
    props.put("bootstrap.servers", "192.168.240.131:9092")
    props.put("zookeeper.connect", "192.168.240.131:2181")
    props.put("group.id", "test1")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //value 反序列化
    props.put("auto.offset.reset", "latest")

    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props)).setParallelism(1)

    val result = dataStreamSource
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .reduce((x,y)=>(y._1,x._2+y._2))
      .map(x => x._1 + " --> " + x._2 + " == " + conf)//todo 尽管driver端的conf不停在变，但是task中的conf还是初始值。参考广播流


    val producerConfig = new Properties()
    producerConfig.put("bootstrap.servers", "192.168.240.131:9092")
    producerConfig.put("zookeeper.connect", "192.168.240.131:2181")
    producerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    producerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //value 反序列化

    result.addSink(new FlinkKafkaProducer011[String]("testsink",new SimpleStringSchema(),producerConfig)).setParallelism(1)

    env.execute("From kafka to es")
  }
}
