package com.yuepengfei.flink

import java.util
import java.util.Random
import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import com.yuepengfei.es.EsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}


/**
 * 实现公司的预警场景
 */
//todo 草，写点代码是真难

object AsynDemo extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  /*//有状态维护要开启checkpoint
  env.enableCheckpointing(1000)
  env.setStateBackend(new FsStateBackend("file:/data/flink/checkpoint"))*/

  //创建主流
  val dataStreamSource: DataStream[String] = env.addSource(new RichParallelSourceFunction[String] {
    var isRun: Boolean = _

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val words = Array("spark", "suning", "spring", "flink", "kafka", "hadoop")
      //这个流产生1000个单词就结束了
      for (i <- 1 to 1000) {
        val random = new Random
        val message = words(random.nextInt(6))
        ctx.collect(message + "#&#" + System.currentTimeMillis())
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      isRun = false
    }
  }).setParallelism(1)


  val asyncStream: DataStream[(String, String)] =
    AsyncDataStream.unorderedWait(dataStreamSource, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 1 * 1000)


  val ds: DataStream[WordTime] = asyncStream.map(x => {
    val data = x._1.split("#&#")
    (data(0), data(1), x._2)
  }).keyBy(_._1)//todo 神坑，keyBy(),0和字段，完全不一样。keyBy(0) --> process[]返回值的泛型，并且key是tuple类型。这个问题浪费了我两个小时
    //设定窗口每次都会把key对应的数据处理一遍，传递下去
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5 * 60)))
    .trigger(new AsyncMyTrigger)
    //.evictor(CountEvictor.of(1))//控制怎么清楚窗口内的数据,保留的是不是最后一条还需考察
    .process(new AsyncMyWindowProcess)


  val dsToES: DataStream[WordTime] = ds.filter(_.flag == 0)
  val dsAndES: DataStream[WordTime] = ds.filter(_.flag == 1)
  val dsToKafka: DataStream[WordTime] = ds.filter(_.flag == 2)

  HandleStream.ProcessDsToES(dsToES)
  HandleStream.ProcessDsAndES(dsAndES)
  HandleStream.ProcessDsToKafka(dsToKafka)

  env.execute("not close")
}

object HandleStream{
  //todo 草啊，链接es浪费了我一个晚上的时间。现在真讨厌这些两天一变的api。还有flink官网，代码都是错的
  def ProcessDsToES(dsToES: DataStream[WordTime]): Unit ={

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.240.131", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {
          return Requests.indexRequest()
            .index("word_time_flag1")
            .`type`("doc")
            .source(element, XContentType.JSON)
        }
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)
    //todo 必须吹一波了。Gson可以序列化case class,fastjson不可以。所以急需用Gson进行解析，但是又不能每来一条数据就now一个对象，所以这个对象应该在driver端new, 但是gson又不可序列化。lazy完美解决了这个问题
    lazy val gson = new Gson()
    dsToES.map(gson.toJson(_))
      .addSink(esSinkBuilder.build())
  }
  def ProcessDsAndES(dsAndES: DataStream[WordTime]): Unit ={
    //异步关联一个es的维表，拿到数据后直接删除es中对应的数据
    val esAndStream: DataStream[(WordTime, String)] = AsyncDataStream.unorderedWait(dsAndES, new AsyncFunction[WordTime, (WordTime, String)] {
      /** The database specific client that can issue concurrent requests with callbacks */

      lazy val client: RestHighLevelClient = EsUtils.getClient()

      /** The context used for the future callbacks */
      implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

      override def asyncInvoke(input: WordTime, resultFuture: ResultFuture[(WordTime, String)]): Unit = {

        // issue the asynchronous request, receive a future for the result
        val resultFutureRequested: Future[String] = Future {
          val jsonStrs = new util.ArrayList[String]()
          val hm = EsUtils.queryTerm(client, "word_time_flag1", "doc", "word", input.word)
          for ((id, source) <- hm.asScala) {
            jsonStrs.add(source)
          }
          EsUtils.deleteByQuery(client, "word_time_flag1", "doc", "word", input.word)
          StringUtils.join(jsonStrs,"#&#")
        }
        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        resultFutureRequested.onSuccess {
          case result: String => resultFuture.complete(Iterable((input, result)))
        }
      }
    }, 60*1000, TimeUnit.MILLISECONDS, 1)
    esAndStream.process(new ProcessFunction[(WordTime, String), WordTime]{
        lazy private val gson = new Gson()
        override def processElement(value: (WordTime, String), ctx: ProcessFunction[(WordTime, String), WordTime]#Context, out: Collector[WordTime]): Unit = {
          val wordTimes: Array[String] = value._2.split("#&#")
          wordTimes.map(gson.fromJson(_,classOf[WordTime])).foreach(out.collect(_))
          out.collect(value._1)
        }
      }).print("dsAndES")
  }

  def ProcessDsToKafka(dsToKafka: DataStream[WordTime]): Unit ={
    dsToKafka.print("dsToKafka")
  }
}

class AsyncMyTrigger extends Trigger[(String, String, String), TimeWindow]{
  override def onElement(element: (String, String, String), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //触发计算并且清除窗口内的数据（仅仅清除数据，不清除状态和元数据）
    TriggerResult.FIRE_AND_PURGE
  }
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  @throws[Exception]
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp)
  }
}



class AsyncMyWindowProcess extends ProcessWindowFunction[(String, String, String),WordTime,String, TimeWindow]{
  lazy val stateDescriptor = new ValueStateDescriptor[Int]("countState",classOf[Int])
  lazy val state = getRuntimeContext.getState(stateDescriptor)

  /**
   * 该方法是每来一个元素调用一次
   * @param key
   * @param context
   * @param elements 每个key在该窗口内的所有数据
   * @param out
   */
  override def process(key: String, context: Context, elements: Iterable[(String, String, String)], out: Collector[WordTime]): Unit = {
    var newState = state.value() + 1
    elements.foreach(x =>{
      if(newState < x._3.toInt){
        //打上标识0
        out.collect(WordTime(x._1,x._2,0))
      } else if(newState == x._3.toInt) {
        //打上表示1
        out.collect(WordTime(x._1,x._2,1))
      } else {
        //打上标识2
        out.collect(WordTime(x._1,x._2,2))
      }
    })
    state.update(newState)
  }

  override def clear(context: Context): Unit = {
    context.windowState.getState(stateDescriptor).clear()
  }
}



//异步读取配置文件
/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

  /** The database specific client that can issue concurrent requests with callbacks */
  lazy val client = new Jedis("192.168.240.131", 6379)

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  override def asyncInvoke(str: String, resultFuture: ResultFuture[(String, String)]): Unit = {

    // issue the asynchronous request, receive a future for the result
    val resultFutureRequested: Future[String] = Future(client.get("threshold"))

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultFutureRequested.onSuccess {
      case result: String => resultFuture.complete(Iterable((str, result)))
    }
  }


}

case class WordTime(word: String, time: String, flag: Int)
