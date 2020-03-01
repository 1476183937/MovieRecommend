package com.hnust.hotmovies

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.{lang, util}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.util.Properties


import scala.collection.mutable.ListBuffer


//2505931162|1306119|80|1582197283877
//电影评分样例类
case class MovieRating(uid: String, mid: Long, rating: Int, timestamp: Long)

//输出数据样例类
case class MovieViewCount(mid: Long, windowEnd: Long, count: Long)

/** 实时热门电影统计 2623138 2843983
  * 每隔5分钟输出最近一小时内评分量最多的前N个电影。将这个需求进行分解我们大概要做这么几件事情：
  * •抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
  * •按一小时的窗口大小，每1分钟统计一次，做滑动窗口聚合（Sliding Window）
  * •按每个窗口聚合，输出每个窗口中点击量前N名的电影
  */
object HotMovies {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //定义kafka配置

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.177.102:9092")
    properties.setProperty("group.id", "movie-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer[String]("ratings",new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()

    //定义kafka数据源
    val source: DataStream[String] = env.addSource(consumer)

    val result: DataStream[String] = source.map(line => {
      println("line: "+line)
      val values: Array[String] = line.split("\\|")

      MovieRating(values(0), values(1).trim.toLong, values(2).trim.toInt, values(3).trim.toLong)
    })
      //指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)
      //根据电影id做keyBy
      .keyBy(_.mid)
      //设置时间窗口大小为1小时，每1分钟滑动一次
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AggCount(), new ResultCount())
      .keyBy(_.windowEnd)
      .process(new TopNMoives())

    result.print()

    env.execute("start")

  }
}

class AggCount() extends AggregateFunction[MovieRating, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: MovieRating, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 2860000 2920000 2980000
class ResultCount() extends WindowFunction[Long, MovieViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[MovieViewCount]): Unit = {
//    println("endTime:" + window.getEnd)
    out.collect(MovieViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNMoives() extends KeyedProcessFunction[Long, MovieViewCount, String] {

  //定义一个状态，保存类型为MovieViewCount
  var movieState: ListState[MovieViewCount] = _

  //初始化状态
  override def open(parameters: Configuration): Unit = {
    movieState = getRuntimeContext.getListState(new ListStateDescriptor[MovieViewCount]("movieState", classOf[MovieViewCount]))
  }

  override def processElement(movie: MovieViewCount,
                              context: KeyedProcessFunction[Long, MovieViewCount, String]#Context,
                              collector: Collector[String]): Unit = {

    //每来一个元素，就存进状态里
    movieState.add(movie)

    //设置定时器
    context.timerService().registerEventTimeTimer(movie.windowEnd)

  }

  //定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, MovieViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //定义一个List
    val listBuffer: ListBuffer[MovieViewCount] = ListBuffer()

    //取出状态里的数据，存到List里
    val movies: lang.Iterable[MovieViewCount] = movieState.get()

    val iterator: util.Iterator[MovieViewCount] = movies.iterator()

    while (iterator.hasNext) {
      listBuffer += iterator.next()
    }
    //清除状态
    movieState.clear()

    //倒序排序，取前10个
    val sortedMovies: ListBuffer[MovieViewCount] = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(10)

    val result = new StringBuilder

    result.append("======================================\n")
    result.append("时间：").append(timestamp).append("\n")
    for (i <- sortedMovies.indices) {
      val currentMovie: MovieViewCount = sortedMovies(i)

      result.append("NO").append(i + 1).append(":")
      result.append("mid: ").append(currentMovie.mid)
      result.append("\tcount: ").append(currentMovie.count).append("\n")
    }

    println("======================================")
    TimeUnit.SECONDS.sleep(1)
    out.collect(result.toString())

  }
}

