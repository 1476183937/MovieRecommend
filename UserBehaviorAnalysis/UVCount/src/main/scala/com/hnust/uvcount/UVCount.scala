package com.hnust.uvcount

import java.lang
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

//import java.util.Date

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

//用户行为样例类：用户id-电影id-类别-时间类型-时间戳
case class UserBehavior(uid:Long, mid: Long, category:String, eventType:String, timestamp:Long)

//输出结果样例类:开始时间-结束时间-数量
case class UVCount(startTime:String, endTime:String, count:Long)

/**
  * 统计每个小时内的用户访问量，并去重
  */
object UVCount {

  def main(args: Array[String]): Unit = {

    //创建环境，设置时间语义，并行度
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val url: URL = getClass.getResource("/UserBehavior.csv")
    val source: DataStream[String] = env.readTextFile(url.getPath)

    val result: DataStream[UVCount] = source.map(line => {
      //368778,3598206,381850,pv,1511690385
      val values: Array[String] = line.split(",")
      UserBehavior(values(0).trim.toLong, values(1).trim.toLong, values(2).trim,
        values(3).trim, values(4).trim.toLong)
    })
      .filter(_.eventType == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .map(x => ("dumpkey", x.uid, x.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //定义一个触发器,来一条数据就触发一次
      .process(new UVCountWithBloom())
       //

    result.print()

    env.execute("count :")

  }

}


class Bloom(size: Long) extends Serializable{

    //定义大小
    val crap = if (size > 0) size else 1 << 27

  //hash函数，根据传入的string求对应的偏移量
    def hash(value:String, seed:Int) :Long={

      var result = 0L
      for (i <- 0 until value.length){
        result = result * seed + value.charAt(i)
      }

      result & crap -1
    }


}

class MyTrigger() extends Trigger[(String, Long, Long), TimeWindow]{

  //在每个元素上的操作:这里设置为来一个就触发并情况窗口中的数据
  override def onElement(element: (String, Long, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  //在ProcessingTime情况下的处理方式
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //在EventTime情况下的处理方式
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //清除操作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}


class UVCountWithBloom() extends ProcessWindowFunction[(String, Long, Long), UVCount, String, TimeWindow]{

  //定义redis连接
  lazy private val jedis = new Jedis("localhost", 6379)
  lazy private val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long, Long)], out: Collector[UVCount]): Unit = {

    var count = 0L

    //redis中存储数据的结构：一个hash类型的uvcount，
    //键为一个小时的开始时间和结束时间(如1511690385_1511690385)，值为该小时内的访客数量
    //还有一个位图：名为一个小时的开始时间和结束时间(如1511690385_1511690385)

    //获取存储的key

    val currentTime: lang.Long = elements.last._3
    val format = new SimpleDateFormat("yyyy/MM/dd/HH")
    val date = new Date(currentTime)

    //开始时间
    val startTime: String = format.format(date)
    val endTime: String = format.format(new Date(date.getTime + 1 * 60 * 60 * 1000))

    //组装成key
    val storeKey = startTime + "_" + endTime

    //从redis取出count
    val jedisCount: String = jedis.hget("uvcount", storeKey)
    //如果redis里面该时间内计数不为空，直接复制给count
    if (jedisCount != null){
      count = jedisCount.toLong
    }

    //获取offect,传入uid和种子
    val offect: Long = bloom.hash(elements.last._2.toString, 61)

    //从位图里面判断该位置是否为真，真表示记录过，不计数，否则，计数并设为真
    val isExist: lang.Boolean = jedis.getbit(storeKey, offect)

    //如果不为真
    if (!isExist){
      //将该位置设为真
      jedis.setbit(storeKey, offect, true)

      //更新计数
      jedis.hset("uvcount", storeKey, (count+1).toString)

      out.collect(UVCount(startTime, endTime, count + 1))
    }else {
      out.collect(UVCount(startTime, endTime, count))
    }

  }
}
