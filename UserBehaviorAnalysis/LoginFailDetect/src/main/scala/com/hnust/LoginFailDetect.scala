package com.hnust

import java.net.URL
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util

import com.hnust.Utils.DBUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.utils
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

//登录失败实践样例类
case class LoginFailEvent(uid:Long, ip:String, eventType:String, timestamp:Long)
//警告信息样例类
case class LoginWarn(uid:Long,time:String, message:String)

/**
  * 用户恶意登录监控
  * 在2s内只要有连续两次登录失败就报警或限制用户登录
  */
object LoginFailDetect {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //设置时间语义和并行度
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //定义Kafka配置
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.177.102:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-login")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    //创建Kafka数据源
    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("login-event", new SimpleStringSchema(), prop)
    consumer.setStartFromLatest()
    val source: DataStream[String] = env.addSource(consumer)


    //获取资源路径
//    val url: URL = getClass.getResource("/login.log")
//    val source = env.readTextFile(url.getPath)

    val keyedStream: KeyedStream[LoginFailEvent, Long] = source.map(line => {
      val values: Array[String] = line.split("\\|")

      LoginFailEvent(values(0).toLong, values(1), values(2), values(3).toLong)
    })
      .assignAscendingTimestamps(_.timestamp) //设置时间戳
      .keyBy(_.uid)                           //按uid分组

    val pattern: Pattern[LoginFailEvent, LoginFailEvent] = Pattern.begin[LoginFailEvent]("begin")
      .where(_.eventType.equals("fail"))
      .next("next")
      .where(_.eventType.equals("fail"))
      .within(Time.minutes(2))

    val patternStream: PatternStream[LoginFailEvent] = CEP.pattern(keyedStream, pattern)


    patternStream.select(new SelectFunction()).print("waring:")

    env.execute()
  }

}

/**
  * LoginFailEvent为输入数据类型
  * LoginWarn为输出数据类型
  */
class SelectFunction() extends PatternSelectFunction[LoginFailEvent, LoginWarn]{
  override def select(pattern: util.Map[String, util.List[LoginFailEvent]]): LoginWarn = {
    val date: Date = new Date(pattern.get("next").iterator().next().timestamp)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: String = format.format(date)


    //操作数据库，设置用户在半小时后才能登陆
    val conn: Connection = DBUtil.getConnection()
    val sql:String = "update user set no_login_time=? where uid=?"
    val statement: PreparedStatement = conn.prepareStatement(sql)

    //设置半小时后的时间
    val endTime: Long = pattern.get("next").iterator().next().timestamp + 1000*60*30
    val endDate = format.format(new Date(endTime))

    statement.setString(1,endDate.toString)
    statement.setString(2,pattern.get("next").iterator().next().uid.toString)


    val result: Int = statement.executeUpdate()
    if(result > 0){
      println("已限制该用户在" + endDate.toString + "后才可登陆")
    }

    //释放资源
    DBUtil.close(conn)

    LoginWarn(pattern.get("begin").iterator().next().uid, time.toString,
      "该用户在短时间内连续登录失败，怀疑有恶意登录")

  }
}
