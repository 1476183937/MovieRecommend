package com.hnust.streaming

import java.io

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{MongoClientURI, casbah}
import com.mongodb.casbah.{Imports, MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MongoConfig(uri: String, db: String)

// 标准推荐
case class Recommendation(mid: String, rating: Double)

// 用户的推荐
case class UserRecs(uid: String, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: String, recs: Seq[Recommendation])

//连接助手对象
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("localhost")
  lazy val monClient = MongoClient(casbah.MongoClientURI("mongodb://localhost:27017/movieRecommender"))
}

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {


    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender",
      "kafka.topic" -> "recommender"
    )

    //
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(3))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._

    //广播电影相似度矩阵
    val simProductsMatrix: collection.Map[String, Map[String, Double]] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        item => {
          (item.mid, item.recs.map(x => (x.mid, x.rating)).toMap)
        }
      }.collectAsMap()


    //广播数据
    //    val simProductsMatrixBroadCast : Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)
    val simProductsMatrixBroadCast: Broadcast[collection.Map[String, Map[String, Double]]] = sc.broadcast(simProductsMatrix)


    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.177.102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    //创建Dstream流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParams)
    )

    //产生评分流：
    //输入数据：uid|mid|rating|date =》 转换后 (uid, mid, rating, date)
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      mesg => {
        val splits: Array[String] = mesg.value().split("\\|")
        println(splits)
        (splits(0).toInt, splits(1).toInt, splits(2).toDouble, splits(3).toInt)
      }
    }

    //核心实时推荐算法
    ratingStream.foreachRDD(
      rdds => {
        rdds.foreach {
          case (uid, mid, rating, date) => {

            println("rating data coming!>>>>>>>>>>>>>>>>>>")
            //获取当前用户最近M次评分：返回格式为Array[(uid,rating)]
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

            //获取与电影p最相似的 K个电影
            val simProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, uid, mid, simProductsMatrixBroadCast.value)

            //计算待选电影的优先级
            //            val streamRecs = computeProductScores(simProductsMatrixBroadCast.value, userRecentlyRatings, simProducts)
            val streamRecs: Array[(Int, Double)] = computeProductScores(simProductsMatrixBroadCast.value, userRecentlyRatings, simProducts)

            //保存到mongodb
            saveRecsToMongoDB(uid, streamRecs)

          }
        }
      }
    )

    //启动streming程序
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()

  }

  //从redis获取某用户最近的几次评分
  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {

    //redis里存储格式为：key->uid:用户id value->mid:rating mid:rating
    jedis.lrange("uid:" + userId.toString, 0, num).map {
      item => {
        val attr: Array[String] = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
    }.toArray

  }

  def getTopSimProducts(num: Int, uid: Int, mid: Int, //collection.Map[String, Map[String, Double]]
                        simProducts: scala.collection.Map[String, scala.collection.immutable.Map[String, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {

    //从广播变量simProducts中找出与mid相似的电影,转换成数组
    //    val allSimProducts: Array[(Int, Double)] = simProducts.get(productId.toString).get.toArray
    val allSimProducts: Array[(String, Double)] = simProducts.get(mid.toString).get.toArray

    //从mongodb的Rating表中获取该用户已评分过的电影的id
    val ratingExist: Array[Int] = ConnHelper.monClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item => {
          item.get("mid").toString.toInt
        }
      }

    //对allSimProducts过滤掉已评分过的电影,排序，取前num个，取出其中的mid
    allSimProducts.filter(item => {
      !ratingExist.contains(item._1)
    })
      .sortWith(_._2 > _._2)
      .take(num) //取前num个
      .map(_._1.toInt) //只取出其中的mid
  }

  /**
    *
    * @param simProducts         电影相似度矩阵
    * @param userRecentlyRatings 用户最近的几次评分
    * @param topSimProducts      与用户这次评分的电影最相似的几个电影
    * @return Array[(Int,Double)]
    */
  def computeProductScores(simProducts: scala.collection.Map[String, scala.collection.immutable.Map[String, Double]],
                           userRecentlyRatings: Array[(Int, Double)],
                           topSimProducts: Array[Int]): Array[(Int, Double)] = {

    //用于保存每一个待选电影与最近评分的每一个电影的权重
    val scores: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个电影的增强因子
    val increMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每一个电影的减弱因子
    val decreMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()

    //遍历userRecentlyRatings和topSimProducts，计算权重
    for (topSimProduct <- topSimProducts; userRecentlyRating <- userRecentlyRatings) {

      //计算待选电影和已评分电影的相似度
      val simScore: Double = getProductsSimScore(simProducts, topSimProduct, userRecentlyRating._1)

      //只取相似度大于0.6的
      if (simScore > 0.2) {
        scores += ((topSimProduct, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 2) { //如果该已评分电影评分大于2，则该待选电影的增强因子加一
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct, 0) + 1
        } else { //如果该已评分电影评分小于2，则该待选电影的增强因子减一
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct, 0) + 1
        }
      }

    }

    //对socres按待选电影的id分组，计算推荐优先级，排序
    //   val intToTuples: Map[Int, ArrayBuffer[(Int, Double)]] = scores.groupBy(_._1)
    scores.groupBy(_._1).map {
      case (mid, sims) => {
        (mid, sims.map(_._2).sum / sims.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
      }
    }
      .toArray
      .sortWith(_._2 > _._2)

  }


  /**
    *
    * @param simProducts         电影相似度矩阵
    * @param topSimProductId     电影id
    * @param userRatingProductId 用户已评分的电影id
    * @return 相似度
    */
  def getProductsSimScore(simProducts: scala.collection.Map[String, scala.collection.immutable.Map[String, Double]],
                          topSimProductId: Int, userRatingProductId: Int): Double = {

    // 从相似度矩阵中根据topSimProductId取出该电影相似的电影=》(电影id，与该电影的相似度) ，在从其中获取相似度

    simProducts.get(topSimProductId.toString) match {
      case Some(sim) => sim.get(userRatingProductId.toString) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }

  }

  //取10的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(10)
  }

  def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    //获取要操作的表
    val streaRecsCollection: MongoCollection = ConnHelper.monClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    //删除之前的实时记录
    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> userId))
    //插入新的记录
    streaRecsCollection.insert(MongoDBObject("uid" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))

  }

}
