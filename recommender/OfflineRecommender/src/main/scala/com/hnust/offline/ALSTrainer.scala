package com.hnust.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//商品评分样例类，为了和Mlib库里的Rating区别开，取名为ProductRating
case class ProductRating(uid: String, mid: String, rating: String, date: String)

/**
  * 模型评估和参数选取
  * 通常的做法是计算均方根误差（RMSE），考察预测评分与实际评分之间的误差。
  */
object ALSTrainer {

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender"
    )

    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .filter(rating => {
        (rating.uid.toDouble < 2147483647) && (rating.mid.toDouble < 2147483647)
      })
      .map(
        rating => Rating(rating.uid.toInt, rating.mid.toInt, rating.rating.toDouble)
      ).cache()

    //将ratingRDD随机切分成测试数据集和训练数据集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))

    val trainData: RDD[Rating] = splits(0)
    val testData: RDD[Rating] = splits(1)

    //输出最优参数
    adjustALSParam(trainData, testData)

    //关闭资源
    spark.stop()

  }

  //获取最优参数
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={

    val result: Array[(Int, Double, Double)] = for (rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainData, rank, 5, lambda)
        val rmse: Double = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    //输出
    println(result.sortBy(_._3).head)

  }

  //获取RMSE值
  def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]):Double ={

    //获取预测值,利用根据训练集得到的model，来对测试集进行预测得到预测评分
    val userProducts: RDD[(Int, Int)] = testData.map(item => (item.user, item.product))
    val predictRating: RDD[Rating] = model.predict(userProducts)

    //获取真实评分：就是测试集里面已有的评分,进行转换，将(userId，productId)作为主键，评分作为值
    val real: RDD[((Int, Int), Double)] = testData.map(item => ((item.user, item.product), item.rating))

    //对预测评分进行转换，以(userId,productId)作为主键，评分作为值
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))

    //根据预测评分和真实评分就均方根误差
    math.sqrt(
      real.join(predict).map{
        case ((userId, productId), (real, predict)) =>{
          val err: Double = real - predict
          err * err
        }
      }.mean()
    )




  }

}
