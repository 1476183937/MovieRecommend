package com.hnust.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

//电影评分样例类，为了和Mlib库里的Rating区别开，取名为MovieRating
case class MovieRating(uid: String, mid: String, rating: String, date: String)

case class MovieInfo(mid:String,name:String, imgUrl:String, director:String, screenwriter:String,
                     mainActors:String, categories:String, location:String, language:String, releaseDate:String, runTime:String,
                     alias:String, summery:String, rating_num:String)
case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(mid: String, rating: String)

//用户推荐列表
case class UserRecs(uid: String, recs: Seq[Recommendation])

// 电影相似度（电影推荐）
case class ProductRecs(mid: String, recs: Seq[Recommendation])


//基于隐语义模型的协同过滤推荐
object OfflineRecommender {

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "movieInfos"

  // 推荐表的名称
  val USER_RECS = "UserRecs"  //用户推荐列表
  val MOVIE_RECS = "MovieRecs" //电影相似度推荐

  //取最大的前20个最为推荐结果
  val USER_MAX_RECOMMENDATION = 20


  def main(args: Array[String]): Unit = {

    //配置定义要用到的参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender"
    )

    //创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //读取数据，并将数据转换成一个三元元组,并放入缓存，提高速率
    val ratingRDD: Dataset[(String, String, String)] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .filter(rating => {
        (rating.uid.toDouble < 2147483647) && (rating.mid.toDouble < 2147483647)
      })
      .map(rating => {
        (rating.uid, rating.mid, rating.rating)
      })
      .cache()

    val movieRDD: RDD[Int] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieInfo]
      .filter(x => {
        x.mid.toDouble < 2147483647
      })
      .map(_.mid.toInt)
      .rdd
      .cache()



    //获取uid和mid的数据集
    val userRDD: RDD[Int] = ratingRDD.map(_._1.toInt).distinct().rdd
//    val productRDD: RDD[Int] = ratingRDD.map(_._2.toInt).distinct().rdd

    //创建训练的数据集:trainData
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1.toInt, x._2.toInt, x._3.toDouble)).rdd

    //调用ALS算法训练隐语义模型:model
    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参数
    //val (rank, iterations, lambda) = (50, 5, 0.01) //
    val (rank, iterations, lambda) = (5,1,24.274) //
    val model: MatrixFactorizationModel = ALS.train(trainData,rank,iterations, lambda)

    //计算用户的推荐矩阵：将uid和mid数据集进行笛卡尔积
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    //获取预测评分
    val preRatings: RDD[Rating] = model.predict(userMovies)

    //对预测评分进行过滤、分组
    val userRecs: DataFrame = preRatings.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => {
          UserRecs(uid.toString, recs.toList.sortWith(_._2 > _._2)
            .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1.toString, x._2.toString)))
        }
      }.toDF()
    //保存到mongodb
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //TODO：计算电影相似度矩阵
//    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map(x => (x._1, new DoubleMatrix(x._2)))

    val productFeatures = model.productFeatures.map{case (mid,features) =>
      (mid, new DoubleMatrix(features))
    }
    // 计算笛卡尔积并过滤合并，将自己和自己的合并过滤
    val productRecs: DataFrame = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) =>
          val simScore: Double = consinSim(a._2, b._2) //求余弦相似度
          (a._1, (b._1, simScore)) //(a的mid，(b的mid，a和b的相似度))
      }
      .filter(_._2._2 > 0.4) //过滤出相似度大于0.6的
      .groupByKey() //分组
      .map {
        case (mid, items) => ProductRecs(mid.toString, items.toList.sortWith(_._2 >_._2)
          .map(x => Recommendation(x._1.toString,x._2.toString)))
    }.toDF()

    //写到Mongodb
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //关闭资源
    spark.stop()

  }

  //计算余弦相似度
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }

}
