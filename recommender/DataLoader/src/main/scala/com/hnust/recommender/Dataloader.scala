package com.hnust.recommender

import java.net.URL

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//创建样例类
case class Product(productId: Int, name:String, imageUrl:String, categories:String, tags:String)
case class Rating(userId:Int, productId:Int, score:Double, timestamp:Int)
case class MongoConfig(uri:String, db:String)

object Dataloader {


//  val PRODUCT_DATA_PATH = "F:\\IDEAProjects\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
//  val RATING_DATA_PATH = "F:\\IDEAProjects\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val PRODUCT_DATA_PATH = getClass.getClassLoader.getResource("products.csv").getPath
  val RATING_DATA_PATH = getClass.getClassLoader.getResource("ratings.csv").getPath

  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTING = "Rating"

  def main(args: Array[String]): Unit = {

    val url: URL = this.getClass.getClassLoader.getResource("products.csv")
    println(url.getPath)

    //配置定义要用到的参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/shopRecomender",
      "mongo.db" -> "shopRecomender"
    )

    //创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Dtaloader")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //读取数据
    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)

    //对数据进行处理，转换成DF
    val productDF: DataFrame = productRDD.map(item => {
      val attrs: Array[String] = item.split("\\^")
      Product(attrs(0).toInt, attrs(1).trim, attrs(4).trim, attrs(5).trim, attrs(6).trim)
    }).toDF()

    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attrs: Array[String] = item.split(",")

      Rating(attrs(0).toInt, attrs(1).toInt, attrs(2).toDouble, attrs(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    storeDtaInMongoDB(productDF, ratingDF)

    //关闭资源
    spark.stop()

  }

  //保存数据到Mongodb
  def storeDtaInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //获取mongodb的客户端,类似于获取连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //获取相应的表
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTING)

    //如果已存在相应的表，则删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //保存数据到mongodb
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTING)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表创建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))

    //关闭资源
    mongoClient.close()

  }

}
