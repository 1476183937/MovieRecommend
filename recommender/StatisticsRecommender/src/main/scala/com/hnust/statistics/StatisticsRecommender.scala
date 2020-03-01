package com.hnust.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Rating(uid: String, mid: String, rating: String, date: String)

case class MongoConfig(uri: String, db: String)

/**
  * 离线统计模块
  */
object StatisticsRecommender {

  val MONGODB_RATING_COLLECTING = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"  //历史热门电影
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"  //最近热门电影
  val AVERAGE_MOVIES = "AverageMovies"       //电影平均得分

  def main(args: Array[String]): Unit = {

    //配置定义要用到的参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender"
    )

    //创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //读取数据
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTING)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张临时表:ratings
    ratingDF.createOrReplaceTempView("ratings")

    // TODO:不同的统计推荐结果

    //1.历史热门电影统计
    val rateMoreMOVIEsDF: DataFrame = spark.sql("select mid, count(mid) as count from ratings group by mid")
    //保存到mogodb
    rateMoreMOVIEsDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //2.最近热门电影统计：按月为单位计算最近时间的月份里面评分数量最多的电影集合
    //定义UDF函数对日期进行转换：yyyyMM
//    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
//    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //2009-03-19
    spark.udf.register("changeDate", (x: String) => {
      val values: Array[String] = x.split("-")
      values(0)+values(1)
    })


    //将原来的时间戳转换成yyyyMM格式
    val ratingOfYearMonth: DataFrame = spark.sql("select mid,rating,changeDate(date) as yearmonth from ratings")
    //创建一个临时表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    //从临时表ratingOfMonth中进行统计，
    val rateMoreRecentlyMOVIEs: DataFrame = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth,mid order by yearmonth desc,count desc")

    //保存到mongodb
    rateMoreRecentlyMOVIEs.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3.电影平均得分统计
    val averageMOVIEsDF: DataFrame = spark.sql("select mid,avg(rating) as avg from ratings group by mid")
    //保存到mongodb
    averageMOVIEsDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", AVERAGE_MOVIES)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    spark.stop()

  }

}
