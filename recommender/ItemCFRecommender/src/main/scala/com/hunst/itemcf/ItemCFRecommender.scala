package com.hunst.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}

//电影评分样例类，为了和Mlib库里的Rating区别开，取名为ProductRating
case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 电影相似度（电影推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object ItemCFRecommender {

  val MONGODB_RATING_COLLECTING = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductsRecs"
  val MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    //配置定义要用到的参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.25.102:27017/shopRecomender",
      "mongo.db" -> "shopRecomender"
    )

    //创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载数据
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTING)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(
        x => (x.userId, x.productId, x.score)
      )
      .toDF("userId", "productId", "score")
      .cache()

    // 统计每个电影的评分个数，并通过内连接添加到 ratingDF 中
    val numRatersPerProduct: DataFrame = ratingDF.groupBy("productId").count()
    val ratingWithCountDF: DataFrame = ratingDF.join(numRatersPerProduct, "productId")

//    ratingWithCountDF.show()

    //TODO:核心算法，计算同现相似度，得到电影的相似度列表
//    ratingWithCountDF.join(ratingWithCountDF, "userId").show()
    val joinedDF: DataFrame = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId", "product1", "socre1", "count1", "product2", "socre2", "count2")
      .select("userId", "product1", "count1", "product2", "count2")

    //创建临时表
    joinedDF.createOrReplaceTempView("joined")

    val cooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as coocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
      """.stripMargin
    ).cache()

//    cooccurrenceDF.show()

    val simDF: DataFrame = cooccurrenceDF.map {
      // 用同现的次数和各自的次数，计算同现相似度
      row => {
        val cooSim: Double = cooccurrenceSim(row.getAs[Long]("coocount").toLong, row.getAs[Long]("count1").toLong, row.getAs[Long]("count2").toLong)
        (row.getAs[Int]("product1"), (row.getAs[Int]("product2"), cooSim))
      }
    }.rdd
      .groupByKey()
      .map {
        case (productId, recs) => {
          ProductRecs(productId,
            recs.toList.filter(x => x._1 != productId) //转换从List并过滤掉自己和自己的
              .sortWith(_._2 > _._2) //排序
              .map(x => Recommendation(x._1, x._2)) //将每一个元素转换成Recommendation
              .take(MAX_RECOMMENDATION)) //取前20个
        }
      }.toDF()

    //保存数据

    simDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", ITEM_CF_PRODUCT_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    spark.stop()

  }

//  计算同现相似度
  def cooccurrenceSim(cooCount: Long, count1: Long, count2: Long): Double ={
    cooCount / math.sqrt( count1 * count2 )
  }

}
