package com.hnust.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

import scala.collection.mutable

//case class Movie(mid: String, name: String, imageUrl: String, categories: String, tags: String)
case class MovieInfo(mid:String,name:String, imgUrl:String, director:String, screenwriter:String,
                     mainActors:String, categories:String, location:String, language:String, releaseDate:String, runTime:String,
                     alias:String, summery:String, rating_num:String,tags:String)

case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(mid: String, score: Double)

// 电影相似度（电影推荐）
case class ProductRecs(mid: String, recs: Seq[Recommendation])

//基于内容的推荐
object ContentRecommender {

  // 定义常量
  val MONGODB_MOVIE_COLLECTION = "movieInfos"
  val CONTENT_MOVIE_RECS = "ContentBaseMovieRecs"


  def main(args: Array[String]): Unit = {

    //配置定义要用到的参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender"
    )

    //创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载数据
    val movieTagsDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieInfo]
      .map(
        x => (x.mid, x.name, x.tags.map(c => if (c == '|') ' ' else c))
      )
      .toDF("mid", "name", "tags")
      .cache()

    //实例化一个分词器，用来做分词，默认按空格分开
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    //用分词器做转换，得到一个新列words的DF
    val wordsData: DataFrame = tokenizer.transform(movieTagsDF)

    wordsData.show()

    //定义一个hashingTF工具，计算词频
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").setNumFeatures(500)
    //特征化的数据
    val featurizedDataDF: DataFrame = hashingTF.transform(wordsData)

    featurizedDataDF.show(truncate = false)

    //定义一个IDF工具，计算TF-IDF
    val idf: IDF = new IDF().setInputCol("rowFeatures").setOutputCol("features")
    //训练一个idf模型
    val iDFModel: IDFModel = idf.fit(featurizedDataDF)
    //得到一个新列features的DF
    val rescaledData : DataFrame = iDFModel.transform(featurizedDataDF)

    rescaledData.show(truncate = false)


    //从rescaledData中只提取出mid和表示特征的features，再进行转换
    val movieFeatures: RDD[(Int, DoubleMatrix)] = rescaledData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    ).rdd
      .map {
        case (mid, feature) => (mid, new DoubleMatrix(feature))
      }


    // 计算笛卡尔积并过滤合并，将自己和自己的合并过滤
    val movietRecs: DataFrame = movieFeatures.cartesian(movieFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) =>
          val simScore: Double = consinSim(a._2, b._2) //求余弦相似度
          (a._1, (b._1, simScore)) //(a的mid，(b的mid，a和b的相似度))
      }
      .filter(_._2._2 > 0.4) //过滤出相似度大于0.4的
      .groupByKey() //分组
      .map {
      case (mid, items) => ProductRecs(mid.toString, items.toList.sortWith(_._2 >_._2).map(x => Recommendation(x._1.toString,x._2)))
    }.toDF()


    movietRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()

  }

  //计算余弦相似度
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }

}
