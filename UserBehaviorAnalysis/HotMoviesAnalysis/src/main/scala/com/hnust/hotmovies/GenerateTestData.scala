package com.hnust.hotmovies

import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, FileReader, InputStreamReader}
import java.util.Random

import util.control.Breaks._
import com.mongodb.casbah
import com.mongodb.casbah.{Imports, MongoClient, MongoClientURI, MongoCollection, MongoCursor, MongoDB}

import scala.collection.immutable.Range


//mongo配置
case class MongoConfig(uri: String, db: String)

//产生测试数据，从mongo读取
object GenerateTestData {

  def main(args: Array[String]): Unit = {
    //获取mongo客户端
    val client = MongoClient(MongoClientURI("mongodb://localhost:27017"))

    //获取数据库movieinfo02中ratings表的连接
    val collection: MongoCollection = client("movieInfo02")("ratings")

    val iterator: Iterator[Imports.DBObject] = collection.find().toIterator

    //获取文件的输出流
    val out = new FileOutputStream(new File("F:\\IDEAProjects\\ECommerceRecommendSystem\\UserBehaviorAnalysis\\HotMoviesAnalysis\\src\\main\\resources\\ratings.csv"))

    val builder = new StringBuilder
    val random = new Random()
    while (iterator.hasNext){
      builder.clear()
      val line: casbah.Imports.DBObject = iterator.next()
      Thread.sleep(10)
      builder.append(line.get("uid") + "|" + (line.get("mid").toString.toInt -random.nextInt(5)) + "|" + line.get("rating") + "|" +System.currentTimeMillis() + "\n")

      out.write(builder.toString().getBytes())
      println(builder.toString())
    }
    out.close()

    client.close()

    genMoreData()



  }

  //将获取到的数据再扩大几倍
  def genMoreData(): Unit = {

    var reader = new BufferedReader(new FileReader("F:\\IDEAProjects\\ECommerceRecommendSystem\\UserBehaviorAnalysis\\HotMoviesAnalysis\\src\\main\\resources\\ratings.csv"))
    val out = new FileOutputStream(new File("F:\\IDEAProjects\\ECommerceRecommendSystem\\UserBehaviorAnalysis\\HotMoviesAnalysis\\src\\main\\resources\\ratings.txt"))
    for (i <- 1 to 20) {
      //读取文件
      var line: String = ""
      val builder = new StringBuilder
      val random = new Random()
      line = reader.readLine()
      while (line != null && !line.equals("")) {
        println("line:" + line)

        val splits: Array[String] = line.split("\\|")

        builder.append(splits(0) + "|" + (splits(1).toString.toInt - random.nextInt(5)) +
          "|" + splits(2) + "|" + (splits(3).toString.toLong + 222000 * i) + "\n")

        out.write(builder.toString().getBytes())
        //        println(builder.toString())
        println("\n")
        builder.clear()
        line = reader.readLine()
      }

      reader.close()
      reader = new BufferedReader(new FileReader("F:\\IDEAProjects\\ECommerceRecommendSystem\\UserBehaviorAnalysis\\HotMoviesAnalysis\\src\\main\\resources\\ratings.csv"))

    }
    if (reader != null){
      reader.close()
    }
    if (out != null){
      out.close()
    }
  }

}
