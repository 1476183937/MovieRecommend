package com.hnust.Utils

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

object DBUtil {


  private var username: String = _
  private var password: String = _
  private var driver: String = _
  private var url: String = _
  private var poolsize: String = _
  private var pool:BlockingQueue[Connection] = null


  def init(): Unit ={
    val prop = new Properties()
    val jdbcPath: String = getClass.getResource("/jdbc.properties").getPath
    prop.load(new FileInputStream(jdbcPath))
    username = prop.getProperty("jdbc.username")
    password = prop.getProperty("jdbc.password")
    driver = prop.getProperty("jdbc.driver")
    url = prop.getProperty("jdbc.url")
    poolsize = prop.getProperty("jdbc.poolsize")
    pool = new LinkedBlockingDeque[Connection](poolsize.toInt)
  }

  def main(args: Array[String]): Unit = {
    val conn: Connection = getConnection()
    val statement: PreparedStatement = conn.prepareStatement("insert into user(uid,username,password) values(?,?,?)")
    statement.setString(1,"111")
    statement.setString(2,"111")
    statement.setString(3,"111")
    statement.executeUpdate()
    conn.close()
    println(conn)

  }

  //创建数据库连接
  def createConn(): Unit ={
    //如果连接池为空，创建一个连接
    if (pool == null) {
      init()
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url,username, password)
      pool.put(connection)
    }
  }

  //获取连接
  def getConnection(): Connection ={
    if(pool != null && pool.size() > 0){
      pool.take()
    }else{
      //如果没有，创建连接
      createConn()
      getConnection()
    }
  }

  //关闭数据库连接
  def close(connection: Connection): Unit ={

    if(connection!=null){
      pool.put(connection)
    }
  }

}
