package com.jamps.sparkCore.worldCount

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {

  def saveToMysql(iter:Iterator[(String, Int)]):Unit= {
    //连接mysql
//    val dbc = "jdbc:mysql://node02:3306/user?user=root&password=root123"
//    val conn = DriverManager.getConnection(dbc)
    classOf[com.mysql.jdbc.Driver]


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/user"
    val username = "root"
    val password = "rootyjp"

    // do database insert
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)


      val prep = connection.prepareStatement("INSERT INTO sparkTest (id,name, count) VALUES (null,?, ?) ")
      iter.foreach(x => {
        val name: String = x._1
        val count: Int = x._2
        prep.setString(1, name)
        prep.setInt(2, count)
        prep.executeUpdate()
      })
      connection.close()
    }

  }

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[2]").setAppName("WordCount2"))
    sc.setLogLevel("WARN")
    val inputRDD: RDD[String] = sc.textFile("datas/input/word.txt")
    // TODO: 3、对要处理的数据进行分析，调用RDD中函数（高阶函数）
    val wordCountsRDD: RDD[(String, Int)] = inputRDD
      // 对每行数据按照分割父分割
      .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
      // 将单词转换为二元组，表示每个单词出现一次
      .map(word => (word, 1))
      // 按照Key进行聚合操作，将相同的Key的出现的次数相加
      .reduceByKey((a, b) => a + b)



    // TODO：4、将结果数据保存到文件系统中（本地文件系统）
    //wordCountsRDD.saveAsTextFile("datas/output-" + System.currentTimeMillis())

    wordCountsRDD.foreach(tuple => println(tuple))
    val l = wordCountsRDD.count()
    println(l)

//      prep.setString(1, "Nothing great was ever achieved without enthusiasm.")
//      prep.setString(2, "Ralph Waldo Emerson")
    Thread.sleep(1000000)
    wordCountsRDD.foreachPartition(saveToMysql _)
    Thread.sleep(1000000)
    //      prep.executeUpdate

    // 为了开发测试，会将线程休眠

    // 当应用运行完成以后，关闭资源
    sc.stop()

  }
}
