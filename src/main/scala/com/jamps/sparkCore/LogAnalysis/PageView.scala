package com.jamps.sparkCore.LogAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object PageView {

  def ipToLong(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    // 返回
    ipNum
  }


  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[2]").setAppName("PageView"))

    // TODO: 2、加载IP地址信息库，获取起始IP地址Long类型值、结束IP地址Long类型值和对应的经度与维度
    val ipInfoRDD: RDD[(Long, Long, String, String)] = sc
      .textFile("datas/ips/ip.txt", minPartitions = 2)
      // 提取字段信息
      .mapPartitions{ datas =>
      // 针对分区数据操作
      datas.map{ info =>
        val arr = info.split("\\|")
        // 以四元组形式返回
        (arr(2).toLong, arr(3).toLong, arr(arr.length - 2), arr(arr.length - 1))
      }
    }
    //todo 获取日志中的ip地址
    val logInputRDD: RDD[String] = sc.textFile("datas/logs/access.log")

    val allIPRDD: RDD[String] = logInputRDD.mapPartitions(iter => {
      //val iter: Iterator[String] = iter
      val ipre: Regex = "(\\d{1,3}\\.){3}\\d{1,3}".r
      iter.map(x => ipre.findFirstIn(x).get).filter(x=> (x!=None))
    })
    allIPRDD.foreach(x=> {
      println(x)
      println(ipToLong(x))
      //println(x.isInstanceOf[Any])
    })

    sc.stop()



  }

}
