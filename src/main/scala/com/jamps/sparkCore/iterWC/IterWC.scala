package com.jamps.sparkCore.iterWC

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object IterWC {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[2]").setAppName("WordCount2"))
    sc.setLogLevel("WARN")
    val inputRDD: RDD[String] = sc.textFile("datas/input/word.txt")
    println("partitions is "+inputRDD.getNumPartitions)
    // TODO: 3、对要处理的数据进行分析，调用RDD中函数（高阶函数）
    val wordCountsRDD: RDD[(String, Int)] = inputRDD
      .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
      .mapPartitions(
        iter=>{
//          val xx: Iterator[String] = xx
          iter.map((_,1))
        })
    val connts: RDD[(String, Int)] = wordCountsRDD.foldByKey(0)((x, y)=>x+y)

    connts.foreachPartition(
      iter =>{
        iter.foreach(tuple=>println(tuple))
      }
    )

    val result1: RDD[(String, Int)] = connts.coalesce(1)
    connts.saveAsTextFile("datas/output/output3-" + System.currentTimeMillis())
    result1.saveAsTextFile("datas/output/output33-" + System.currentTimeMillis())

    val result2: RDD[(String, Int)] = connts.coalesce(1,true)
    result2.saveAsTextFile("datas/output/output33ture-" + System.currentTimeMillis())

    Thread.sleep(100)
    sc.stop()

  }

}
