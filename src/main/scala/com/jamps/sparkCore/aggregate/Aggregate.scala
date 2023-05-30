package com.jamps.sparkCore.aggregate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Aggregate {
  def main(args: Array[String]): Unit = {
    /**wordcount*/
//    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("Aggregate"))
//    sc.setLogLevel("WARN")
//
//    val inputRDD: RDD[String] = sc.textFile("datas/group/group.data")
//    val wordCountsRDD: RDD[(String, Int)] = inputRDD
//      .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
//      .mapPartitions(
//        iter=>{
//          //          val xx: Iterator[String] = xx
//          iter.map((_,1))
//        })
//
//    val result: RDD[(String, Int)] = wordCountsRDD.aggregateByKey(0)(
//      (k: Int, v: Int) =>
//        k + v
//      ,
//      (k: Int, v: Int) =>
//        k + v
//    )
//    val result2: RDD[(String, Int)] = result.coalesce(1,true)
//    result2.saveAsTextFile("datas/output/outputAggWCture-" + System.currentTimeMillis())
    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf()
      .setMaster("local[1]")
      .setAppName("Aggregate"))
        sc.setLogLevel("WARN")
    val inputGrpSortRDD: RDD[String] = sc.textFile("datas/group/group.data",1)
    val rawdata: RDD[(String, Int)] = inputGrpSortRDD.mapPartitions(
      iter => iter.map(line => {
        val arr: Array[String] = line.split(" ")
        (arr(0), arr(1).toInt)
      }))

    /**使用groupbyKey然后进行分组聚合
      */
    val groupeddata: RDD[(String, Iterable[Int])] = rawdata.groupByKey()
    val resultGrouped: RDD[(String, List[Int])] = groupeddata.mapPartitions(
      iter => iter.map {
        //    case (k:String,x:Iterable[Int])=>println(k+x.toList.mkString(","))
        //    (k:String,x:Int)=>println(k+x.toList.mkString(","))
        //        x => {
        //          val y: (String, Iterable[Int]) = x
        //          (y._1, y._2.toList.sortBy(x => -x).take(3))
        //        }
        case (x: String, y: Iterable[Int]) => (x, y.toList.sortBy(x => -x).take(3))
      })
    resultGrouped.foreachPartition(iter=>{
      iter.foreach(x=>println(x))
    })
println("********************")
    val resultRDD: RDD[(String, ListBuffer[Int])] = rawdata.aggregateByKey(new ListBuffer[Int])(
      (u: ListBuffer[Int], v: Int) => {
        u += v;
        val partitionSorted: ListBuffer[Int] = u.sortBy(x=> -x)
        // println("in partition,in different key "+partitionSorted.mkString(","))
        //返回前三个
        partitionSorted.take(3)
      },
      (u: ListBuffer[Int], v: ListBuffer[Int]) => {
        val resultRDD: ListBuffer[Int] = u ++ v
        resultRDD.sortBy(x => -x).take(3)
      })
    resultRDD.foreachPartition(
      iter=>{
        iter.foreach(x=>println(x._1+" "+x._2.toList.mkString("--")))
      }
    )
//    val stringToInts: collection.Map[String, ListBuffer[Int]] = resultRDD.collectAsMap()
    println("================================")

    val maxGrouped: RDD[(String, Int)] = rawdata.reduceByKey(
      (a: Int, b: Int) => {
        if (a > b) {
          a
        } else b
      }
    )
    maxGrouped.foreachPartition(iter=> iter.foreach{
      case (x:String,y:Int) if (y>98) => println(x+"偏函数大于98"+y)
      case (x:String,y:Int) if (y<99) => println(x+"偏函数小于等于98"+y)
    })



    Thread.sleep(1000000)
    sc.stop()
    //resultTop3.count()



  }

}
