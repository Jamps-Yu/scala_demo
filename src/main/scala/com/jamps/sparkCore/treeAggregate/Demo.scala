package com.jamps.sparkCore.treeAggregate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContext.getOrCreate(new SparkConf()
      .setMaster("local[1]")
      .setAppName("Aggregate"))
    sc.setLogLevel("WARN")
    val a = (1 to 10).toList

    val i: Int = a.fold(0)(_+_)
    val j: Int = a.aggregate(0)(
      {case (a: Int, b: Int)if(a<=10) => a + b
      case (a: Int, b: Int)if(a>10) => a
      },
      (a: Int, b: Int) => a + b
    )
    println(j)

    val aa: RDD[Int] = sc.parallelize(a)
    val jj: Int = aa.treeAggregate(0)(
      { case (a: Int, b: Int) if (a <= 10) => a + b
      case (a: Int, b: Int) if (a > 10) => a
      },
      (a: Int, b: Int) => a + b, depth = 2
    )
    println(jj)

  }

}
