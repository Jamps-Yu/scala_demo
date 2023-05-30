package com.jamps.sparkCore.LogAnalysis

import java.util

import scala.util.matching.Regex

object Test {

  def binarySearch(input:Int,arr:Array[Int]):Int={

    var end: Int = arr.length-1
    var start: Int = 0
    var mid: Int = end+(start-end)/2
    var count:Int=1

    if(input==arr(mid)) {
      return mid
    }
    while (mid < end){

      println(s"第$count 次比较，start is $start,mid is $mid, end is $end")
      if(input>arr(mid)){
        start=mid
        mid=end+(start-end)/2

      }else{
        end=mid
        mid=end+(start-end)/2
      }
      count +=1
    }

      mid
  }

  def binarySearchRange(input:Int, arr: List[(Int,Int)]):Int={
    var start: Int = 0
    var end: Int = arr.length-1
    var mid: Int = end+(start-end)/2
    var midLeft: Int = arr(mid)._1
    var midRight: Int = arr(mid)._2
    var count:Int=1


    if(input >= midLeft && input< midRight) {
      return mid
    }
    while (mid < end){
      println(s"第$count 次比较，start is $start,mid is $mid, end is $end")
      if(input>=midRight){
        start=mid
        mid=end+(start-end)/2
        midLeft= arr(mid)._1
        midRight= arr(mid)._2
      }else if(input<midLeft){
        end=mid
        mid=end+(start-end)/2
        midLeft= arr(mid)._1
        midRight= arr(mid)._2
      }else{return -1}
      count +=1
  }
    mid
  }

  def main(args: Array[String]): Unit = {
    val ipre: Regex = "(\\d{1,3}\\.){3}\\d{1,3}".r
//    val ipre: Regex = "[0-9]+".r

//    val ips: Regex.MatchIterator = ipre.findAllIn("183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] \"-\" 400 0 \"-\" \"-\"\n163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] \"HEAD / HTTP/1.1\" 200 20 \"-\" \"DNSPod-Monitor/1.0\"")
    val ips= ipre.findFirstIn("163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] \"HEAD / HTTP/1.1\" 200 20 \"-\" \"DNSPod-Monitor/1.0\"")
    println(ips)
    for(i <- ips) {
      println("*****")
      println(i)
    }
    val list: List[(Int,Int)] = List((1,10),(10,15),(20,30))
    val location: Int = binarySearch(49,(1 to 50).toArray)
    println(location)
    val position: Int = binarySearchRange(16,list)
    println("position is "+position)
  }

}
