package com.hjf.scare.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-07 11:13
 */
object Test06 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test06")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/studentData")
    lines.map(s => {
      (MySort(s.split(" ")(1).toInt, s.split(" ")(2).toInt), s)
    }).sortByKey(false).map(_._2).foreach(println)

  }
}

case class MySort(val age:Int, val score:Int) extends Ordered[MySort]{
  override def compare(that: MySort): Int = {
    // 先按分数从高到低进行排序
    // 如果分数相同时按年龄从小到大进行排序
    if (this.score == that.score){
      this.age - that.age
    } else {
      that.score - this.score
    }
  }
}