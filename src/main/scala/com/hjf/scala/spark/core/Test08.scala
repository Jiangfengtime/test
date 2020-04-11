package com.hjf.scala.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
 * @author Jiang锋时刻
 * @create 2020-04-07 15:03
 *    累加器
 *      val rdd = sc.textFile...
 *      val i = 0
 *      val rdd2 = rdd.map(one => {
 *        i += 1
 *        one
 *      })
 *      rdd3.collect()
 *      println("i = " + i
 *    注意:
 *      累加器在Driver端定义初始化
 */
object Test08 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Test08").master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //定义累加器
    val accumulator: LongAccumulator = sc.longAccumulator



    val lines: RDD[String] = sc.textFile("./data/words.txt")
//    var i = 0
    val rdd1: RDD[String] = lines.map(one => {
//      i += 1
      accumulator.add(1)
      one
    })
    rdd1.collect()
//    println(s"i = $i")
    println(s"accumulator = ${accumulator.value}")







  }

}
