package com.hjf.scala.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-10 14:54
 */
object pipelineTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("pipeline")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("./data/words.txt")
    val rdd2: RDD[String] = rdd1.map(x => {
      println("*** map ***" + x)
      x + "~"
    })
    val rdd3 = rdd2.filter(one => {
      println(s"=== filter === $one")
      true
    })

    rdd3.count()
  }

}
