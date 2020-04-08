package com.hjf.scare.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-08 0:58
 *
 *    管道模式
 */
object Test09 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test09")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/pipelineData")
    val value: RDD[String] = lines.map(x => {
      println("*** map ***")
      x + "`"
    })

    val result: RDD[String] = value.filter(one => {
      println(s"*** filter *** $one")
      true
    })

    result.count()
  }
}
