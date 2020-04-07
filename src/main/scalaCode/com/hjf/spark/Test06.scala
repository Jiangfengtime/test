package com.hjf.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-07 0:13
 */
object Test06 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test06").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val studentInfo: RDD[(String, Int, Int)] = sc.parallelize(Array(
      ("张三", 20, 80), ("李四", 23, 90), ("王五", 21, 95), ("赵六", 22, 90)
    ))
    // 按分数从高到低排序
     val result: RDD[(String, Int, Int)] = studentInfo.sortBy(_._3, false)
//    val result: RDD[(String, Int, Int)] = studentInfo.sortBy(_._3, false).sortBy(_._2)
    result.foreach(println)
  }

}


object MySort{
  implicit val ordering = new Ordering[]
}