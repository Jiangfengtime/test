package com.hjf.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-03 19:13
 */
object Test02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Cache")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
//    sc.setCheckpointDir("./data/ck")
    var lines: RDD[String] = sc.textFile("E://data/persistData.txt")

//    lines.checkpoint()
//    lines.count()












//    val startTime1 = System.currentTimeMillis()

    //lines = lines.cache()
//    lines = lines.persist(StorageLevel.MEMORY_ONLY_SER)




//    val result1: Long = lines.count()
//    val endTime1 = System.currentTimeMillis()
//    println(result1)
//    println("运行时间:" + (endTime1 - startTime1))
//
//
//    val startTime2 = System.currentTimeMillis()
//
//    val result2: Long = lines.count()
//    val endTime2 = System.currentTimeMillis()
//    println(result2)
//    println("运行时间:" + (endTime2 - startTime2))





  }


}
