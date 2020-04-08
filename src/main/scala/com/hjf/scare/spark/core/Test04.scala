package com.hjf.scare.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-05 1:48
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test04").setMaster("local")
    val sc = new SparkContext(conf)

//    val rdd: RDD[String] = sc.parallelize(List[String]("s1", "s2", "s3", "s4", "s5", "s6"), 2)

    // 相当于每执行一句就会创建一次连接
    //    val count: Long = rdd.map(one => {
    //      println("建立数据库连接...")
    //      println(s"插入数据库:$one")
    //      println("关闭数据库连接...")
    //      one + "!"
    //    }).count()
    //    println(count)

    //    rdd.foreach(one => {
    //      println("建立数据库连接...")
    //      println(s"插入数据库:$one")
    //      println("关闭数据库连接...")
    //    })




   //mapPartitions 和 foreachPartition


    // 相当于每个分区只创建一次连接
    //  val count: Long = rdd.mapPartitions(iter => {
    //    val list = new ListBuffer[String]
    //    println("建立数据库连接...")
    //    while (iter.hasNext) {
    //      val str: String = iter.next()
    //      println(s"插入数据库:$str")
    //      list.+=(str)
    //    }
    //    println("关闭数据库连接...")
    //    list.iterator
    //  }).count()
    //
    //  println(count)


//    rdd.foreachPartition(iter => {
//      println("建立数据库连接...")
//      while (iter.hasNext){
//        val str = iter.next()
//        println(s"插入数据库:$str")
//      }
//
//      println("关闭数据库连接...")
//    })


    // distinct:过滤掉重复项
//    val arrRDD: RDD[Int] = sc.parallelize(List[Int](1, 2, 2, 4, 5, 6))
//    // 实现原理和计算wordCount一样,只是后面只获取key值
//    // arrRDD.map(one => {(one, 1)}).reduceByKey((v1, v2) => {v1 + v2}).map(one => one._1).foreach(println)
//    arrRDD.distinct().foreach(println)




    val nameRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](
      ("张三", 28), ("张三", 21), ("李四", 23), ("李四", 18), ("王五", 16)))
    val scoreRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](
      ("张三", 87), ("张三", 78), ("李四", 68), ("李四", 90)))

    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = nameRDD.cogroup(scoreRDD)
    result.foreach(println)
    /*
        (张三,(CompactBuffer(28, 21),CompactBuffer(87, 78)))
        (李四,(CompactBuffer(23, 18),CompactBuffer(68, 90)))
        (王五,(CompactBuffer(16),CompactBuffer()))
     */



  }
}
