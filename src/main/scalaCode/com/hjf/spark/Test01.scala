package com.hjf.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-02 23:22
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Test01")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

//    val rdd: RDD[String] = sc.parallelize(Array[String]("a", "b"))
    val rdd: RDD[String] = sc.makeRDD(Array[String]("a", "b"))
    rdd.foreach(println)



//    val lines: RDD[String] = sc.textFile("./data/words.txt")

    // map:一对一
    // lines.map(one => {one + "#"}).foreach(println)

    // flatMap:一对多
    // lines.flatMap(one => {one.split(" ")}).foreach(println)

    // filter:过滤
    // val words: RDD[String] = lines.flatMap(one => {one.split(" ")})
    // words.filter(one => {"hello".equals(one)}).foreach(println)

//    val words: RDD[String] = lines.flatMap(one => {
//      one.split(" ")
//    })
//    val pairWords: RDD[(String, Int)] = words.map(one => (one, 1))
//
//    val reduce: RDD[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {v1 + v2})

    // sortBy:排序.默认正序. true:正序, false:倒序
    // val result: RDD[(String, Int)] = reduce.sortBy(tp => tp._2, false)
    // result.foreach(println)

    // val transReduce: RDD[(Int, String)] = reduce.map(tp => {tp.swap})
    // val result: RDD[(Int, String)] = transReduce.sortByKey(false)
    // result.map(_.swap).foreach(println)

    // sample():
    // 第一个参数,是否有放回的抽样
    // 第二个参数,设置抽取比例 [接近这个比例]
    // 第三个参数,设置种子.设置后,重复执行时,结果不会发生变化
    // val result: RDD[String] = lines.sample(true, 0.1, 100L)
    // result.foreach(println)

    // count()
    // val count: Long = lines.count()
    // println(count)

    // collect():将结果回收到Driver端
    // val result: Array[String] = lines.collect()
    // result.foreach(println)

    // first():获取第一条数据
    // val result: String = lines.first()
    // println(result)

    //take(n):获取前n条数据
//    val result: Array[String] = lines.take(5)
//    result.foreach(println)



  }

}
