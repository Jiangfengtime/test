package com.hjf.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-02 16:11
 */

//RDD:Resilient Distributed Dateset 弹性分布式数据集.
// RDD五大特性:
// 1.RDD是由一些列partition组成
// 2.算子(函数)作用在RDD的partition上的
// 3.RDD之间有依赖关系
// 4.分区器是作用在k,v格式的RDD上
// 5.partition提供数据计算的最佳位置.利于数据处理的本地化."计算移动,数据不移动"


// 1.Spark读取HDFS中数据的方法textFile底层是调用MR读取HDFS文件的方法,
//  首先会split,每个split对应一个block,每个split对应生成RDD的每个partition.

// 2.什么是k,v格式的RDD?
//  RDD中的数据是一个个的tuple2数据,那么这个RDD就是k,v格式的RDD

// 3.哪里体现了RDD的弹性(容错)
//  1).RDD之前有依赖关系
//  2).RDD的partition可多可少

// 4.哪里体现了RDD的分布式?
//  RDD的partition是分布在多个节点上的




object SparkWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // conf 可以设置SparkApplication的名称;设置Spark运行的模式;设置spark运行的资源
    conf.setAppName("wordCount")
    conf.setMaster("local")
    conf.set("spark.driver.memory", "1g")

    //SparkContext是通往spark集群的唯一通道
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/words.txt")
    val words: RDD[String] = lines.flatMap(line => {
      line.split(" ")
    })

    val pairWords: RDD[(String, Int)] = words.map(word => {
      new Tuple2(word, 1)
    })

    val result: RDD[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })

    result.foreach(one => {
      println(one)
    })
  }



//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.setAppName("wordCount")
//    conf.setMaster("local")
//
//
//    val sc = new SparkContext(conf)
//    sc.textFile("./data/words.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).foreach(println)
//
//    sc.stop()
//
//  }



}
