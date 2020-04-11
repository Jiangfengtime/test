package com.hjf.scala.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-07 14:43
 *
 *  广播变量:
 *    val rdd1 = sc.textFile()
 *    val list = List[String](xxx)
 *    val rdd2 = rdd1.filter(
 *      one => {
 *        list.contains(one)
 *      }
 *    }
 *    rdd2.foreach...
 *
 *  注意:
 *    1.不能将RDD广播出去,可以将RDD的结果广播出去, rdd.collect()
 *    2.广播变量只能在Driver端定义,在Executor中使用,不能在Executor中改变广播变量的值
 */
object Test07 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test07")
    val sc: SparkContext = new SparkContext(conf)

    val list: List[String] = List[String]("张三", "李四")

    // 广播
    val bcList: Broadcast[List[String]] = sc.broadcast(list)

    val nameRDD: RDD[String] = sc.parallelize(List[String]("张三", "李四", "王五"))

    val result: RDD[String] = nameRDD.filter(name => {
      val innerList: List[String] = bcList.value

      innerList.contains(name)
    })

    result.foreach(println)



























  }

}
