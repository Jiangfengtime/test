package com.hjf.scare.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-05 20:03
 */
object Test05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test05").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    /*
      countByKey:统计key-value对中相同key的数量
      countByValue:将原数据对当做key值,统计各key值(原数据)的数量
     */

//    val rdd1: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](
//      ("a", 100), ("b", 200), ("c", 300), ("a", 100), ("b", 2), ("c", 300)))
//    val count: collection.Map[String, Long] = rdd1.countByKey()
//    count.foreach(println)
//    /*
//      (a,2)
//      (b,2)
//      (c,2)
//     */

//    val values: collection.Map[(String, Int), Long] = rdd1.countByValue()
//    values.foreach(println)
//    /*
//      ((b,2),1)
//      ((a,100),2)
//      ((c,300),2)
//      ((b,200),1)
//     */

    // 不一定是key-value格式
    val rdd2: RDD[String] = sc.parallelize(List[String]("hello", "world", "hive", "hbase", "hive", "world"))
    val result: collection.Map[String, Long] = rdd2.countByValue()
    result.foreach(println)
    /*
      (hive,2)
      (hello,1)
      (hbase,1)
      (world,2)
     */






    /*
      reduce
     */
//    val rdd1: RDD[Int] = sc.parallelize(List[Int](1, 2, 3, 4, 5))
//    val result: Int = rdd1.reduce((v1, v2) => {v1 + v2})
//    // val result: Int = rdd1.reduce(_ + _)
//    println(result)



    /*
      groupByKey:根据key值聚合
     */
//    val rdd1: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](("a", 10), ("a", 20), ("b", 30), ("c", 40)))
//    val result: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
//    result.foreach(println)
//    /*
//      (a,CompactBuffer(10, 20))
//      (b,CompactBuffer(30))
//      (c,CompactBuffer(40))
//     */


    /*
      zip, zipWithIndex
     */
//    val rdd1: RDD[String] = sc.parallelize(List[String]("张三", "李四", "王五", "赵六"))
//    val rdd2: RDD[String] = sc.parallelize(List[String]("100", "200", "300", "400"))
//    // zip:将两个数据集压缩在一起,但必须保证两个数据集中数据量必须保证一致,否则会报错
//     val result: RDD[(String, String)] = rdd1.zip(rdd2)
//     result.foreach(println)
//    /*
//      (张三,100)
//      (李四,200)
//      (王五,300)
//      (赵六,400)
//     */
//
//    // zipWithIndex:将数据集中的内容和其在数据集中下标组成key-value压缩
////    val result: RDD[(String, Long)] = rdd1.zipWithIndex()
////    result.foreach(println)
//    /*
//      (张三,0)
//      (李四,1)
//      (王五,2)
//      (赵六,3)
//     */

    /*
      repartition, coalesce
     */
//    val rdd1: RDD[String] = sc.parallelize(List[String](
//      "str1", "str2", "str3", "str4",
//      "str5", "str6", "str7", "str8",
//      "str9", "str10", "str11", "str12"
//    ), 3)
//    // index: 分区号
//    // iter:分区号下的数据
//    val rdd2: RDD[String] = rdd1.mapPartitionsWithIndex((index, iter) => {
//      val list = new ListBuffer[String]()
//      while (iter.hasNext) {
//        val one: String = iter.next()
//        list.+=(s"rdd1 partition = [$index], values = [$one]")
//      }
//      list.iterator
//    })
//    //rdd2.foreach(println)
//
//    // repartition:可以修改分区数,默认会产生shuffle  [宽依赖]
//    // coalesce:可以修改分区数,默认不会产生shuffle   [窄依赖]
//    // coalesce(2, true):就相当于repartition.repartition的底层就是coalesce(2, true)
//
//    // 注意:当coalesce由少的分区分到多的分区时,不让产生suffer,不起作用.
//    // repartition常用于增多分区
//    // coalesce常用于减少分区
//    val rdd3: RDD[String] = rdd2.coalesce(4)
//    val rdd4: RDD[String] = rdd3.mapPartitionsWithIndex((index, iter) => {
//      val list = new ListBuffer[String]()
//      while (iter.hasNext) {
//        val one: String = iter.next()
//        list.+=(s"rdd1 partition = [$index], values = [$one]")
//      }
//      list.iterator
//    })
//
//    //rdd4.foreach(println)
//
//    val result: Array[String] = rdd4.collect()
//    result.foreach(println)

  }

}
