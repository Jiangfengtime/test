package com.hjf.scare.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-04 23:12
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test03")
    val sc = new SparkContext(conf)


//    val nameRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](("张三", 28), ("李四", 21), ("王五", 23), ("赵六",
//      18)))
//
//    val scoreRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](("张三", 87), ("李四", 78),("王五", 68)))

    // join:只会返回左右两侧都包含的key值
    // val result: RDD[(String, (Int, Int))] = nameRDD.join(scoreRDD)
    // result.foreach(println)
    /*
      (张三,(28,87))
      (李四,(21,78))
      (王五,(23,68))
     */


    // join:返回右侧都包含的key值为主
    // val result: RDD[(String, (Option[Int], Int))] = nameRDD.rightOuterJoin(scoreRDD)
    // result.foreach(println)
    /*
      (张三,(Some(28),87))
      (李四,(Some(21),78))
      (王五,(Some(23),68))
     */

    // join:返回左侧都包含的key值为主
    // val result: RDD[(String, (Int, Option[Int]))] = nameRDD.leftOuterJoin(scoreRDD)
    // result.foreach(println)
    /*
      (张三,(28,Some(87)))
      (李四,(21,Some(78)))
      (赵六,(18,None))
      (王五,(23,Some(68)))
     */
    // result.foreach(one => {
    //   val name = one._1
    //   val age = one._2._1
    //   val score = one._2._2.getOrElse("未参加考试")
    //   println(s"姓名:$name, 年龄:$age, 成绩:$score")
    // })
    /*
      姓名:张三, 年龄:28, 成绩:87
      姓名:李四, 年龄:21, 成绩:78
      姓名:赵六, 年龄:18, 成绩:未参加考试
      姓名:王五, 年龄:23, 成绩:68
     */
    // fullOuterJoin 左右两侧有的key,全都保留
    // val result: RDD[(String, (Option[Int], Option[Int]))] = nameRDD.fullOuterJoin(scoreRDD)
    // result.foreach(println)


//    val nameRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](("张三", 28), ("李四", 21), ("王五", 23), ("赵六",
//      18)), 3)
//    val scoreRDD: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](("张三", 87), ("李四", 78),("王五", 68)), 4)
//
//    val count1: Int = nameRDD.getNumPartitions
//    val count2: Int = scoreRDD.getNumPartitions

//    val result: RDD[(String, (Option[Int], Int))] = nameRDD.rightOuterJoin(scoreRDD)
//    val count3 :Int = result.getNumPartitions
//
//    // 合并后的分区数和分区数多的那个保持一致
//    println(s"nameRDD有 $count1 个分区, scoreRDD有 $count2 个分区, result有 $count3 个分区")
//    // nameRDD有 3 个分区, scoreRDD有 4 个分区, result有 4 个分区

    // union:拼接
//    val result: RDD[(String, Int)] = nameRDD.union(scoreRDD)
//    // result.foreach(println)
//    // 分区数等于之和
//    val count3 :Int = result.getNumPartitions
//    println(count3)


//    val rdd1: RDD[Int] = sc.parallelize(List[Int](1, 2, 3, 4))
//    val rdd2: RDD[Int] = sc.parallelize(List[Int](3, 4, 5, 6))

    // intersection :交集
    // val result: RDD[Int] = rdd1.intersection(rdd2)

    // subtract:差集
    // val result: RDD[Int] = rdd1.subtract(rdd2)

    // 求并集（先union，再distinct）
    // val result: RDD[Int] = rdd1.union(rdd2).distinct()
    // result.foreach(println)


  }

}
