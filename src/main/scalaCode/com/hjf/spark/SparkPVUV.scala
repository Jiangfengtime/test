package com.hjf.spark

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{AbstractSeq, mutable}
import scala.collection.mutable.ListBuffer

/**
 * @author Jiang锋时刻
 * @create 2020-04-06 16:29
 */
object SparkPVUV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("PVUV")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/pvuvdata")
    // 42.62.88.214	新疆	2018-11-12	1542011088714	734986595720971991	www.baidu.com	Click
    /*
      pv pageview:
      统计各网址访问次数

      步骤:
        1.将每一行数据以"\t"为分隔符,进行分隔
        2.将分隔后的网站组成一个key-value对. 如:(www.baidu.com, 1)
        3.统计相同key(网址)的数量
        4.将统计结果以key-value对中的value值进行排序
        5.遍历
     */
     // lines.map(line => {(line.split("\t")(5), 1)}).reduceByKey(_ + _).sortBy(tp => {tp._2}, false).foreach(println)

    /*
    任务:
      uv unique vistor
      统计各用户访问网址的次数
      即:同一个用户多次访问同一个网址时只计算一次
    思路:
      将ip地址和网址拼接成一个字符串,然后过滤掉相同的,最后统计各网址的数量

    步骤:
      1.将ip地址和网址拼接成一个字符串
      2.将拼接成的字符串去重,这样就能保证同一个用户访问同一个网址时只计算一次
      3.将拼接成的字符串分隔出网址,并组成key-value对.如:(www.baidu.com, 1)
      4.统计相同key(网址)的数量
      5.将统计结果以key-value对中的value值进行排序
      6.遍历
     */

//    lines.map(line => {line.split("\t")(0) + "_" + line.split("\t")(5)}).distinct()
//      .map(one => {(one.split("_")(1), 1)}).reduceByKey(_ + _).sortBy(_._2, false).foreach(println)

    /*
      任务:
        统计每个网址访问量前三的省份

      思路:
        将网址当做key, 省份当做value,然后调用groupByValue,将同一网址的所有地区都塞到一起,然后再统计每个省份的数量
     */

    //将网址和地址组成key-value格式   如:(www.jd.com,天津)
    val site_local: RDD[(String, String)] = lines.map(line => {
      (line.split("\t")(5), line.split("\t")(1))
    })


    //将相同的key用groupByKey组合    如:(www.mi.com,CompactBuffer(河北, 新疆, 吉林, 西藏))
    val site_localIterable: RDD[(String, Iterable[String])] = site_local.groupByKey()

    val result: RDD[(String, AbstractSeq[(String, Int)] with io.Serializable)] = site_localIterable.map(one => {
      // 创建一个可变的Map
      val cityMap = mutable.Map[String, Int]()

      val site = one._1
      // 创建迭代器
      val localIter = one._2.iterator

      while (localIter.hasNext) {
        // 依次获取每个元素
        val city: String = localIter.next()
        // 如果Map中有当前city
        if (cityMap.contains(city)) {
          // 获取Map中当前city的value值
          val count = cityMap.get(city).get
          // value值 +1
          cityMap.put(city, count + 1)
        } else {
          // 如果Map中没有当前city,则插入(city, 1)
          cityMap.put(city, 1)
        }
      }

      // 将Map中的元素list化
      val tuples: List[(String, Int)] = cityMap.toList
        // 排序 [降序]
        .sortBy(one => {
          -one._2
        })

      // 如果list元素超过3个
      if (tuples.size > 3) {
        // 创建一个可变list
        val resultList = new ListBuffer[(String, Int)]()

        // 将前三个元素插入到可变List
        for (i <- 0 to 2) {
          resultList.append(tuples(i))
        }
        // 返回List中前三个元素
        (site, resultList)

      } else {
        // 返回全部元素
        (site, tuples)
      }
    })

    // 遍历
    result.foreach(println)


  }

}
