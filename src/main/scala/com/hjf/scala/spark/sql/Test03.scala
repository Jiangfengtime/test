package com.hjf.scala.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Jiang锋时刻
 * @create 2020-04-09 10:25
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("Test03").getOrCreate()

//    val frame: DataFrame = spark.read.format("json").load("./data/NestJson.txt")
//    frame.createOrReplaceTempView("infoView")
//    spark.sql("select name, info.age, score, info.gender from infoView").show(3)

    val frame: DataFrame = spark.read.format("json").load("./data/jsonArray.txt")
//    frame.createOrReplaceTempView("infoView")
//    spark.sql("select name, info.age, score, info.gender from infoView").show(3)

    // false 参数表示结果不会省略部分字段
    frame.show(false)
    frame.printSchema()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // explode:用于将所有的json格式展开
    val transDF: DataFrame = frame.select($"name", $"age", explode($"scores")).toDF("name", "age", "allScores")
    transDF.show(4)

    val result: DataFrame = transDF.select($"name", $"age", $"allScores.语文" as "语文", $"allScores.数学" as "数学", $"allScores.英语" as "英语")
    result.show()


  }

}
