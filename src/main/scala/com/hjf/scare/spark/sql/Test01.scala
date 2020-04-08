package com.hjf.scare.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jiang锋时刻
 * @create 2020-04-09 0:19
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Test01").master("local").getOrCreate()
    val df: DataFrame = spark.read.json("./data/nameInfo.txt")
//    df.show(2)
//
//    df.printSchema()

//    val rows: Array[Row] = df.take(2)
//    rows.foreach(println)

    // select name, age from df where age > 18
    // val result: Dataset[Row] = df.select("name", "age").where(df.col("age").gt(18))
//    val result: DataFrame = df.select(df.col("name").equalTo("张三"), df.col("age"))
//    result.show()

    df.createOrReplaceTempView("t1")
    val result: DataFrame = spark.sql("select name, age from t1 where age > 18")
    result.show()

    df.createOrReplaceGlobalTempView("t2")
    val result1: DataFrame = spark.sql("select name, age from t1 where age > 18")
    result1.show()






    spark.stop()


  }

}
