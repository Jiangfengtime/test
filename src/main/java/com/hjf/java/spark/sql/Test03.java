package com.hjf.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * @author Jiang锋时刻
 * @create 2020-04-09 10:23
 */
public class Test03 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Test03").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
    }
}
