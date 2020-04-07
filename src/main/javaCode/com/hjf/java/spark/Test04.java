package com.hjf.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * @author Jiang锋时刻
 * @create 2020-04-05 15:41
 */
public class Test04 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Test04").setMaster("local");
        SparkContext sc = new SparkContext(conf);


    }
}
