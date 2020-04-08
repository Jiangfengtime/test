package com.hjf.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.omg.DynamicAny.NameDynAnyPair;

import java.util.Arrays;

/**
 * @author Jiang锋时刻
 * @create 2020-04-08 23:48
 */
public class Test02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Test02").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
                "{'name':'张三', 'age':'18'}",
                "{'name':'李四', 'age':'19'}",
                "{'name':'王五', 'age':'20'}"
        ));
        JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
                "{'name':'张三', 'score':'90'}",
                "{'name':'李四', 'score':'85'}",
                "{'name':'王五', 'score':'95'}"
        ));

        Dataset<Row> nameDs = sqlContext.read().json(nameRDD);
        nameDs.show();
        Dataset<Row> scoreDs = sqlContext.read().json(scoreRDD);
        scoreDs.show();

        nameDs.registerTempTable("name");
        scoreDs.registerTempTable("score");

        Dataset<Row> sql = sqlContext.sql("select name.name, age, score.score " +
                "from name join score on name.name = score.name");
//        Dataset<Row> sql = sqlContext.sql("select name.name, age, score " +
//                "from name, score where name.name = score.name");
        sql.show();


        sc.stop();
    }
}
