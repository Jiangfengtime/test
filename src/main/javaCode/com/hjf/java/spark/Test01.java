package com.hjf.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.tools.nsc.util.ClassPath;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Jiang锋时刻
 * @create 2020-04-03 0:51
 */
public class Test01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Test01");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words.txt");

//        JavaRDD<String> result = lines.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String line) throws Exception {
//                return "hello world".equals(line);
//            }
//        });

//        result.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

//        System.out.println(result.count());



//        JavaRDD<Object> map = lines.map(new Function<String, Object>() {
//            @Override
//            public Object call(String s) throws Exception {
//                return s + "*";
//            }
//        });
//
//        map.foreach(new VoidFunction<Object>() {
//            @Override
//            public void call(Object o) throws Exception {
//                System.out.println(o);
//            }
//        });



//        JavaPairRDD<String, String> result = lines.mapToPair(new PairFunction<String, String, String>() {
//
//            @Override
//            public Tuple2<String, String> call(String s) throws Exception {
//                return new Tuple2<>(s, s + "#");
//            }
//        });
//
//        result.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2);
//            }
//        });


//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                List<String> list = Arrays.asList(s.split(" "));
//                return list.iterator();
//
//            }
//        });
//
//        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> resultRDD = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//
//        // key value 交换顺序
//        JavaPairRDD<Integer, String> swapRDD = resultRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return stringIntegerTuple2.swap();
//            }
//        });
//
//        //排序
//        JavaPairRDD<Integer, String> sortRDD = swapRDD.sortByKey(false);
//
//        // 交换回来
//        JavaPairRDD<String, Integer> result = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//                return integerStringTuple2.swap();
//            }
//        });
//
//
//        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tp) throws Exception {
//                System.out.println(tp);
//            }
//        });


//        List<String> collect = lines.collect();
//
//        for (String one: collect){
//            System.out.println(one);
//        }


        JavaRDD<String> sample = lines.sample(true, 0.1);
        sample.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        sc.stop();
    }
}
