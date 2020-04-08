package com.hjf.java.spark.code;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;
import scala.collection.mutable.ListBuffer;


import java.util.*;

/**
 * @author Jiang锋时刻
 * @create 2020-04-05 22:14
 */
public class Test05 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test05");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
//                "str1", "str2", "str3", "str4",
//                "str5", "str6", "str7", "str8",
//                "str9", "str10", "str11", "str12"
//        ), 3);
//        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iter.hasNext()) {
//                    String one = iter.next();
//                    list.add("rdd1 partition = [" + index + "], values = [" + one + "]");
//                }
//                return list.iterator();
//            }
//        }, true);
////        List<String> result = rdd2.collect();
////
////        for (String s : result){
////            System.out.println(s);
////        }
//
//         /*
//          repartition, coalesce
//         */
//        JavaRDD<String> rdd3 = rdd2.repartition(4);
//        JavaRDD<String> rdd4 = rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iter.hasNext()) {
//                    String one = iter.next();
//                    list.add("rdd3 partition = [" + index + "], values = [" + one + "]");
//                }
//                return list.iterator();
//            }
//        }, true);
//        List<String> result = rdd4.collect();
//        for (String s : result){
//            System.out.println(s);
//        }



//        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("张三", "李四", "王五", "张三"));
//        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("张三", "李四", "田七", "周八"));
        /*
            zip
         */
//         JavaPairRDD<String, String> rdd3 = rdd1.zip(rdd2);
//         List<Tuple2<String, String>> result = rdd3.collect();
//         for(Tuple2<String, String> s : result){
//             System.out.println(s);
//         }

        /*
            zipWithIndex
         */
//        JavaPairRDD<String, Long> rdd4 = rdd1.zipWithIndex();
//        List<Tuple2<String, Long>> result = rdd4.collect();
//        for (Tuple2<String, Long> s :result){
//            System.out.println(s);
//        }

        /*
            groupByKey
         */
//        JavaPairRDD<String, Long> rdd4 = rdd1.zipWithIndex();
//        JavaPairRDD<String, Iterable<Long>> rdd5 = rdd4.groupByKey();
//        List<Tuple2<String, Iterable<Long>>> result = rdd5.collect();
//        for(Tuple2<String, Iterable<Long>> s : result){
//            System.out.println(s);
//        }


        /*
            reduce
         */
//        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
//        Integer result = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        System.out.println(result);


        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2("张三", 10),
            new Tuple2("张三", 10), new Tuple2("李四", 20), new Tuple2("李四", 20), new Tuple2("王五", 30),
            new Tuple2("王五", 300), new Tuple2("赵六", 40), new Tuple2("赵六", 400)
        ));

//        Map<String, Long> map = rdd1.countByKey();
//        Set<Map.Entry<String, Long>> entries = map.entrySet();
//        for(Map.Entry<String, Long> entry : entries){
//            String key = entry.getKey();
//            Long value = entry.getValue();
//
//            System.out.println("key = " + key + ", value = " + value);
//
//        }
        Map<Tuple2<String, Integer>, Long> map = rdd1.countByValue();
        Set<Map.Entry<Tuple2<String, Integer>, Long>> entries = map.entrySet();
        for(Map.Entry<Tuple2<String, Integer>, Long> entry : entries){
            Tuple2<String, Integer> key = entry.getKey();
            Long value = entry.getValue();

            System.out.println("key = " + key + ", value = " + value);

        }


    }
}
