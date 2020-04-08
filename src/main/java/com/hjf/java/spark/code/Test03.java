package com.hjf.java.spark.code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * @author Jiang锋时刻
 * @create 2020-04-05 10:07
 */
public class Test03 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Test02").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaPairRDD<String, Integer> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2("张三", 28), new Tuple2("李四", 21), new Tuple2("王五", 23), new Tuple2("赵六", 18)), 3);
        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2("张三", 87), new Tuple2("李四", 78), new Tuple2("王五", 68), new Tuple2("田七", 89)), 4);

        // JavaPairRDD<String, Tuple2<Integer, Integer>> result = nameRDD.join(scoreRDD);
        /*
            (张三,(28,87))
            (李四,(21,78))
            (王五,(23,68))
         */

        // JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> result = nameRDD.leftOuterJoin(scoreRDD);
        /*
            (张三,(28,Optional[87]))
            (李四,(21,Optional[78]))
            (赵六,(18,Optional.empty))
            (王五,(23,Optional[68]))
         */

//         JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result = nameRDD.rightOuterJoin(scoreRDD);
        /*
            (张三,(Optional[28],87))
            (李四,(Optional[21],78))
            (王五,(Optional[23],68))
         */
//        result.foreach(new VoidFunction() {
//            @Override
//            public void call(Object o) throws Exception {
//                System.out.println(o);
//            }
//        });

//        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Optional<Integer>, Integer>> one) throws Exception {
//                String name = one._1;
//                Optional<Integer> age = one._2._1;
//                Integer score = one._2._2;
//
////                if (age.isPresent()){
////                    System.out.println("姓名:" + age + " 成绩:" + score);
////                } else {
////                    System.out.println("姓名:" + name + ", 年龄:" + age + " 成绩:" + score);
////                }
//
//                System.out.println("姓名:" + name + ", 年龄:" + age.orElse(0) + " 成绩:" + score);
//                /*
//                    姓名:张三, 年龄:28 成绩:87
//                    姓名:田七, 年龄:0 成绩:89
//                    姓名:李四, 年龄:21 成绩:78
//                    姓名:王五, 年龄:23 成绩:68
//                 */
//            }
//        });

//        int count1 = nameRDD.getNumPartitions();
//        int count2 = scoreRDD.getNumPartitions();
//        JavaPairRDD<String, Tuple2<Integer, Integer>> result = nameRDD.join(scoreRDD);
//        int count3 = result.getNumPartitions();
//
//        System.out.println("nameRDD的分区数:" + count1 + ",scoreRDD的分区数:" + count2 + ",join后的分区数:" + count3);



//        JavaPairRDD<String, Tuple2<Integer, Integer>> result = nameRDD.join(scoreRDD);
//        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Integer>> tp) throws Exception {
//                System.out.println(tp);
//            }
//        });+



        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));

        // union:拼接
        // JavaRDD<Integer> result = rdd1.union(rdd2);

        // intersection:交集
        // JavaRDD<Integer> result = rdd1.intersection(rdd2);

        // subtract:差集
        // JavaRDD<Integer> result = rdd1.subtract(rdd2);

        // 并集:先union,再distinct
        JavaRDD<Integer> result = rdd1.union(rdd2).distinct();


        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.stop();


    }
}
