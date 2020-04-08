package com.hjf.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author Jiang锋时刻
 * @create 2020-04-08 20:55
 */
public class Test01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test01");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        /*
            Dateset 有数据,有列的schema
            sqlContext 读取json文件加载成Dateset, Dateset中的列会按照ASCII码排序
            写sql加载出来的dataFrame会按照指定的字段顺序显示列

            注意:
                1. json文件中不能嵌套json格式的内容
                2. 读取json格式的两种方法
                     1)sqlContext.read().format("json").load(path)
                     2)sqlContext.read().json(path)
                3. ds.show(num):显示前n行内容
                4. ds.javaRDD/scala.df.rdd 将Dataset转换成RDD
                5. ds.printSchema()显示Dataset中的Schema信息
                6. 想查询sql语句,首先要将Dataset注册成临时表ds.registerTempTable(tableName)
                   再使用sql:sqlContext.sql("sql语句")
                7. df加载过来之后将按ascii排序
         */
        // Dataset<Row> ds = sqlContext.read().format("json").load("./data/nameInfo.txt");
        Dataset<Row> ds = sqlContext.read().json("./data/nameInfo.txt");
        // show(num):显示所有数据, 指定显示行数
        // ds.show();

        JavaRDD<Row> javaRDD = ds.javaRDD();
        javaRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                // System.out.println(row);
//                System.out.println(row.get(1));

                //报错
//                System.out.println(row.getAs("name"));
            }
        });


        // printSchema:打印字段类型
//        ds.printSchema();

        // select name, age from ds where age > 18
//        ds.select("name", "age").where(ds.col("age").gt(18)).show();

        // 将DataFrame注册成临时表
        // t1这张表不在内存中,也不在磁盘中,相当于一个指针指向源文件,底层操作解析 spark job读取源文件
        ds.registerTempTable("t1");
        Dataset<Row> sql = sqlContext.sql("select name, age from t1 where age > 18");
        sql.show();


        sc.stop();


    }
}
