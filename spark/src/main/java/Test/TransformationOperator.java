package Test;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-12 23:11
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 */
public class TransformationOperator {

    public static  SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
    public static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupBykey();
        //reduceBykey();
      // sortBykey();
        //join();
        //union();
        // intersection();
        // cartesian();
        // distinct();
        //mapPartitions();
        //  repartition();
        //coalesce();
        aggregateByKey();
        //mapPartitionsWithIndex();
        //cogroup();
        // repartitionAndSortWithinPartitions();
        //  sample();
    }

    public static void map(){
        final List<String> list = Arrays.asList("张无忌", "赵敏", "周芷若");
        /*final Set<String> set = Sets.newLinkedHashSet();
        set.addAll(list);*/

        final JavaRDD<String> rdd = sc.parallelize(list);


        System.out.println("\nLambda表达式实现：");
        rdd.map(name -> "Hello " + name).foreach(s -> println(s));
    }


    public static void flatMap() {
        final List<String> list = Arrays.asList("张无忌 赵敏", "宋青书 周芷若");
        final JavaRDD<String> rdd = sc.parallelize(list);

        System.out.println("\nLambda表达式实现：");
        rdd.flatMap(names -> Arrays.asList(names.split(" ")).iterator())
                .map(name -> "Hello " + name)
                .foreach(name -> println(name));

    }

    /**
     * 从RDD过滤出来偶数
     */
    public static void filter(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final JavaRDD<Integer> rdd = sc.parallelize(list);

        System.out.println("\nLambda表达式实现：");
        rdd.filter(x -> x%2 == 0).foreach(x -> println(String.valueOf(x)));
    }

    /**RDD()
     * bykey
     */
    public static void groupBykey(){
        final List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<String, String>("峨眉", "周芷若"),
                new Tuple2<String, String>("武当", "宋青书"),
                new Tuple2<String, String>("峨眉", "灭绝师太"),
                new Tuple2<String, String>("武当", "张三丰")
        );

        final JavaPairRDD<String, String> rdd = (JavaPairRDD<String, String>)sc.parallelizePairs(list);


        final JavaPairRDD<String, Iterable<String>> groupBykeyRDD = rdd.groupByKey();

        System.out.println("\nLambda表达式实现1：");
        groupBykeyRDD.foreach(names -> {
            println(names._1);
            Iterator<String> iterator = names._2.iterator();
            while (iterator.hasNext()) {
                System.out.print(iterator.next() + " ");
            }
            System.out.println();
        });

        System.out.println("\nLambda表达式实现1：");
        groupBykeyRDD.foreach(names -> {
            println(names._1);
            names._2.forEach(name -> System.out.print(name + " "));
            System.out.println();
        });

    }

    /**
     * 一线城市： 8 年  -> 100万
     *   5:  50以上IT
     */
    public static void reduceBykey(){
        final List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("峨眉", 40),
                new Tuple2<String, Integer>("武当", 30),
                new Tuple2<String, Integer>("峨眉",60),
                new Tuple2<String, Integer>("武当",99)
        );
        //reduceBykey
        final JavaPairRDD<String, Integer> rdd = (JavaPairRDD<String, Integer>)sc.parallelizePairs(list);


        System.out.println("\nLambda表达式实现：");
        rdd.reduceByKey((x, y) -> x + y).foreach(tuple -> println(tuple._1 + " " + tuple._2));

    }


    public static void sortBykey(){
        final List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(98,"东方不败"),
                new Tuple2<Integer, String>(80,"岳不群"),
                new Tuple2<Integer, String>(85,"令狐冲"),
                new Tuple2<Integer, String>(83,"任我行")
        );
        final JavaPairRDD<Integer, String> rdd = (JavaPairRDD<Integer, String>)sc.parallelizePairs(list);

        System.out.println("\nLambda表达式实现：");
        rdd.sortByKey(false).foreach(tuple -> println(tuple._1 + " -> " + tuple._2));

    }


    public static void join(){
        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );

        final JavaPairRDD<Integer, String> nemesrdd = (JavaPairRDD<Integer, String>)sc.parallelizePairs(names);
        final JavaPairRDD<Integer, Integer> scoresrdd = (JavaPairRDD<Integer, Integer>)sc.parallelizePairs(scores);
        /**
         * <Integer, 学号
         * Tuple2<String, 名字
         * Integer>> 分数
         */


        System.out.println("\nLambda表达式实现：");
        sc.parallelizePairs(names).join(sc.parallelizePairs(scores)).sortByKey()
                .foreach(tuple -> println("学号：" + tuple._1 + " 名字：" + tuple._2._1 + " 分数：" + tuple._2._2));

    }

    public static void union(){
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);

        System.out.println("\nLambda表达式实现：");
        rdd1.union(rdd2).foreach(value -> System.out.print(value + " "));
    }

    /**
     * 交集
     */
    public static void intersection(){
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);

        System.out.println("\nLambda表达式实现：");
        rdd1.intersection(rdd2).foreach(number -> System.out.print(number + " "));

    }

    public static void distinct() {
        final List<Integer> list1 = Arrays.asList(1, 2, 3,3,4,4);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);

        System.out.println("\nLambda表达式实现：");
        rdd1.distinct().sortBy(value-> value, true, 1).foreach(value -> println(value + " "));
    }

    /**
     * 笛卡尔积
     * A={a,b}
     * B={0,1,2}
     * A B 笛卡尔积
     * a0,a1,a2
     * b0,b1,b2
     */
    public static void cartesian(){

        final List<String> A = Arrays.asList("a", "b");
        final List<Integer> B = Arrays.asList(0, 1, 2);

        final JavaRDD<String> rddA = sc.parallelize(A);
        final JavaRDD<Integer> rddB = sc.parallelize(B);

        System.out.println("\nLambda表达式实现：");
        rddA.cartesian(rddB).foreach(tuple -> println(tuple._1 + " -> " + tuple._2));
    }

    /**
     * map:
     *    一条数据一条数据的处理（文件系统，数据库等等）
     * mapPartitions：
     *    一次获取的是一个分区的数据（hdfs）
     *    正常情况下，mapPartitions 是一个高性能的算子
     *    因为每次处理的是一个分区的数据，减少了去获取数据的次数。
     *
     *    但是如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
     */
    public static void  mapPartitions(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
        final JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        System.out.println("\nLambda表达式实现：");
        rdd.mapPartitions(integerIterator -> {
            Iterable<Integer> integers = () -> integerIterator;
            return StreamSupport.stream(integers.spliterator(),true)
                    .map(value -> "hello-" + value).iterator();
        }).foreach(value -> println(value));

    }

    /**
     * 进行重分区
     * HDFS -》 hello.txt   2个文件块（不包含副本）
     * 2个文件块 -》2个分区  -》当spark任务运行，一个分区就启动一个task任务。
     *
     * 解决的问题：本来分区数少  -》 增加分区数
     */
    public static void repartition(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> rdd = (JavaRDD<Integer>) sc.parallelize(list, 1);
        // coalesce(numPartitions, shuffle = true)

        System.out.println("\nLambda表达式实现：");
        rdd.repartition(2).foreach(number -> println(number + ""));

    }

    /**
     * 实现单词计数
     */
    public static void aggregateByKey(){


        final List<String> list = Arrays.asList("you,jump", "i,jump");
        final JavaRDD<String> rdd = sc.parallelize(list);

        System.out.println("\nLambda表达式实现：");
        rdd.flatMap(lines -> Arrays.asList(lines.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .aggregateByKey(0, (v1, v2) -> v1 + v2, (v1, v2) -> v1 + v2)
                .foreach(v -> println(v._1 + " -> " + v._2));

    }

    /**
     * 分区数由多  ->  变少
     */
    public static void coalesce(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> rdd = (JavaRDD<Integer>) sc.parallelize(list, 3);

        System.out.println("\nLambda表达式实现：");
        rdd.coalesce(1).foreach(v -> println(v + ""));

    }

    /**
     * map: 每次获取和处理的就是一条数据
     * mapParitions: 每次获取和处理的就是一个分区的数据
     *  mapPartitionsWithIndex:每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥？
     */
    public static void mapPartitionsWithIndex(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        final JavaRDD<Integer> rdd = sc.parallelize(list, 2);//HashParitioners Rangepartitionw 自定义分区

        System.out.println("\nLambda表达式实现：");
        rdd.mapPartitionsWithIndex((integer, iterator) -> {
            Iterable integers = () -> iterator;
            return StreamSupport.stream(integers.spliterator(), true)
                    .map(value -> integer + "_" + value).iterator();
        }, true)
                .foreach(value -> println(value + ""));

    }

    /**
     * When called on datasets of type (K, V) and (K, W),
     * returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.
     */
    public static void cogroup(){
        //sh s   sha  shan shang sa san sang
        final List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "林平之"),
                new Tuple2<Integer, String>(3, "岳不群"),
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "林平之"),
                new Tuple2<Integer, String>(3, "岳不群")
        );

        final List<Tuple2<Integer, Integer>> list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 90),
                new Tuple2<Integer, Integer>(2, 91),
                new Tuple2<Integer, Integer>(3, 89),
                new Tuple2<Integer, Integer>(1, 98),
                new Tuple2<Integer, Integer>(2, 78),
                new Tuple2<Integer, Integer>(3, 67)
        );

        final JavaPairRDD<Integer, String> rdd1 = (JavaPairRDD<Integer, String> )sc.parallelizePairs(list1);
        final JavaPairRDD<Integer, Integer> rdd2 = (JavaPairRDD<Integer, Integer> )sc.parallelizePairs(list2);

        final JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> rdd3 =
                (JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>) rdd1.cogroup(rdd2);
        rdd3.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple) throws Exception {
                final Integer id = tuple._1;
                final Iterable<String> names = tuple._2._1;
                final Iterable<Integer> scores = tuple._2._2;
                println("ID:"+id + " Name: "+names+ " Scores: "+ scores);
            }
        });

    }

    /**
     * 少  -> 多
     *
     */
    public static void repartitionAndSortWithinPartitions(){//调优
        final List<Integer> list = Arrays.asList(1, 2, 11, 3, 12, 4, 5);
        final JavaRDD<Integer> rdd = sc.parallelize(list, 1);
        final JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer number) throws Exception {
                return new Tuple2<>(number, number);
            }
        });
        //new HashPartitioner(2) new RangePartitioner<>()
        pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                final Integer number = Integer.valueOf(key.toString());
                if(number % 2 == 0){
                    return 0;
                }else{
                    return 1;
                }
            }
        }).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>,
                Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, Integer>> iterator) throws Exception {
                final ArrayList<String> list = new ArrayList<>();
                while(iterator.hasNext()){
                    list.add(index + "_"+ iterator.next());
                }
                return list.iterator();
            }
        },false)
                .foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        println(s);
                    }
                });


    }


    /**
     * 有放回
     * 无放回
     */
    public static void sample(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7,9,10);
        final JavaRDD<Integer> rdd = sc.parallelize(list);
        /**
         * withReplacement: Boolean,
         *       true: 有放回的抽样
         *       false: 无放回抽象
         * fraction: Double：
         *      RDD  里面的每个元素被抽到的概率有多大
         * seed: Long：
         *      随机种子
         *
         *
         */
        final JavaRDD<Integer> rdd2 = rdd.sample(false, 0.5);

        rdd2.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                println(integer + "");
            }
        });
    }

    public static  void pipe(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7,9,10);
        final JavaRDD<Integer> rdd = sc.parallelize(list);

        //   final JavaRDD<String> pipe = rdd.pipe("sh wordcouont.sh");

    }

    public static void println(String str){
        System.out.println(str);
    }

}