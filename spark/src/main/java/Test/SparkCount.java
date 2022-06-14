/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-08 15:09
 */
package Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.*;
import org.apache.commons.lang.StringUtils;

public class SparkCount {
    //创建sparkconf 和JavaSparkContext
    public static  SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
    public static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {

        aggregateByKey();

    }

    /**
     * 实现单词计数
     */
    public static void aggregateByKey(){
        String input = "/Users/xuejiameng/java_big_data/spark/data/";
        JavaRDD<String> unionrdd = sc.emptyRDD();
        JavaPairRDD<String, String> wordFileNameRDD = null;
        int num =0;
            while (num<=2){
                String file_name = input+num; //文件名称
                JavaRDD<String> rdd1 = sc.textFile(file_name);
                int fileName = num;
                //4.将遍历的多个rdd拼接成1个Rdd
                unionrdd=unionrdd.union(rdd1);;
               wordFileNameRDD=
                        unionrdd.flatMap(lines -> Arrays.asList(lines.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word,String.valueOf(fileName)));
                num++;
            }

        JavaPairRDD<Tuple2<String, String>, Integer> wordFileNameCountPerPairs = wordFileNameRDD.mapToPair(wordFileNamePair -> new Tuple2<>(wordFileNamePair, 1))
                .reduceByKey(Integer::sum);
        JavaPairRDD<String, Tuple2<String, Integer>> wordCountPerFileNamePairs = wordFileNameCountPerPairs.mapToPair(wordFileNameCountPerPair -> new Tuple2<>(wordFileNameCountPerPair._1._1, new Tuple2<>(wordFileNameCountPerPair._1._2, wordFileNameCountPerPair._2)));
        JavaPairRDD<String, String> result = wordCountPerFileNamePairs.groupByKey().mapToPair(wordCountPerFileNamePairIterator -> new Tuple2<>(wordCountPerFileNamePairIterator._1, StringUtils.join(wordCountPerFileNamePairIterator._2.iterator(), ','))).sortByKey();


        for(Tuple2<String, String> pair : result.collect()) {
            System.out.printf("\"%s\", {%s}%n", pair._1, pair._2);
        }


    }



}



