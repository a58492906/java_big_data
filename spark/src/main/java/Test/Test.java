/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-08 15:09
 */
package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test {
    /**
     * 使用Java语言开发sparkstreaming完成WordCount
     */
    public static void main(String[] args) throws InterruptedException {
        //0.TODO 准备环境
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkDemo").setMaster("local[*]");
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        jsc.setLogLevel("WARN");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        //1.TODO 加载数据
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("node1", 9999);
        //2.TODO 处理数据-WordCount
        JavaPairDStream<String, Integer> result = lines.flatMap(line -> Arrays.asList(line.split(",", -1)).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        //3.TODO 输出结果
        result.print();
        //4.TODO 启动并等待停止
        jssc.start();
        jssc.awaitTermination();

        //nc -lk 999
    }

}

