package Test;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-26 22:41
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLTest3 {
    public static void main(String[] args){
        //java版本
        SparkConf conf = new SparkConf();
        conf.setMaster("local");    //本地单线程运行
        conf.setAppName("testJob");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLTest3")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/test")
                .option("dbtable", "(SELECT * FROM city) city")
                .option("user", "root")
                .option("password", "root")
                .load();

        jdbcDF.printSchema();
        jdbcDF.show();

        spark.stop();
    }
}

