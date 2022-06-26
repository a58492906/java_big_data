package Test;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-26 22:41
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;


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

        spark.conf().set("spark.sql.planChangeLog.level","WARN");

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/test")
                .option("dbtable", "city")
                .option("user", "root")
                .option("password", "root")
                .load();
        jdbcDF.show();


            jdbcDF.registerTempTable("city");
        jdbcDF.registerTempTable("city2");
          //  spark.sql("select NAME,CODE from (select NAME,CODE from city where 1 = 1 and CODE>=101010100) tmp where tmp.CODE<=20000000 ").explain(true);

        spark.sql("SELECT DISTINCT a.NAME FROM (" +
                "SELECT*FROM city WHERE CODE> 101010100 AND 1=1) a WHERE a.id=2 except " +
                "SELECT NAME FROM city2 WHERE CODE> 101280101 ").explain(true);

        jdbcDF.printSchema();

        spark.stop();
    }
}

