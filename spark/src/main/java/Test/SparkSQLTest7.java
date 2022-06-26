package Test;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-26 22:41
 */
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLTest7 {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLTest7")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark.sql("use test");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM emp");

        sqlDF.describe().show();

        spark.stop();

    }


}
