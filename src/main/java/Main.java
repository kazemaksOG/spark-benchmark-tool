
import config.Config;
import jobs.UdfContainer;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.*;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Map;
import utils.PoissonWait;
import static org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {


    public static void main(String[] args) throws InterruptedException {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        Config config = null;
        try {
            config = Config.parseBase("configs/base_config.json");
        } catch (IOException e) {
            System.out.println("Couldn't read the file " +e.getMessage());
        }

        System.out.println(config.toString());
        SparkSession spark = SparkSession.builder().appName("Bench Runner")
                .master(config.getMaster())
                .config(config.getSparkConfig())
                .getOrCreate();

        // register UDFS
        UdfContainer.registerUdfs(spark);

        Dataset<Row> parquet = spark.read().parquet("resources/tripdata-partitionBy-PULocationID.parquet");
        parquet.printSchema();


        Dataset<Row> mappedParquet = parquet.withColumn("DOLocationID", functions.callUDF("loop1000", col("DOLocationID")));
        mappedParquet = mappedParquet.agg(sum("DOLocationID")).alias("sum");

        Row[] collected = (Row[]) mappedParquet.take(10);
        for(Row row : collected) {
            System.out.println(row.toString());
        }

//        parquet.write().partitionBy("PULocationID").parquet("resources/tripdata-partitionBy-PULocationID");



//        Dataset<Row> mappedParquet = parquet.groupBy("hvfhs_license_num").agg(sum("tips")).alias("sum");
//
//        mappedParquet.explain();
//        Row[] collected = (Row[]) mappedParquet.take(10);

        System.out.println("END\n\n\n");
//        Thread.sleep(1000000);
    }
}