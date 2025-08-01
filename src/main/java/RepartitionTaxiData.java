import config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class RepartitionTaxiData {
        public static void main(String[] args) throws InterruptedException {
            System.out.printf("Repartitioning taxi dataset\n");

            SparkSession spark = SparkSession.builder().appName("Bench Runner")
                    .getOrCreate();

            System.out.println("###############Listener added");
            spark.sparkContext().setLocalProperty("job.class", "jobs.implementations.SuperShortOperation");

            Dataset<Row> parquet = spark.read().parquet("resources/taxi-data.parquet");
            parquet.printSchema();

            parquet.write().partitionBy("PULocationID").parquet("resources/tripdata-partitionBy-PULocationID.parquet");

        }

}
