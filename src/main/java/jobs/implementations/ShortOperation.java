package jobs.implementations;

import jobs.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class ShortOperation extends Job {


    public ShortOperation(SparkSession spark, String inputPath) {
        super(spark, inputPath);
    }


    @Override
    public void run() {
        measurementUnit.startMeasurement("setup_time");
        Dataset<Row> parquetDataset = spark.read().parquet(inputPath);

        measurementUnit.endMeasurement("setup_time");

        measurementUnit.startMeasurement("execution_time");
        Dataset<Row> mappedParquet = parquetDataset.groupBy("hvfhs_license_num").agg(sum(sqrt(col("tips").plus(col("driver_pay"))))).alias("sum");

        Row[] collected = (Row[]) mappedParquet.take(10);

        measurementUnit.endMeasurement("execution_time");
    }
}

