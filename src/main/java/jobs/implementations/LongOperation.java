package jobs.implementations;

import config.Workload;
import jobs.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class LongOperation extends Job {


    public LongOperation(SparkSession spark, String inputPath, Workload.Partitioning partitioning) {
        super(spark, inputPath, partitioning);
    }


    @Override
    public void run() {
        Dataset<Row> parquetDataset = defaultParquetSetup();

        measurementUnit.startMeasurement("execution_time");
        Dataset<Row> mappedParquet = parquetDataset.alias("taxiA").join(parquetDataset.alias("taxiB"), col("taxiA.pickup_datetime").equalTo(col("taxiB.dropoff_datetime")));

        Row[] collected = (Row[]) mappedParquet.take(10);

        measurementUnit.endMeasurement("execution_time");
    }
}

