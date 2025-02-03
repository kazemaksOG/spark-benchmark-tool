package jobs.implementations;

import jobs.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.sum;

public class DefaultOperations extends Job {


    public DefaultOperations(SparkSession spark, String inputPath) {
        super(spark, inputPath);
    }


    @Override
    public void run() {
        measurementUnit.startMeasurement("Setup time");
        Dataset<Row> parquetDataset = spark.read().parquet(inputPath);

        measurementUnit.endMeasurement("Setup time");

        measurementUnit.startMeasurement("Execution time");
        Dataset<Row> mappedParquet = parquetDataset.select(mean("DISTANCE").alias("MEAN_DISTANCE"));
        double meanValue = mappedParquet.first().getDouble(0);

        measurementUnit.endMeasurement("Execution time");
    }
}
