package jobs.implementations.udf;


import jobs.Job;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.TreeMap;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class Loop20 extends Job {


    public Loop20(SparkSession spark, String inputPath, TreeMap<String, String> params) {
        super(spark, inputPath, params);
    }


    @Override
    public void run() {
        Dataset<Row> parquetDataset = defaultParquetSetup();

        measurementUnit.startMeasurement("execution_time");

        Dataset<Row> mappedParquet = parquetDataset.withColumn("DOLocationID", functions.callUDF("loop_20", col("DOLocationID")));
        mappedParquet = mappedParquet.agg(sum("DOLocationID")).alias("sum");

        Row[] collected = (Row[]) mappedParquet.take(10);

        measurementUnit.endMeasurement("execution_time");
    }
}
