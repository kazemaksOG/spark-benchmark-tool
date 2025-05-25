package jobs.implementations.udf;


import jobs.Job;
import jobs.UdfContainer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.TreeMap;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class LoopCustom extends Job {


    public LoopCustom(SparkSession spark, String inputPath, TreeMap<String, String> params) {
        super(spark, inputPath, params);
    }


    @Override
    public void run() {
        Dataset<Row> parquetDataset = defaultParquetSetup();

        measurementUnit.startMeasurement("execution_time");
        // Get task runtime
        double task_runtime_s = Double.parseDouble(params.get("task_runtime_s"));
        // Generate UDF of that runtime
        UDF1<Integer, Integer> loopCustom = UdfContainer.generateLoopCustom(task_runtime_s);
        String name = task_runtime_s + "_" + UUID.randomUUID().toString().replace("-", "");
        this.spark.udf().register(name, loopCustom, DataTypes.IntegerType);

        // Run the task
        Dataset<Row> mappedParquet = parquetDataset.withColumn("DOLocationID", functions.callUDF(name, col("DOLocationID")));
        mappedParquet = mappedParquet.agg(sum("DOLocationID")).alias("sum");

        Row[] collected = (Row[]) mappedParquet.take(10);

        measurementUnit.endMeasurement("execution_time");
    }
}
