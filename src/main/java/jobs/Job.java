package jobs;

import config.Workload;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.MeasurementUnit;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Job implements Runnable{
    protected SparkSession spark;
    protected String inputPath;
    protected Workload.Partitioning partitioning;
    protected MeasurementUnit measurementUnit;

    public Job(SparkSession spark, String inputPath, Workload.Partitioning partitioning) {
        this.spark = spark;
        this.inputPath = inputPath;
        this.partitioning = partitioning;
        this.measurementUnit = new MeasurementUnit();
    }


    public Dataset<Row> defaultParquetSetup() {
        measurementUnit.startMeasurement("setup_time");
        Dataset<Row> parquetDataset = spark.read().parquet(inputPath);

        measurementUnit.endMeasurement("setup_time");

        measurementUnit.startMeasurement("partitioning_time");
        switch(partitioning) {
            case COALESCE -> {
                int coresPerExecutor = Integer.parseInt(spark.conf().get("spark.executor.cores", "1"));
                if (coresPerExecutor < 1) {
                    throw new IllegalArgumentException("The number of Cores per executor must be greater than 0");
                }
                parquetDataset = parquetDataset.coalesce(coresPerExecutor);
            }

            case REPARTITION -> {
                int coresPerExecutor = Integer.parseInt(spark.conf().get("spark.executor.cores", "1"));
                if (coresPerExecutor < 1) {
                    throw new IllegalArgumentException("The number of Cores per executor must be greater than 0");
                }
                parquetDataset = parquetDataset.repartition(coresPerExecutor);
            }
            default -> {

            }
        }
        measurementUnit.endMeasurement("partitioning_time");

        return parquetDataset;
    }

    public HashMap<String, Long> getResults() {
        return measurementUnit.getResults();
    }

    public void run() {
        System.out.println("jobs.Job not defined");
    };
}
