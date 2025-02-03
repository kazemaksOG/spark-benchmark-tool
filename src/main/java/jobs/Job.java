package jobs;

import org.apache.spark.sql.SparkSession;
import utils.MeasurementUnit;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Job implements Runnable{
    protected SparkSession spark;
    protected String inputPath;
    static protected AtomicInteger globaljobId = new AtomicInteger(0);
    protected MeasurementUnit measurementUnit;

    public Job(SparkSession spark, String inputPath) {
        this.spark = spark;
        this.inputPath = inputPath;
        this.measurementUnit = new MeasurementUnit();
    }

    public int getJobId() {
        return globaljobId.incrementAndGet();
    }

    public HashMap<String, Long> getResults() {
        return measurementUnit.getResults();
    }

    public void run() {
        System.out.println("jobs.Job not defined");
    };
}
