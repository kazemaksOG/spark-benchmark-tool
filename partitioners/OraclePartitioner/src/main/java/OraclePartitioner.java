import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.PerformanceEstimatorInterface;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.CustomPartitioner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


// We want each task to run in about 0.5s
// DEFAULTS:
// loop1000 111s -> 111s / 0.5 -> 222
// loop100 15s-30s -> 20s / 0.5 -> 40
// loop20: 1s-16s -> 8s / 0.5 ->  16



public class OraclePartitioner implements CustomPartitioner{
    HashMap<String, Long> jobClassToTaskAmount;

    public OraclePartitioner() {
        jobClassToTaskAmount = new HashMap<>();

        jobClassToTaskAmount.put("jobs.implementations.udf.Loop1000", 222L);

        jobClassToTaskAmount.put("jobs.implementations.udf.Loop100", 40L);

        jobClassToTaskAmount.put("jobs.implementations.udf.Loop20", 16L);
    }


    @Override
    public long getMaxSplitBytes(SparkSession session, long openCostInBytes, int minPartitionNum, long totalBytes, int sqlId) {
        System.out.println("#### in getMaxSplitBytes");
        String jobClass = session.sparkContext().localProperties().get().getProperty("job.class");
        System.out.println("###### JOBCLASS: " + jobClass);
        long taskAmount = jobClassToTaskAmount.getOrDefault(jobClass, 32L);
        long splitPartitionNum = Math.max(minPartitionNum, taskAmount);
        long splitBytes = totalBytes / splitPartitionNum;
        System.out.println("minPartitionNum" + minPartitionNum);
        System.out.println("taskAmount  = " + taskAmount);
        System.out.println("splitPartitionNum = " + splitPartitionNum);
        System.out.println("splitBytes = " + splitBytes);

        return Math.max(openCostInBytes, splitBytes);
    }

    @Override
    public int getMinNumPartitions(SparkSession session, SparkPlan plan, long totalSize) {
        System.out.println("#### in getMinNumPartitions");
        String jobClass = session.sparkContext().localProperties().get().getProperty("job.class");
        System.out.println("###### JOBCLASS: " + jobClass);
        long taskAmount = jobClassToTaskAmount.getOrDefault(jobClass, 32L);
        System.out.println("taskAmount = " + taskAmount);
        return (int)taskAmount;
    }
}
