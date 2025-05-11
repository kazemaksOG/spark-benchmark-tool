import org.apache.spark.scheduler.PerformanceEstimatorInterface;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.CustomPartitioner;
import org.apache.spark.sql.execution.datasources.PartitionDirectory;

import scala.collection.Seq;

public class RuntimePartitioner implements CustomPartitioner {
    private static double MAX_TASK_RUNTIME = 500.0;
    @Override
    public long getMaxSplitBytes(SparkSession session, long openCostInBytes, int minPartitionNum, long totalBytes, int sqlId) {
        System.out.println("#### in getMaxSplitBytes");
        String jobClass = session.sparkContext().localProperties().get().getProperty("job.class");
        System.out.println("###### JOBCLASS: " + jobClass);
        PerformanceEstimatorInterface performanceEstimator = session.sparkContext().getPerformanceEstimator().getOrElse(null);
        long runtimeEstimate = performanceEstimator.getSqlRuntime(sqlId, totalBytes);
        long minRuntimePartitionNum = (long)(runtimeEstimate / MAX_TASK_RUNTIME);
        // for now, we ignore minPartitionNum supplied by Spark
        long splitPartitionNum = minRuntimePartitionNum;
//        long splitPartitionNum = Math.max(minPartitionNum, minRuntimePartitionNum);
        long splitBytes = totalBytes / splitPartitionNum;
        System.out.println("runtimeEstimate = " + runtimeEstimate);
        System.out.println("minPartitionNum" + minPartitionNum);
        System.out.println("minRuntimePartitionNum = " + minRuntimePartitionNum);
        System.out.println("splitPartitionNum = " + splitPartitionNum);
        System.out.println("splitBytes = " + splitBytes);

        return Math.max(openCostInBytes, splitBytes);
    }

    @Override
    public int getMinNumPartitions(SparkSession session, SparkPlan plan, long totalSize) {
        System.out.println("#### in getMinNumPartitions");
        String jobClass = session.sparkContext().localProperties().get().getProperty("job.class");
        System.out.println("###### JOBCLASS: " + jobClass);
        SparkPlanInfo info = SparkPlanInfo.fromSparkPlan(plan);
        PerformanceEstimatorInterface performanceEstimator = session.sparkContext().getPerformanceEstimator().getOrElse(null);
        long runtimeEstimate = performanceEstimator.getSqlRuntime(info.nodeId(), totalSize);
        int minRuntimePartitionNum = (int)(runtimeEstimate / MAX_TASK_RUNTIME);
        System.out.println("runtimeEstimate = " + runtimeEstimate);
        System.out.println("minPartitionNum = " + minRuntimePartitionNum);
        return minRuntimePartitionNum;
    }
}
