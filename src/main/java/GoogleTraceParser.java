import config.Config;
import jobs.UdfContainer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class GoogleTraceParser {


    public static void main(String[] args) throws InterruptedException {

        Config config = null;
        try {
            config = Config.parseBase("configs/base_config.json");
        } catch (IOException e) {
            System.err.println("Couldn't read the file " +e.getMessage());
            return;
        }

        System.out.println(config.toString());
        SparkSession spark = SparkSession.builder().appName("Bench Runner")
                .master(config.getMaster())
                .config(config.getSparkConfig())
                .getOrCreate();

        // Google_parquets askalon-new_ee43_parquet askalon_ee2_parquet
        String benchmark = "Google_parquets";
        Dataset<Row> googleWorkflows = spark.read().parquet("resources/" + benchmark + "/workflows/schema-1.0");
        Dataset<Row> googleTasks = spark.read().parquet("resources/" + benchmark + "/tasks/schema-1.0");


        googleWorkflows.printSchema();
        googleTasks.printSchema();

        long day = 17;
        long second = 5000;
        long period_s = 500;
        long startTime = (day * 24 * 60 * 60 * 1000L) + second * 1000L;
        long endTime = startTime + period_s * 1000L;


        Dataset<Row> combined = googleWorkflows.alias("workflows")
                .where(col("workflows.ts_submit").geq(startTime))
                .where(col("workflows.ts_submit").leq(endTime))
                .join(googleTasks.alias("tasks"), col("workflows.id").equalTo(col("tasks.workflow_id")))
//                .where(col("tasks.resource_type").equalTo("core"))
                .where(col("tasks.resource_amount_requested").gt(0))
                .withColumn("ts_submit_seconds", col("workflows.ts_submit").divide(1000))
                .withColumn("resource_run_time", col("tasks.runtime").multiply(col("tasks.resource_amount_requested")))
                .select(
                        col("workflows.id").alias("workflow_id"),
                        col("workflows.ts_submit"),
                        col("ts_submit_seconds"),
                        col("workflows.task_count"),
                        col("workflows.total_resources"),

                        col("tasks.id").alias("task_id"),
                        col("tasks.runtime"),
                        col("tasks.resource_amount_requested"),
                        col("resource_run_time"),
                        col("tasks.user_id")
                )
                .sort(col("resource_run_time").desc());

        combined.write().option("header", "true").csv("./results/macro_benchmark.csv");

        long uniqueUsers = combined.select("tasks.user_id").distinct().count();
        long tasks = combined.count();
        Dataset<Row> tasksPerUser = combined.groupBy(col("tasks.user_id")).count();
        Row[] collectedUsers = (Row[]) tasksPerUser.collect();
        Row[] collected = (Row[]) combined.take(200);


        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total amount of tasks: " + tasks);
        System.out.println("Tasks per user: ");
        for(Row row : collectedUsers) {
            System.out.println(row.toString());
        }

        System.out.println("Raw rows");

        for(Row row : collected) {
            System.out.println(row.toString());
        }


        System.out.println("END\n\n\n");

    }
}
