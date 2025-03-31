
import config.Config;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.*;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Map;
import utils.PoissonWait;
import static org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    static class StageLis extends SparkListener {
        public ArrayList<Integer> list = new ArrayList<>();
        @Override
        public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
            System.out.println("Stage completed: " + stageCompleted.stageInfo().stageId());
            list.add(stageCompleted.stageInfo().stageId());
        }
        @Override
        public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
            System.out.println("########################## Stage submitted: " + stageSubmitted.stageInfo().stageId());
            System.out.println(stageSubmitted.stageInfo().name());
            System.out.println(stageSubmitted.stageInfo().numTasks());
            System.out.println( stageSubmitted.stageInfo().details());
            System.out.println( stageSubmitted.stageInfo().parentIds());
            String job = stageSubmitted.properties().getProperty("job.class");;
            System.out.println(job);
            TaskMetrics taskMetrics = stageSubmitted.stageInfo().taskMetrics();
            if (taskMetrics != null) {
                System.out.println(taskMetrics.executorRunTime());
            } else {
                System.out.println("No task metrics");
            }
            Seq<RDDInfo> scalaSeq = stageSubmitted.stageInfo().rddInfos(); // Scala Seq<RDDInfo>
            List<RDDInfo> javaList = JavaConverters.seqAsJavaList(scalaSeq);
            for (RDDInfo info : javaList) {
                System.out.println(info.toString());
                System.out.println("Scope: " + info.scope().get().name());

            }
        }

        @Override
        public void onOtherEvent(SparkListenerEvent event) {
            if (event instanceof SparkListenerSQLExecutionStart eventsql) {
                System.out.println("########################### SQL Execution Start");
                System.out.println(eventsql.details());
                System.out.println(eventsql.description());
                System.out.println(eventsql.physicalPlanDescription());
                System.out.println(eventsql.time());
            } else if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate eventsql) {
                System.out.println("########################### SQL Execution Update");
                System.out.println(eventsql.physicalPlanDescription());
            } else if (event instanceof SparkListenerSQLExecutionEnd eventsql) {
                System.out.println("########################### SQL Execution Update");
                System.out.println(eventsql.time());
            }
            // Ignore other cases
        }
    }


    public static void main(String[] args) throws InterruptedException {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        Config config = null;
        try {
            config = Config.parseBase("configs/base_config.json");
        } catch (IOException e) {
            System.out.println("Couldn't read the file " +e.getMessage());
        }

        System.out.println(config.toString());
        SparkSession spark = SparkSession.builder().appName("Bench Runner")
                .master(config.getMaster())
                .config(config.getSparkConfig())
                .getOrCreate();
        StageListener s = new StageListener();

        spark.sparkContext().addSparkListener(s);

//        StageLis st = new StageLis();
//        spark.sparkContext().addSparkListener(st);

        System.out.println("###############Listener added");
        spark.sparkContext().setLocalProperty("job.class", "jobs.implementations.SuperShortOperation");

        Dataset<Row> parquet = spark.read().parquet("resources/fhvhv_tripdata_2024-08.parquet");
        parquet.printSchema();
        Dataset<Row> mappedParquet = parquet.groupBy("hvfhs_license_num").agg(sum("tips")).alias("sum");

        mappedParquet.explain();
        Row[] collected = (Row[]) mappedParquet.take(10);

        System.out.println("END\n\n\n");
        Thread.sleep(1000000);
    }
}