package OraclePerformanceEstimator;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.metric.SQLMetricInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;

class StageListener extends SparkListener {
    private BlockingQueue<SparkListenerEvent> queue;
    public StageListener(BlockingQueue<SparkListenerEvent> queue) {
        this.queue = queue;
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        this.queue.add(stageCompleted);
        System.out.println("####################### Stage completed: " + stageCompleted.stageInfo().stageId());
        System.out.println(stageCompleted.stageInfo().name());
        System.out.println( stageCompleted.stageInfo().details());
        System.out.println("Parent IDs:"  + stageCompleted.stageInfo().parentIds());
    }



    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        this.queue.add(stageSubmitted);
        System.out.println("########################## Stage submitted: " + stageSubmitted.stageInfo().stageId());
        System.out.println(stageSubmitted.stageInfo().name());
        System.out.println( stageSubmitted.stageInfo().details());
        System.out.println("Parent IDs:"  + stageSubmitted.stageInfo().parentIds());
//        System.out.println("Properties:");
//        for(var prop : stageSubmitted.properties().entrySet()) {
//            System.out.println("prop: " + prop.getKey() + " = " + prop.getValue());
//        }
        String job = stageSubmitted.properties().getProperty("job.class");;
        System.out.println(job);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart eventsql) {
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Start");
            System.out.println("id:" + eventsql.executionId());
            System.out.println("root id:" + eventsql.rootExecutionId());

            System.out.println("details:" + eventsql.details());
            System.out.println(eventsql.time());
            System.out.println("desc:" + eventsql.description());
            System.out.println("modified conf:" + eventsql.modifiedConfigs().mkString());
            System.out.println(eventsql.physicalPlanDescription());
            printPlanTree(eventsql.sparkPlanInfo());

        } else if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate eventsql) {
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Update");
            System.out.println(eventsql.physicalPlanDescription());
            System.out.println("id:" + eventsql.executionId());
            printPlanTree(eventsql.sparkPlanInfo());

        } else if (event instanceof SparkListenerSQLExecutionEnd eventsql) {
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Completed");
            System.out.println("err message: " + eventsql.errorMessage());
        }
        // Ignore other cases
    }


    private void printStageInfoDetails(StageInfo stageInfo) {
        TaskMetrics taskMetrics = stageInfo.taskMetrics();
        if (taskMetrics != null) {
            System.out.println("task metric: " + taskMetrics.executorRunTime());
        } else {
            System.out.println("No task metrics");
        }
        Seq<RDDInfo> scalaSeq = stageInfo.rddInfos(); // Scala Seq<RDDInfo>
        List<RDDInfo> javaList = JavaConverters.seqAsJavaList(scalaSeq);
        for (RDDInfo info : javaList) {
            System.out.println(info.toString());
            System.out.println("Scope: " + info.scope().get().name());
        }
    }

    private void printPlanTree(SparkPlanInfo sparkPlanInfo) {
        Stack<SparkPlanInfo> stack = new Stack<>();
        stack.push(sparkPlanInfo);
        // convert plan tree into a doubly linked trees of stage profiles
        System.out.println("########################### SQL going down the tree");
        while(!stack.empty()) {
            SparkPlanInfo currentStage = stack.pop();

            System.out.println("sqlId: " + currentStage.nodeId());
            System.out.println("sqlName: " + currentStage.nodeName());
//            System.out.println("Simplified: " + currentStage.simpleString());
//            System.out.println("Metadata: " + currentStage.metadata().mkString());

            Seq<SQLMetricInfo> metScala = currentStage.metrics(); // Scala Seq<RDDInfo>
            List<SQLMetricInfo> listMetrics = JavaConverters.seqAsJavaList(metScala);
            System.out.println("metrics: ");
            for(var metric : listMetrics) {
                System.out.println("name: " + metric.name() + "type: " + metric.metricType() + "id: " + metric.accumulatorId());
            }

            // convert and iterate over all children
            Seq<SparkPlanInfo> scalaSeq = currentStage.children(); // Scala Seq<RDDInfo>
            List<SparkPlanInfo> javaList = JavaConverters.seqAsJavaList(scalaSeq);
            for(SparkPlanInfo children: javaList) {
                stack.push(children);
            }
        }
    }
}