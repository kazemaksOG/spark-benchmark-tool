import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.concurrent.BlockingQueue;

class StageListener extends SparkListener {
    private BlockingQueue<SparkListenerEvent> queue;
    public StageListener(BlockingQueue<SparkListenerEvent> queue) {
        this.queue = queue;
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        this.queue.add(stageCompleted);
        System.out.println("Stage completed: " + stageCompleted.stageInfo().stageId());
    }
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        this.queue.add(stageSubmitted);
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
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Start");
            System.out.println(eventsql.details());
            System.out.println(eventsql.description());
            System.out.println(eventsql.physicalPlanDescription());
            System.out.println(eventsql.time());
        } else if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate eventsql) {
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Update");
            System.out.println(eventsql.physicalPlanDescription());
        } else if (event instanceof SparkListenerSQLExecutionEnd eventsql) {
            this.queue.add(eventsql);
            System.out.println("########################### SQL Execution Update");
            System.out.println(eventsql.time());
        }
        // Ignore other cases
    }
}