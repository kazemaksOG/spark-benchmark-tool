package OraclePerformanceEstimator;

import org.apache.spark.scheduler.*;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class OraclePerformanceEstimator implements PerformanceEstimatorInterface {

    private final JobProfileContainer jobProfileContainer;
    private final StageListener listener;
    private BlockingQueue<SparkListenerEvent> queue;
    private Thread estimationThread;

    public OraclePerformanceEstimator() {
        this.queue = new LinkedBlockingQueue<>();
        listener = new StageListener(queue);
        jobProfileContainer = new JobProfileContainer();
    }

    @Override
    public long getStageRuntime(int stageId) {
        return jobProfileContainer.getStageRuntime(stageId);
    }


    @Override
    public JobRuntime getJobRuntime(int stageId) {
        return jobProfileContainer.getJobRuntime(stageId);
    }

    @Override
    public long getSqlRuntime(int sqlId, long totalSize) {
        return jobProfileContainer.getSqlRuntime(sqlId, totalSize);
    }

    @Override
    public void registerListener(LiveListenerBus listenerBus) {
        listenerBus.addToSharedQueue(listener);
    }

    @Override
    public void startEstimationThread() {
        estimationThread = new Thread(() -> {
            try {
                // Listen for new spark events until application shuts down
                for(;;) {
                    SparkListenerEvent event = queue.take();
                    CompletableFuture.runAsync(() -> handleListenerEvent(event))
                            .exceptionally(ex -> {
                                System.out.println("##### ERROR: crashed async event listener" + ex.getMessage());
                                ex.printStackTrace();
                                return null;
                            });
                }
            } catch (InterruptedException e) {
                System.out.println("####### WARNING: Shutting down performance thread");
            }
        });
        // Needed to not halt Spark shutdown
        estimationThread.setDaemon(true);
        estimationThread.start();
    }

    private void handleListenerEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerStageSubmitted stageEvent) {
            jobProfileContainer.handleSparkListenerStageSubmitted(stageEvent);
        } else if (event instanceof SparkListenerStageCompleted stageEvent) {
            jobProfileContainer.handleSparkListenerStageCompleted(stageEvent);
        } else if (event instanceof SparkListenerSQLExecutionStart sqlEvent) {
            jobProfileContainer.handleSparkListenerSQLExecutionStart(sqlEvent);
        } else if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate sqlEvent) {
            jobProfileContainer.handleSparkListenerSQLAdaptiveExecutionUpdate(sqlEvent);
        } else if (event instanceof SparkListenerSQLExecutionEnd sqlEvent) {
            jobProfileContainer.handleSparkListenerSQLExecutionEnd(sqlEvent);
        }
    }

    @Override
    public void shutdown() {
        estimationThread.interrupt();
    }


}
