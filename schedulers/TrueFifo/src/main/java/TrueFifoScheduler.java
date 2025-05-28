import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TrueFifoScheduler implements SchedulableBuilder {

    Pool rootPool;
    SparkContext sc;
    PerformanceEstimatorInterface performanceEstimator;

    TrueFifoScheduler(Pool rootPool, SparkContext sc) {
        this.rootPool = rootPool;
        this.sc = sc;

        performanceEstimator = sc.getPerformanceEstimator().getOrElse(() -> {
            throw new RuntimeException("Performance estimator not available");
        });

    }

    @Override
    public Pool rootPool() {
        return rootPool;
    }

    @Override
    public void buildPools() {

    }

    private void setPriority(Schedulable schedulable, Properties properties) {
        // TaskSetManager represents stages, other schedulables are ignored
        if(!(schedulable instanceof TaskSetManager stage)) {
            return;
        }

        // get job runtime and the job id the stage belongs to, so virtual deadline can be correctly set
        int stageId = stage.stageId();
        JobRuntime jobRuntime = performanceEstimator.getJobRuntime(stageId);
        stage.priority_$eq((int)jobRuntime.id());
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }
}
