import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterFairScheduler implements SchedulableBuilder {

    class Stage implements Comparable<Stage> {
        long stageId;
        long stageRuntime;

        long startVirtualTime;

        long virtualDeadline;

        Stage(TaskSetManager stage, long virtualStartTime, long runtime) {
            this.stageId = stage.stageId();

            this.startVirtualTime = virtualStartTime;
            this.stageRuntime = runtime;
            // set deadlines
            this.virtualDeadline = this.startVirtualTime + this.stageRuntime;
            stage.deadline_$eq(this.virtualDeadline);

            System.out.println("##### INFO: stageId: " + stageId + " startVirtualTime: " + startVirtualTime + " runtime: " + runtime + " virtualDeadline: " + virtualDeadline);
        }



        @Override
        public int compareTo(@NotNull Stage otherStage) {
            // Jobs should be sorted based on virtual deadline, indicating when they would end in a fair scheduler
            int priority = Long.compare(this.virtualDeadline, otherStage.virtualDeadline);
            // Since TreeSet uses comparator for also checking if elements are equal, we dont want to overwrite elements
            // with the same virtual deadlines
            if(priority == 0) {
                return Long.compare(this.stageId, otherStage.stageId);
            }
            return priority;
        }

    }



    Pool rootPool;
    SparkContext sc;
    int totalCores;

    // Virtual time related variables
    long virtualTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis();
    long previousCurrentTime = startTime;
    PerformanceEstimatorInterface performanceEstimator;

    TreeSet<Stage> activeStages = new TreeSet<>();

    ClusterFairScheduler(Pool rootPool, SparkContext sc) {
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

    private double convertReadableTime(long time) {
        return (time - startTime) / 1000.0;
    }

    private void setPriority(Schedulable schedulable, Properties properties) {
        // TaskSetManager represents stages, other schedulables are ignored
        if(!(schedulable instanceof TaskSetManager stage)) {
            return;
        }

        this.totalCores = this.sc.defaultParallelism();


        long currentTime = System.currentTimeMillis();

        System.out.println("####### Current time: " + convertReadableTime(currentTime));
        System.out.println("####### Current virtualTime: " + convertReadableTime(virtualTime));
        System.out.println("####### Current previousCurrentTime: " + convertReadableTime(previousCurrentTime));


        // advance virtual time if any stage has finished
        double stageShare = !activeStages.isEmpty() ? ((double) this.totalCores) / (activeStages.size()) : 0 ;
        Iterator<Stage> stageIterator = activeStages.iterator();
        while (stageIterator.hasNext()) {
            Stage activeStage = stageIterator.next();
            System.out.println("######## Job deadline: " + convertReadableTime(activeStage.virtualDeadline));

            long virtualTimeSpent = activeStage.virtualDeadline - virtualTime;
            long realTimeSpent = (long)(virtualTimeSpent / stageShare);
            long stageRealFinishTime = previousCurrentTime + realTimeSpent;

            // check if earliest job has finished
            if(stageRealFinishTime > currentTime) {
                break;
            }

            // remove job from active jobs
            stageIterator.remove();

            // calculate how much virtual time has advanced
            System.out.println("######## Share: " + stageShare);
            System.out.println("####### Virtual time spent" + (virtualTimeSpent));

            virtualTime += virtualTimeSpent;
            previousCurrentTime = stageRealFinishTime;
            stageShare = !activeStages.isEmpty() ? ((double) this.totalCores) / (activeStages.size()) : 0 ;

            System.out.println("######## Updating virtual time:");
            System.out.println("VirtualTime: " + convertReadableTime(virtualTime) );
            System.out.println("previousCurrentTime: " + convertReadableTime(previousCurrentTime));
        }


        // ######## Modify current stage for submission #########

        int stageId = stage.stageId();
        long stageRuntime = performanceEstimator.getStageRuntime(stageId);

        Stage currentStage = new Stage(stage, virtualTime, stageRuntime);

        activeStages.add(currentStage);
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }


}
