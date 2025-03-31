import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterFairScheduler implements SchedulableBuilder {



    Pool rootPool;
    SparkContext sc;
    int totalCores;
    StageListener listener;

    // Virtual time related variables
    long virtualTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis();
    long lastStageSubmissionTime = virtualTime;
    ConcurrentHashMap<Integer, TaskSetManager> activeStages = new ConcurrentHashMap<>();

    ClusterFairScheduler(Pool rootPool, SparkContext sc) {
        this.rootPool = rootPool;
        this.sc = sc;

    }

    @Override
    public Pool rootPool() {
        return rootPool;
    }

    @Override
    public void buildPools() {

    }

    private void initialize() {
        // Find and initialize the listener
        for( SparkListenerInterface l : sc.listenerBus().listeners()) {
            if (l instanceof StageListener stageListener) {
                listener = stageListener;
            }
        }
        this.totalCores = sc.defaultParallelism();
    }

    private double convertReadableTime(long time) {
        return (time - startTime) / 1000.0;
    }

    private void setPriority(Schedulable schedulable, Properties properties) {
        // Initialize if this is the first arriving schedulable
        if (listener == null) {
            initialize();
        }

        // TaskSetManager represents stages, other schedulables are ignored
        if(!(schedulable instanceof TaskSetManager taskSetManager)) {
            return;
        }

        // ######## Update virtual time #########

        // see if any stages have finished
        List<Tuple2<Integer, Long>> endTimes = listener.getStageEndTimes();

        long currentTime = System.currentTimeMillis();

        System.out.println("####### Current time: " + convertReadableTime(currentTime));
        System.out.println("####### Current virtualTime: " + convertReadableTime(virtualTime));
        System.out.println("####### Current lastStageSubmissionTime: " + convertReadableTime(lastStageSubmissionTime));
        // advance virtual time if any stage has finished
        for(Tuple2<Integer, Long> stageTuple : endTimes) {
            int stageId = stageTuple._1();
            long endTime = stageTuple._2();
            System.out.println("######## Stage completed: " + stageId + " with endtime: " + convertReadableTime(endTime));

            if(!activeStages.containsKey(stageId)) {
                // This should technically never occur
                System.out.println("######## ERROR: stage was already finished: " +stageId);
                continue;
            }

            // remove the the job from active stages
            TaskSetManager tm = activeStages.remove(stageId);
            if (tm == null) {
                // This should technically never occur
                System.out.println("######## ERROR: stage was already removed: " + stageId);
            }
            // calculate the appropriate share
            double share = ((double) this.totalCores) / (activeStages.size() + 1.0);
            // calculate how much virtual time has advanced
            System.out.println("######## Share: " + share);
            System.out.println("####### Diff" + (endTime - lastStageSubmissionTime));
            long advancement = (long)((endTime - lastStageSubmissionTime) * share);

            if(advancement < 0) {
                System.out.println("######## ERROR: advancement is negative: " + advancement);
            } else {
                virtualTime += advancement;
                lastStageSubmissionTime = endTime;
            }
                System.out.println("######## Updating virtual time:");
                System.out.println("Advancement time: " + advancement);
                System.out.println("VirtualTime: " + convertReadableTime(virtualTime) );
                System.out.println("LastStageSubmissionTime: " + convertReadableTime(lastStageSubmissionTime));

        }

        // If no active stages, reset virtual time
        if(activeStages.isEmpty()) {
            System.out.println("######## Reset");
            virtualTime = currentTime;
            lastStageSubmissionTime = currentTime;
        }


        // advance virtual time by the amount of all stages running concurrently
        // sanity check if laststageSubmission is not in the future
        if(lastStageSubmissionTime <= currentTime) {
            double share = (double)(this.totalCores) / (double)(activeStages.size() + 1);
            long advancement = (long) ((currentTime - lastStageSubmissionTime) * share);
            virtualTime += advancement;
            System.out.println("######## Since last stage submit:");
            System.out.println("VirtualTime: " + convertReadableTime(virtualTime) );
            System.out.println("LastStageSubmissionTime: " + convertReadableTime(lastStageSubmissionTime));
        } else {
            System.out.println("######## ERROR: lastStageSubmission ahead of current time !!!");
        }


        // ######## Modify current stage for submission #########
        int stageId = taskSetManager.stageId();

        // Calculate deadline of current stage
        long expectedRuntime = listener.getRuntimeEstimate(stageId);
        long deadline = virtualTime + expectedRuntime;
        taskSetManager.deadline_$eq(deadline);

        System.out.println("######### Task scheduled with priority: " + convertReadableTime(deadline));
        activeStages.put(taskSetManager.stageId(), taskSetManager);
        lastStageSubmissionTime = currentTime;
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }


}
