import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterFairScheduler implements SchedulableBuilder {

    class Job implements Comparable<Job> {
        long jobId;
        long jobRuntime;

        long startVirtualTime;

        long virtualDeadline;
        List<TaskSetManager> activeStages;

        Job(long userVirtualTime, JobRuntime initialJobRuntime) {
            this.jobId = initialJobRuntime.id();
            this.jobRuntime = initialJobRuntime.time();

            this.startVirtualTime = userVirtualTime;

            // set deadlines
            this.virtualDeadline = this.startVirtualTime + initialJobRuntime.time();

            this.activeStages = new ArrayList<>();
        }

        public void addStage(TaskSetManager stage) {
            this.activeStages.add(stage);
        }

        public void updateDeadline(long time) {
            this.jobRuntime = time;
            this.virtualDeadline = this.startVirtualTime + this.jobRuntime;
            System.out.println("####### INFO: deadline for : " + this.jobId + " with runtime : " + this.jobRuntime + " virtual time : " + convertReadableTime(virtualTime));
            activeStages.removeIf(stage -> this.updateDeadline(stage, this.virtualDeadline));
        }

        /**
         *
         * @param stage
         * @return true if the stage has physically finished, false otherwise
         */
        private boolean updateDeadline(TaskSetManager stage, long deadline) {
            // If stage has finished, no need to keep track of it anymore
            if(stage.tasksSuccessful() == stage.numTasks()) {
                return true;
            }
            System.out.println("######## Stage calculations:" + stage.stageId());
            System.out.println("### deadline: " + convertReadableTime(stage.deadline()) + " -> " + convertReadableTime(deadline));
            stage.deadline_$eq(deadline);

            return false;
        }

        @Override
        public int compareTo(@NotNull Job otherJob) {
            // Jobs should be sorted based on virtual deadline, indicating when they would end in a fair scheduler
            int priority = Long.compare(this.virtualDeadline, otherJob.virtualDeadline);
            // Since TreeSet uses comparator for also checking if elements are equal, we dont want to overwrite elements
            // with the same virtual deadlines
            if(priority == 0) {
                return Long.compare(this.jobId, otherJob.jobId);
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


    HashMap<Long, Job> jobIdToJob = new HashMap<>();
    TreeSet<Job> activeJobs = new TreeSet<>();

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


    private void updateJobRuntime(Job currentJob, long time) {
        // to resort the treeset, we have to remove and add the job back
        if(!this.activeJobs.remove(currentJob)) {
            System.out.println("######### ERROR: updating job runtime but current job does not exist in activeJobs with id: " + currentJob.jobId);
        }
        currentJob.updateDeadline(time);
        this.activeJobs.add(currentJob);
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
        double jobShare = !activeJobs.isEmpty() ? ((double) this.totalCores) / (activeJobs.size()) : 0 ;
        Iterator<Job> jobIterator = activeJobs.iterator();
        while (jobIterator.hasNext()) {
            Job activeJob = jobIterator.next();
            System.out.println("######## Job deadline: " + convertReadableTime(activeJob.virtualDeadline));

            long virtualTimeSpent = activeJob.virtualDeadline - virtualTime;
            long realTimeSpent = (long)(virtualTimeSpent / jobShare);
            long jobRealFinishTime = previousCurrentTime + realTimeSpent;

            // check if earliest job has finished
            if(jobRealFinishTime > currentTime) {
                break;
            }

            // remove job from active jobs
            jobIterator.remove();

            // calculate how much virtual time has advanced
            System.out.println("######## Share: " + jobShare);
            System.out.println("####### Virtual time spent" + (virtualTimeSpent));

            virtualTime += virtualTimeSpent;
            previousCurrentTime = jobRealFinishTime;
            jobShare = !activeJobs.isEmpty() ? ((double) this.totalCores) / (activeJobs.size()) : 0 ;

            System.out.println("######## Updating virtual time:");
            System.out.println("VirtualTime: " + convertReadableTime(virtualTime) );
            System.out.println("previousCurrentTime: " + convertReadableTime(previousCurrentTime));
        }


        // ######## Modify current stage for submission #########

        int stageId = stage.stageId();
        JobRuntime jobRuntime = performanceEstimator.getJobRuntime(stageId);

        Job currentJob;
        // check if job id is valid
        if (jobRuntime.id() != JobRuntime.JOB_INVALID_ID()) {
            // Find the corresponding job, or make a new one
            currentJob = this.jobIdToJob.computeIfAbsent(jobRuntime.id(), jobId -> {
                Job newJob = new Job(virtualTime, jobRuntime);
                // add the job to active jobs
                this.activeJobs.add(newJob);
                return newJob;
            });

        } else {
            // Job does not belong to anything, treat it as a single stage job
            currentJob = new Job(virtualTime, jobRuntime);
            this.activeJobs.add(currentJob);
        }

        // add the stage to the corresponding job
        currentJob.addStage(stage);

        this.updateJobRuntime(currentJob, jobRuntime.time());
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }


}
