import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class  UserClusterFairScheduler implements SchedulableBuilder {

    class Job implements Comparable<Job> {
        long jobId;
        long virtualDeadline;
        long previousCurrentTime;
        long startVirtualTime;
        List<TaskSetManager> activeStages;

        Job(long virtualTime, long currentTime, JobRuntime initialJobRuntime) {
            this.jobId = initialJobRuntime.id();
            this.startVirtualTime = virtualTime;
            this.virtualDeadline = this.startVirtualTime + initialJobRuntime.time();
            this.previousCurrentTime = currentTime;

            this.activeStages = new ArrayList<>();
        }

        public void addStage(TaskSetManager stage) {
            this.activeStages.add(stage);
        }

        public void updateDeadlines(long virtualTime, long currentTime, double jobShare) {
            // update all stages and remove the ones that have finished physically
            activeStages.removeIf(stage -> this.updateDeadline(stage, virtualTime, currentTime, jobShare));
        }

        /**
         *
         * @param stage
         * @param virtualTime
         * @param currentTime
         * @return true if the stage has physically finished, false otherwise
         */
        private boolean updateDeadline(TaskSetManager stage, long virtualTime, long currentTime, double jobShare) {
            // If stage has finished, no need to keep track of it anymore
            if(stage.tasksSuccessful() == stage.numTasks()) {
                return true;
            }
            // Get how much of the expected time has progressed
            long remainingTime = this.virtualDeadline - virtualTime;

            // Calculate the new deadline
            long deadline = currentTime + (long)(remainingTime / jobShare);

            System.out.println("######## Stage calculations:" + stage.stageId());
            System.out.println("### deadline: " + convertReadableTime(stage.deadline()) + " -> " + convertReadableTime(deadline));

            // only update if task has not finished yet virtually and physically
            if (remainingTime > 0) {
                stage.deadline_$eq(deadline);
            } else {
                System.out.println("### NO UPDATE, VIRTUALLY COMPLETED");
            }
            System.out.println("virtualDeadline: " + convertReadableTime(virtualDeadline));
            System.out.println("virtualTime: " + convertReadableTime(virtualTime));
            System.out.println("remaining time: " + remainingTime);
            System.out.println("jobShare: " + jobShare);
            System.out.println("currentTime: " + convertReadableTime(currentTime));
            return false;
        }

        @Override
        public int compareTo(@NotNull UserClusterFairScheduler.Job otherJob) {
            // Jobs should be sorted based on virtual deadline, indicating when they would end in a fair scheduler
            return Long.compare(this.virtualDeadline, otherJob.virtualDeadline);
        }


        public void updateJobDeadline(long time) {
            this.virtualDeadline = this.startVirtualTime + time;
        }
    }

    class User {

        String name;
        double share;
        long virtualTime;
        long previousCurrentTIme;
        HashMap<Long, Job> jobIdToJob = new HashMap<>();
        TreeSet<Job> activeJobs;

        User(String name, double share, long startTime) {
            this.name = name;
            this.share = share;

            this.virtualTime = startTime;
            this.previousCurrentTIme = startTime;

            this.jobIdToJob = new HashMap<>();
            this.activeJobs = new TreeSet<>();
        }

        /**
         *
         * @param currentTime
         * @return the time user had finished, otherwise empty.
         */
        public Optional<Long> userFinishTime(long currentTime) {
            // if no jobs, return current time
            if (activeJobs.isEmpty()) {
                System.out.println("###### ERROR: user finish time called on empty user: " + name + "time: " + convertReadableTime(currentTime));
                return Optional.of(currentTime - 1);
            }


            // get what could be current virtual time
            double jobShare = this.share / activeJobs.size();
            long currentVirtualTime = this.virtualTime + (long)((currentTime - this.previousCurrentTIme) * jobShare);

            // check if last deadline has passed
            long lastDeadline = activeJobs.last().virtualDeadline;
            if(lastDeadline <= currentVirtualTime) {
                // return the real time user finished his jobs
                long realTimeSpent = (long)((lastDeadline - this.virtualTime) / jobShare);
                long userFinishTime = this.previousCurrentTIme + realTimeSpent;
                if (userFinishTime > currentTime) {
                    System.out.println("### ERROR: user finish time is greater than current time, finish: " + convertReadableTime(userFinishTime) + " current time" + convertReadableTime(currentTime));
                    return Optional.of(currentTime - 10);
                }
                return Optional.of(userFinishTime);
            } else {
                return Optional.empty();
            }

        }

        /**
         *
         * @param currentTime
         * @return true if all jobs have finished, false otherwise
         */
        public boolean updateDeadlines(long currentTime) {
            // only update if time has passed
            if (currentTime <= this.previousCurrentTIme) {
                return false;
            }

            if (activeJobs.isEmpty()) {
                return true;
            }

            // calculate current shares
            int activeJobsSize = this.activeJobs.size();
            double jobShare = this.share / activeJobsSize;

            // update virtual time
            // Virtual time progresses with the speed of jobShares, since that is the speed each job is
            // progressing forward with. Since we assume all jobs share equal resources, the corresponding
            // virtual time is the same across these jobs
            this.virtualTime += (long)((currentTime - this.previousCurrentTIme) * jobShare);
            this.previousCurrentTIme = currentTime;


            System.out.println("######## User:" + name + " job share:" + jobShare + " user share: " + this.share + " time: " + this.virtualTime);

            // iterate over jobs and update them
            Iterator<Job> jobIterator = activeJobs.iterator();
            while (jobIterator.hasNext()) {
                Job job = jobIterator.next();

                // check if job has finished, otherwise update progress
                if(job.virtualDeadline < this.virtualTime) {
                    jobIterator.remove();
                    activeJobsSize--;
                    jobShare = activeJobsSize > 0 ? this.share / activeJobsSize : 0;
                } else {
                    job.updateDeadlines(this.virtualTime, currentTime, jobShare);
                }
            }
            return activeJobs.isEmpty();
        }

        public void updateShare(double share) {
            this.share = share;
        }

        private Job createAndAddJob(long currentTime, JobRuntime jobRuntime) {
            Job job = new Job(this.virtualTime, currentTime, jobRuntime);
            this.activeJobs.add(job);
            return job;
        }

        /** This function assumes that updateDeadlines has been called previously for this user.
         * This is necessary for virtual times to be correct
         * @param expectedRuntime
         * @param tm
         */
        public void addStage(JobRuntime jobRuntime, TaskSetManager tm, long currentTime) {
            Job currentJob;
            boolean jobShareUpdate = false;
            // check if job id is valid
            if (jobRuntime.id() != JobRuntime.JOB_INVALID_ID()) {
                // Add stage to an existing job or make a new job
                if(this.jobIdToJob.containsKey(jobRuntime.id())) {
                    currentJob = this.jobIdToJob.get(jobRuntime.id());
                    // update jobs deadline, with a more recent estimate
                    currentJob.updateJobDeadline(jobRuntime.time());
                } else {
                    jobShareUpdate = true;
                    currentJob = createAndAddJob(currentTime, jobRuntime);
                    this.jobIdToJob.put(jobRuntime.id(), currentJob);
                }

            } else {
                // If id is invalid, job has no profile, hence we treat it as a 1 stage job.
                jobShareUpdate = true;
                currentJob = createAndAddJob(currentTime, jobRuntime);
            }

            double jobShare = this.share / this.activeJobs.size();
            // if shares changed, update all jobs, otherwise just current job
            if(jobShareUpdate) {
                this.activeJobs.forEach(job -> job.updateDeadlines(this.virtualTime, currentTime, jobShare));
            }
            // create a stage and add to the corresponding job
            currentJob.addStage(tm);
            currentJob.updateDeadlines(this.virtualTime, currentTime, jobShare);


            System.out.println("######## User:" + name + " adding stage stage: " + tm.stageId() +
                    " with deadline: " + convertReadableTime(currentJob.virtualDeadline) + " with runtime: " + jobRuntime.time());

        }
    }

    Pool rootPool;
    SparkContext sc;
    int totalCores;
    PerformanceEstimatorInterface performanceEstimator;

    // User fair scheduling variables
    ConcurrentHashMap<String, User> activeUsers = new ConcurrentHashMap<>();
    long startTime = System.currentTimeMillis();
    UserClusterFairScheduler(Pool rootPool, SparkContext sc) {
        this.rootPool = rootPool;
        this.sc = sc;
        this.totalCores = 1;

        performanceEstimator = sc.getPerformanceEstimator().getOrElse(() -> {
            throw new RuntimeException("Performance estimator not available");
        });
        System.out.println("######## UserClusterFairScheduler started with cores: " + this.totalCores);
    }

    @Override
    public Pool rootPool() {
        return rootPool;
    }

    @Override
    public void buildPools() {

    }

    protected double convertReadableTime(long time) {
        return (time - startTime) / 1000.0;
    }


    private void checkAndUpdateUserFinish(long currentTime) {
        do {
            long userMinFinishedTime = currentTime;
            User minUser = null;
            // find user with the earliest finish time
            for(User user : activeUsers.values()) {
                Optional<Long> userFinishTime = user.userFinishTime(currentTime);
                // check if user has finished
                if(userFinishTime.isEmpty()) {
                    continue;
                }
                // check if this is the smallest user
                if(userFinishTime.get() < userMinFinishedTime) {
                    userMinFinishedTime = userFinishTime.get();
                    minUser = user;
                }
            }

            // check if any user has finished
            if(minUser == null) {
                break;
            } else {
                // progress all users until minUser leaves the queue
                for(User user : activeUsers.values()) {
                    if(user.name.equals(minUser.name)) {
                        continue;
                    }
                    user.updateDeadlines(userMinFinishedTime);
                }

                // remove the user with smallest finish time
                activeUsers.remove(minUser.name);
                // recalculate user shares
                double userShare = ((double) this.totalCores) / ((double)activeUsers.size());
                // Update all users with the new share values
                for(User user : activeUsers.values()) {
                    user.updateShare(userShare);
                }
            }
        } while(true);
    }

    private void addStageAndUpdate(TaskSetManager taskSetManager, Properties properties, long currentTime) {
        int stageId = taskSetManager.stageId();

        // get both stage and job runtimes: job runtime is used for setting deadlines, while stage runtime is used to keep track of when stage ends
        JobRuntime jobRuntime = performanceEstimator.getJobRuntime(stageId);

        // Get current user
        String userName = properties.getProperty("user.name");
        if (userName == null) {
            userName = "DEFAULT";
        }

        System.out.println("######## Adding stage User: " + userName + " description: " + properties.getProperty("spark.job.description"));


        // get or create a user
        User addingUser = activeUsers.computeIfAbsent(userName, mapUserName -> {
            System.out.println("######## New user: " + mapUserName);
            double userShare = ((double) this.totalCores) / (activeUsers.size() + 1.0);
            // Update all shares, since a new user takes equal portion
            for(User user : activeUsers.values()) {
                user.updateShare(userShare);
            }
            return new User(mapUserName, userShare, currentTime);
        });


        // update all deadlines to catch up to current time, and recalculate deadlines based on shares
        for(User user : activeUsers.values()) {
            if(user.updateDeadlines(currentTime)) {
                System.out.println("####### ERROR: user finished all their jobs, should not happen here!");
                activeUsers.remove(user.name);
            }
        }

        // add the stage to the user
        addingUser.addStage(jobRuntime, taskSetManager, currentTime);
    }

    /** In current Spark version, resource offers are handled serially using
     * a synchronized statement in
     * resourceOffers(offers: IndexedSeq[WorkerOffer], isAllFreeResources: Boolean = true),
     * This ensures that all these operations happen sequentially, and we do not worry about concurrency
     *
     * @param schedulable
     * @param properties
     */
    private void setPriority(Schedulable schedulable, Properties properties) {
        // TaskSetManager represents stages, other schedulables are ignored
        if(!(schedulable instanceof TaskSetManager taskSetManager)) {
            return;
        }
        // Update totalCore amount, sometimes it changes as more executors are added/removed
        this.totalCores = this.sc.defaultParallelism();

        // ######## 1. Update virtual fair scheduler #########
        long currentTime = System.currentTimeMillis();
        System.out.println("####### Current time: " + convertReadableTime(currentTime) + " with cores: " + this.totalCores);

        // check if any user has finished
        checkAndUpdateUserFinish(currentTime);

        // ######## 2. Add stage and update deadlines #########
        addStageAndUpdate(taskSetManager, properties, currentTime);

        System.out.println("######## INFO_CHECK: time taken for scheudling " + (System.currentTimeMillis() - currentTime));
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }
}
