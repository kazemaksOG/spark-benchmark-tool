import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class  UserClusterFairScheduler implements SchedulableBuilder {


    public class CustomTreeSet<T> extends TreeSet<T> {
        public CustomTreeSet() {
            super();
        }

        public CustomTreeSet(java.util.Comparator<? super T> comparator) {
            super(comparator);
        }
        // 'update' method; returns false if removal fails or duplicate after update

        public boolean linearRemove(Object o) {
            Iterator<T> it = this.iterator();
            while (it.hasNext()) {
                T current = it.next();
                if (current.equals(o)) {
                    it.remove();
                    return true;
                }
            }
            return false;
        }
    }



    private static final double BASE_GRACE_PERIOD_MS = 3000;

    class UserContainer {
        ConcurrentHashMap<String, User> activeUsers = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, User> historicUsers = new ConcurrentHashMap<>();
        CustomTreeSet<User> orderedUsers = new CustomTreeSet<>();
        int totalCores;
        long globalVirtualTime;
        long previousCurrentTime;
        double gracePeriod;
        UserContainer() {
            this.totalCores = 1;
            this.globalVirtualTime = 0;
            this.previousCurrentTime = 0;
            this.gracePeriod = BASE_GRACE_PERIOD_MS;
        }

        private void updateGracePeriod() {
            this.gracePeriod = BASE_GRACE_PERIOD_MS * this.totalCores / 2;
        }

        public void setCores(int cores) {
            this.totalCores = cores;
            this.updateGracePeriod();
        }

        public long getGlobalVirtualTime() {
            return this.globalVirtualTime;
        }

        public User addOrGetUser(String userName, long jobId) {
            // first check if user previously existed
            if (this.historicUsers.containsKey(userName)) {
                // revive the user and remove from history
                User oldUser = this.historicUsers.remove(userName);
                oldUser.revive(this.globalVirtualTime, this.gracePeriod, jobId);

                // add user to active users and ordered list
                if (this.activeUsers.put(userName, oldUser) != null) {
                    System.out.println("ERROR: User " + userName + " already in active users?");
                }
                this.orderedUsers.add(oldUser);
                return oldUser;
            }
            // find the user in active users, or create it
            return this.activeUsers.computeIfAbsent(userName, mapUserName -> {
                System.out.println("######## New user: " + mapUserName);
                User newUser = new User(mapUserName, this.globalVirtualTime);
                // adding it to ordered list
                this.orderedUsers.add(newUser);
                return newUser;
            });
        }


        public void progressVirtualTime(long currentTime, double userShare) {
            // If currentTime is smaller than previous time, it means
            // that a revived user is trying to move the time forward, but its in the past
            if (currentTime < this.previousCurrentTime) {
                return;
            }
            // check if there are any users in the system
            if(!this.activeUsers.isEmpty()) {
                // Update global virtual time
                long passedRealTime = currentTime - this.previousCurrentTime;
                System.out.println("#### INFO: usershare " + userShare + " passedRealTime " + passedRealTime + " globalVirtualTime " + this.globalVirtualTime);
                this.globalVirtualTime += (long) (passedRealTime * userShare);

                // Update virtual time of all active users
                for (User user : this.activeUsers.values()) {
                    user.updateVirtualTime(userShare, this.previousCurrentTime, currentTime);
                }
            }

            // Save current time to measure progress in the next update
            this.previousCurrentTime = currentTime;
        }


        public void updateVirtualTime(long currentTime) {

            // Phase 1: Advance the virtual time as users leave the system
            // This is necessary since virtual time progresses faster with less users in the system

            // repeat until no finished user is encountered
            Iterator<User> userIterator = orderedUsers.iterator();
            while (userIterator.hasNext()) {
                User minUser = userIterator.next();
                double userShare = ((double) this.totalCores) / ((double) activeUsers.size());


                Optional<Long> userFinishTime = minUser.userRealFinishTime(
                        this.globalVirtualTime,
                        this.previousCurrentTime,
                        currentTime,
                        userShare);

                // If the earliest user is not finished, break
                if(userFinishTime.isEmpty()) {
                    break;
                }

                // Progress virtual time until the time minUser leaves the system
                // This is done before removing the user, because in case of revival, their virtual time must be consistent
                this.progressVirtualTime(userFinishTime.get(), userShare);

                // remove the user from active users
                this.historicUsers.put(minUser.name, minUser);
                if(this.activeUsers.remove(minUser.name) == null) {
                    System.out.println("####### ERROR: User " + minUser.name + " is already removed from hashmap");
                }
                userIterator.remove();
                System.out.println("INFO: removing user " + minUser.name + " from hashmap");


            }

            // Phase 2: advance virtual time until current time
            double userShare = ((double) this.totalCores) / ((double) activeUsers.size());
            this.progressVirtualTime(currentTime, userShare);
        }

        public void updateUserOrder(User addingUser) {
            // to resort the treeset, we have to remove and add the job back
            if(!this.orderedUsers.linearRemove(addingUser)) {
                System.out.println("######### ERROR: updating user but current user does not exist in orderedUsers with name: " + addingUser.name);
                System.out.println("######### current users:");
                for (User user : this.orderedUsers) {
                    String deadline = user.activeJobs.size() > 0 ? Long.toString(user.activeJobs.last().getGlobalVirtualDeadline()) : "empty";
                    System.out.println(user.name + " " + deadline);
                }
            }
            this.orderedUsers.add(addingUser);
        }
    }



    class Job implements Comparable<Job> {
        long jobId;
        long jobRuntime;

        long startUserVirtualTime;

        long userVirtualDeadline;
        long globalVirtualDeadline;
        List<TaskSetManager> activeStages;

        Job(long userVirtualTime, JobRuntime initialJobRuntime) {
            this.jobId = initialJobRuntime.id();
            this.jobRuntime = initialJobRuntime.time();

            this.startUserVirtualTime = userVirtualTime;

            // set deadlines
            this.globalVirtualDeadline = 0;
            this.userVirtualDeadline = this.startUserVirtualTime + initialJobRuntime.time();

            this.activeStages = new ArrayList<>();
        }

        public void addStage(TaskSetManager stage) {
            this.activeStages.add(stage);
        }

        public void updateUserDeadline(long time) {
            this.userVirtualDeadline = this.startUserVirtualTime + time;
            this.jobRuntime = time;
        }

        public void updateGlobalDeadlines(long globalVirtualTime) {
            // update all stages and remove the ones that have finished physically
            this.globalVirtualDeadline = globalVirtualTime + this.jobRuntime;
            System.out.println("####### INFO: global deadline for : " + this.jobId + " with runtime : " + this.jobRuntime + " global virtual time : " + globalVirtualTime + " globalVirtualDeadline : " + this.globalVirtualDeadline);
            activeStages.removeIf(stage -> this.updateDeadline(stage, this.globalVirtualDeadline));
        }

        /**
         *
         * @param stage
         * @return true if the stage has physically finished, false otherwise
         */
        private boolean updateDeadline(TaskSetManager stage, long globalVirtualDeadline) {
            // If stage has finished, no need to keep track of it anymore
            if(stage.tasksSuccessful() == stage.numTasks()) {
                return true;
            }
            System.out.println("######## Stage calculations:" + stage.stageId());
            System.out.println("### deadline: " + stage.deadline() + " -> " + globalVirtualDeadline);
            stage.deadline_$eq(globalVirtualDeadline);

            return false;
        }

        @Override
        public int compareTo(@NotNull UserClusterFairScheduler.Job otherJob) {
            if(this.jobId == otherJob.jobId) {
                return 0;
            }
            // Jobs should be sorted based on virtual deadline, indicating when they would end in a fair scheduler
            int priority = Long.compare(this.userVirtualDeadline, otherJob.userVirtualDeadline);
            // Since TreeSet uses comparator for also checking if elements are equal, we dont want to overwrite elements
            // with the same virtual deadlines
            if(priority == 0) {
              return Long.compare(this.jobId, otherJob.jobId);
            }
            return priority;
        }


        public long getGlobalVirtualDeadline() {
            return this.globalVirtualDeadline;
        }
    }

    class User implements Comparable<User> {

        String name;
        long userVirtualTime;
        long globalVirtualStartTime;
        long globalVirtualEndTime;
        HashMap<Long, Job> jobIdToJob;
        TreeSet<Job> activeJobs;
        HashMap<Long, Job> finishedJobs;

        User(String name, long globalVirtualTime) {
            this.name = name;
            this.userVirtualTime = 0;
            this.globalVirtualStartTime = globalVirtualTime;
            this.globalVirtualEndTime = globalVirtualTime;

            this.jobIdToJob = new HashMap<>();
            this.activeJobs = new TreeSet<>();
            this.finishedJobs = new HashMap<>();
        }

        /**
         *
         * @param currentTime
         * @return the time user had finished, otherwise empty.
         */
        public Optional<Long> userRealFinishTime(long globalVirtualTime, long previousCurrentTime, long currentTime, double userShare) {
            // if no jobs, return current time
            if (activeJobs.isEmpty()) {
                System.out.println("###### ERROR: user finish time called on empty user: " + name + "time: " + convertReadableTime(currentTime));
                return Optional.of(currentTime - 1);
            }
            long currentGlobalVirtualTime = globalVirtualTime + (long)((currentTime - previousCurrentTime) * userShare);
            long lastJobGlobalVirtualDeadline = this.globalVirtualEndTime;
            if(lastJobGlobalVirtualDeadline <= currentGlobalVirtualTime) {
                System.out.println("##### INFO:"
                        + " gloablVirtaulEndtime : " + this.globalVirtualEndTime
                        + " userVirtualTime: " + this.userVirtualTime
                        + " currentGlobalVirtualTime: " + currentGlobalVirtualTime);
                long realTimeSpent = (long)((lastJobGlobalVirtualDeadline - globalVirtualTime) / userShare);
                long userFinishTime = previousCurrentTime + realTimeSpent;
                return Optional.of(userFinishTime);
            } else {
                return Optional.empty();
            }

        }


        public void updateVirtualTime(double userShare, long previousCurrentTime, long currentTime ) {
            int jobAmount = this.activeJobs.size();
            if(jobAmount == 0) {
                System.out.println("######## ERROR: updateVirtualTime called on empty job list for user: " + this.name);
                return;
            }
            double jobShare = userShare / (double) jobAmount;
            Iterator<Job> jobIterator = activeJobs.iterator();
            while (jobIterator.hasNext()) {
                Job job = jobIterator.next();
                long passedRealTime = currentTime - previousCurrentTime;
                long currentUserVirtualTime = this.userVirtualTime + (long)(passedRealTime * jobShare);
                if(job.userVirtualDeadline <= currentUserVirtualTime) {
                    long virtualTimeSpent = job.userVirtualDeadline - this.userVirtualTime;
                    long realTimeSpent = (long)(virtualTimeSpent / jobShare);
                    // advance virtual time based on amount of current shares
                    // if negative, it means that some stages are lagging behind, but we do not need to add them
                    // since virtual time already accounted for this job finishing
                    if(virtualTimeSpent >= 0 && !this.finishedJobs.containsKey(job.jobId)) {
                        this.userVirtualTime += virtualTimeSpent;
                        previousCurrentTime += realTimeSpent;
                        // advance user job virtual start time
                        this.globalVirtualStartTime += job.jobRuntime;
                        this.finishedJobs.put(job.jobId, job);
                        System.out.println("##### INFO: advancing globalStartTime for user: " + name
                                + " globalStartTime: " + this.globalVirtualStartTime
                                + " jobid: " + job.jobId 
                                + " realTimeSpent: " + realTimeSpent
                                + " virtualTimeSpent: " + virtualTimeSpent
                                + " userVirtualTime: " + userVirtualTime);
                    } else {
                        System.out.println("##### ERROR: late stage for user: " + name + "jobId:" + job.jobId + "job user deadline:" + job.userVirtualDeadline + " virtualTimeSpent:" + virtualTimeSpent + " userVirtualTime:" + userVirtualTime + " finished: " +!this.finishedJobs.containsKey(job.jobId));
                    }

                    // remove the finished job and recalculate share
                    jobIterator.remove();
                    jobAmount--;
                    jobShare = jobAmount > 0 ? userShare / (double) jobAmount : 0;
                } else {
                    break;
                }
            }
            long passedRealTime = currentTime - previousCurrentTime;
            this.userVirtualTime += (long)(passedRealTime * jobShare);
        }

        /** This function assumes that updateVirtualTime has been called previously for this user.
         * This is necessary for virtual times to be correct
         * @param tm
         */
        public void addStage(long globalVirtualTime, JobRuntime jobRuntime, TaskSetManager tm) {
            Job currentJob;
            // check if job id is valid
            if (jobRuntime.id() != JobRuntime.JOB_INVALID_ID()) {
                // Find the corresponding job, or make a new one
                currentJob = this.jobIdToJob.computeIfAbsent(jobRuntime.id(), jobId -> {
                    Job newJob = new Job(this.userVirtualTime, jobRuntime);
                    // add the job to active jobs
                    this.activeJobs.add(newJob);
                    return newJob;
                });

            } else {
                // Job does not belong to anything, treat it as a single stage job
                currentJob = new Job(this.userVirtualTime, jobRuntime);
                this.activeJobs.add(currentJob);
            }

            // add the stage to the corresponding job
            currentJob.addStage(tm);

            // update job runtime if it changed
            if(currentJob.jobRuntime != jobRuntime.time()) {
                this.updateJobRuntime(currentJob, jobRuntime.time());
            }

            // Update global deadlines of jobs
            this.updateDeadlines();

            System.out.println("######## User:" + name + " adding stage stage: " + tm.stageId() + " globalVirtualTime: " + globalVirtualTime +
                    " with global deadline: " + currentJob.getGlobalVirtualDeadline() + "with global start time: " + this.globalVirtualStartTime + " with runtime: " + jobRuntime.time());

        }

        private void updateDeadlines() {
            // check if not empty
            Iterator<Job> jobIterator = this.activeJobs.iterator();
            if(!jobIterator.hasNext()) return;

            // the first jobs takes the globalVirtualTime from when the user
            // started + all the finished job times combined
            long currentGlobalVirtualTime = this.globalVirtualStartTime;
            // jobs finish one after another, so we chain their deadlines
            while (jobIterator.hasNext()) {
                Job job = jobIterator.next();
                job.updateGlobalDeadlines(currentGlobalVirtualTime);
                currentGlobalVirtualTime = job.getGlobalVirtualDeadline();
            }
            // keep note of time when all jobs end for this user
            this.globalVirtualEndTime = currentGlobalVirtualTime;
        }

        private void updateJobRuntime(Job currentJob, long time) {
            // to resort the treeset, we have to remove and add the job back
            if(!this.activeJobs.remove(currentJob)) {
                System.out.println("######### ERROR: updating job runtime but current job does not exist in activeJobs with id: " + currentJob.jobId);
            }
            currentJob.updateUserDeadline(time);
            this.activeJobs.add(currentJob);
        }

        public void revive(long globalVirtualTime, double gracePeriod, long jobId) {
            // if user is within grace period, and the stage submitted is a continuation of an existing job, keep the old virtual time
            if(this.jobIdToJob.containsKey(jobId) && globalVirtualTime - this.globalVirtualEndTime <= gracePeriod) {
                System.out.println("Reviving user with old virtual time, global virtual time " + globalVirtualTime + " grace period: " + gracePeriod +" global end time: " + this.globalVirtualEndTime + " global start time" + this.globalVirtualStartTime);
            } else {
                System.out.println("Reviving user with new virtual time, global virtual time and start time " + globalVirtualTime + " grace period: " + gracePeriod +" global end time: " + this.globalVirtualEndTime + " previous global start time" + this.globalVirtualStartTime);
                this.globalVirtualStartTime = globalVirtualTime;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof User other)) return false;
            return this.name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public int compareTo(@NotNull UserClusterFairScheduler.User otherUser) {
            if(this.name.equals(otherUser.name)) {
                return 0;
            }
            // We sort jobs based on their latest job global virtual deadline
            if(otherUser.activeJobs.isEmpty()) {
                return 1;
            }
            if(this.activeJobs.isEmpty()) {
                return -1;
            }
            int priority = Long.compare(this.activeJobs.last().getGlobalVirtualDeadline(), otherUser.activeJobs.last().getGlobalVirtualDeadline());
            // Since TreeSet uses comparator for also checking if elements are equal, we dont want to overwrite elements
            // with the same latest virtual deadlines
            if(priority == 0) {
                return this.name.compareTo(otherUser.name);
            }
            return priority;
        }
    }

    Pool rootPool;
    SparkContext sc;
    PerformanceEstimatorInterface performanceEstimator;

    // User fair scheduling variables
    UserContainer userContainer = new UserContainer();
    long startTime = System.currentTimeMillis();
    UserClusterFairScheduler(Pool rootPool, SparkContext sc) {
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

    protected double convertReadableTime(long time) {
        return (time - startTime) / 1000.0;
    }


    private void checkAndUpdateUsers(long currentTime) {

        this.userContainer.updateVirtualTime(currentTime);


    }

    private void addStageAndUpdate(TaskSetManager stage, Properties properties) {

        // Get user submitting the stage
        String userName = properties.getProperty("user.name");
        if (userName == null) {
            userName = "DEFAULT";
        }
        System.out.println("######## Adding stage User: " + userName + " description: " + properties.getProperty("spark.job.description"));


        // get job runtime and the job id the stage belongs to, so virtual deadline can be correctly set
        int stageId = stage.stageId();
        JobRuntime jobRuntime = performanceEstimator.getJobRuntime(stageId);

        // get or create the user
        User addingUser = this.userContainer.addOrGetUser(userName, jobRuntime.id());


        // add the stage to the user
        addingUser.addStage(this.userContainer.getGlobalVirtualTime(), jobRuntime , stage);
        
        // update user order
        this.userContainer.updateUserOrder(addingUser);
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
        if(!(schedulable instanceof TaskSetManager stage)) {
            return;
        }
        // Update totalCore amount, sometimes it changes as more executors are added/removed
        this.userContainer.setCores(this.sc.defaultParallelism());

        // ######## 1. Progress virtual time for all users #########
        long currentTime = System.currentTimeMillis();
        System.out.println("####### Current time: " + convertReadableTime(currentTime));
        checkAndUpdateUsers(currentTime);

        // ######## 2. Add stage and update deadlines #########
        addStageAndUpdate(stage, properties);

        System.out.println("######## INFO_CHECK: time taken for scheudling " + (System.currentTimeMillis() - currentTime));
    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }
}
