import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class  UserClusterFairScheduler implements SchedulableBuilder {


    class UserStage implements Comparable<UserStage> {
        long virtualDeadline;

        long expectedRuntime;
        long remainingTime;
        TaskSetManager tm;
        UserStage(long virtualDeadline, long expectedRuntime, TaskSetManager tm) {
            this.expectedRuntime = expectedRuntime;
            this.remainingTime = expectedRuntime;
            this.virtualDeadline = virtualDeadline;
            this.tm = tm;
        }

        public synchronized void updateDeadline(long virtualTime, long currentTime, double stageShare) {
            // Get how much of the expected time has progressed
            this.remainingTime = virtualDeadline - virtualTime;

            // Calculate the new deadline
            long deadline = currentTime + (long)(remainingTime / stageShare);

            System.out.println("######## Stage calculations:" + tm.stageId());
            System.out.println("### deadline: " + convertReadableTime(tm.deadline()) + " -> " + convertReadableTime(deadline));
            // only update if task is not finished yet
            if (this.remainingTime > 0) {
                this.tm.deadline_$eq(deadline);
            } else {
                System.out.println("### completed");
            }
            System.out.println("virtualDeadline: " + convertReadableTime(virtualDeadline));
            System.out.println("virtualTime: " + convertReadableTime(virtualTime));
            System.out.println("remaining time: " + remainingTime);
            System.out.println("stageShare: " + stageShare);
            System.out.println("currentTime: " + convertReadableTime(currentTime));
            System.out.println("### expectedRuntime: " + this.expectedRuntime);

        }

        @Override
        public int compareTo(UserStage userStage) {
            return Long.compare(this.virtualDeadline, userStage.virtualDeadline);
        }
    }

    class User {

        String name;
        double share;
        long virtualTime;
        long previousCurrentTIme;
        TreeSet<UserStage> activeStages;

        User(String name, double share, long startTime) {
            this.name = name;
            this.share = share;

            this.virtualTime = startTime;
            this.previousCurrentTIme = startTime;
            this.activeStages = new TreeSet<>();
        }

        public long userFinishTime(long currentTime) {
            // if no stages, return current time
            if (activeStages.isEmpty()) {
                System.out.println("###### ERROR: user finish time called on empty user: " + name + "time: " + convertReadableTime(currentTime));
                return currentTime - 1;
            }


            // get what could be current virtual time
            double stageShare = this.share / activeStages.size();
            long currentVirtualTime = this.virtualTime + (long)((currentTime - this.previousCurrentTIme) * stageShare);

            // check if last deadline has passed
            long lastVirtualDeadline = activeStages.last().virtualDeadline;
            if(lastVirtualDeadline <= currentVirtualTime) {
                // return the real time user finished his jobs
                long realTimeSpent = (long)((lastVirtualDeadline - this.virtualTime) / stageShare);
                long userFinishTime = this.previousCurrentTIme + realTimeSpent;
                if (userFinishTime > currentTime) {
                    System.out.println("### ERROR: user finish time is greater than current time, finish: " + convertReadableTime(userFinishTime) + " current time" + convertReadableTime(currentTime));
                    return currentTime - 1;
                }
                return userFinishTime;
            } else {
                return -1;
            }

        }

        public synchronized boolean updateDeadlines(long currentTime) {
            // only update if time has passed
            if (currentTime <= this.previousCurrentTIme) {
                return false;
            }
            // if no stages, do nothing
            if (activeStages.isEmpty()) {
                return true;
            }

            // calculate current shares
            int activeStagesSize = this.activeStages.size();
            double stageShare = this.share / activeStagesSize;

            // update virtual time
            this.virtualTime += (long)((currentTime - this.previousCurrentTIme) * stageShare);
            this.previousCurrentTIme = currentTime;


            System.out.println("######## User:" + name + " stage share:" + stageShare + " user share: " + this.share + " time: " + this.virtualTime);

            // iterate over stages and update them
            Iterator<UserStage> stageIterator = activeStages.iterator();
            while (stageIterator.hasNext()) {
                UserStage stage = stageIterator.next();

                // check if stage has finished, otherwise update progress
                if(stage.virtualDeadline < this.virtualTime) {
                    stageIterator.remove();
                    activeStagesSize--;
                    stageShare = activeStagesSize > 0 ? stageShare / activeStagesSize : 0;
                } else {
                    stage.updateDeadline(this.virtualTime, currentTime, stageShare);
                }

            }
            return activeStages.isEmpty();
        }

        public synchronized void updateShare(double share) {
            this.share = share;
        }

        /** This function assumes that updateDeadlines has been called previously, with updated virtual time
         *
         * @param expectedRuntime
         * @param tm
         */
        public synchronized void addStage(long expectedRuntime, TaskSetManager tm) {

            long virtualDeadline = this.virtualTime + expectedRuntime;
            UserStage stage = new UserStage(virtualDeadline, expectedRuntime, tm);

            System.out.println("######## User:" + name + " adding stage stage: " + tm.stageId() + " with deadline: " + convertReadableTime(virtualDeadline));

            activeStages.add(stage);
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


    private void setPriority(Schedulable schedulable, Properties properties) {
        // TaskSetManager represents stages, other schedulables are ignored
        if(!(schedulable instanceof TaskSetManager taskSetManager)) {
            return;
        }
        this.totalCores = this.sc.defaultParallelism();

        // ######## Update virtual fair scheduler #########
        long currentTime = System.currentTimeMillis();
        System.out.println("####### Current time: " + convertReadableTime(currentTime) + " with cores: " + this.totalCores);

        // check if any user has finished
        do {
            long userMinFinishedTime = currentTime;
            User minUser = null;
            // find user with the earliest finish time
            for(User user : activeUsers.values()) {
                long userFinishTime = user.userFinishTime(currentTime);
                // check if user has finished
                if(userFinishTime == -1) {
                    continue;
                }
                // check if this is the smallest user
                if(userFinishTime < userMinFinishedTime) {
                    userMinFinishedTime = userFinishTime;
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



        // ######## Add stage and update deadlines #########
        int stageId = taskSetManager.stageId();

        // Calculate deadline of current stage
        long expectedRuntime = performanceEstimator.getRuntimeEstimate(stageId);

        // Get current user
        String userName = properties.getProperty("user.name");
        if (userName == null) {
            userName = "DEFAULT";
        }
        String description = properties.getProperty("spark.job.description");
        System.out.println("######## Adding stage User: " + userName + " description: " + description);

        synchronized(this) {
            // get or create a user
            User addingUser = activeUsers.computeIfAbsent(userName, mapUserName -> {
                double userShare = ((double) this.totalCores) / (activeUsers.size() + 1.0);
                // Update all shares, since a new user takes equal portion
                for(User user : activeUsers.values()) {
                    user.updateShare(userShare);
                }
                return new User(mapUserName, userShare, currentTime);
            });
            // add the stage to the user
            addingUser.addStage(expectedRuntime, taskSetManager);

            // update all deadlines to catch up to current time, and recalculate deadlines based on shares
            for(User user : activeUsers.values()) {
                if(user.updateDeadlines(currentTime)) {
                    System.out.println("####### ERROR: user finished all their jobs, should not happen here!");
                    activeUsers.remove(user.name);
                }
            }
        }

        System.out.println("######## INFO_CHECK: time taken for scheudling " + (System.currentTimeMillis() - currentTime));

    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }
}
