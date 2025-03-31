import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class  UserClusterFairScheduler implements SchedulableBuilder {


    class UserStage {
        long lastProgressTime;
        long expectedRuntime;
        long progress;
        double previousShare;
        TaskSetManager tm;
        UserStage(long startTime, long expectedRuntime, TaskSetManager tm) {
            this.lastProgressTime = startTime;
            this.expectedRuntime = expectedRuntime;
            this.progress = 0;
            this.previousShare = 0;
            this.tm = tm;
        }

        public synchronized void updateDeadline(double stageShare, Long currentTime) {
            if(currentTime < this.lastProgressTime) {
                System.out.println("######## ERROR: end time skew detected: " +tm.stageId());
                System.out.println("######## ERROR: previous: " + this.lastProgressTime + " current: " + currentTime);
            }


            // Get how much of the expected time has progressed
            this.progress += (long)((currentTime - this.lastProgressTime) * this.previousShare);

            // Calculate the new deadline
            long deadline = currentTime + (long)((this.expectedRuntime - this.progress) / stageShare);


            System.out.println("######## Stage calculations:" + tm.stageId());
            System.out.println("currentTime: " + convertReadableTime(currentTime));
            System.out.println("lastProgressTime: " + convertReadableTime(this.lastProgressTime));
            System.out.println("previousShare: " + this.previousShare + " -> " + stageShare);
            System.out.println("### progress: " + this.progress);
            System.out.println("### expectedRuntime: " + this.expectedRuntime);
            System.out.println("### deadline: " + convertReadableTime(tm.deadline()) + " -> " + convertReadableTime(deadline));

            this.tm.deadline_$eq(deadline);
            // update previous variables
            this.previousShare = stageShare;
            this.lastProgressTime = currentTime;
        }
    }

    class User {

        String name;
        double share;
        HashMap<Integer, UserStage> activeStages = new HashMap<>();

        User(String name, double share) {
            this.name = name;
            this.share = share;
        }

        /** Updates deadlines of all tasksets for this user
         *
         * @param estimatedCurrentTime - time used for advancing tasks in fair scheduler
         */
        public synchronized void updateDeadlines(Long estimatedCurrentTime) {
            double stageShare = share / (activeStages.size());
            System.out.println("######## User:" + name + " stage share:" + stageShare + " user share: " + this.share);
            for(UserStage userStage : activeStages.values()) {
                userStage.updateDeadline(stageShare, estimatedCurrentTime);
            }
        }

        public synchronized void updateShare(double share) {
            this.share = share;
        }

        public synchronized void addStage(long startTime, long expectedRuntime, TaskSetManager tm) {

            UserStage stage = new UserStage(startTime, expectedRuntime, tm);
            activeStages.put(tm.stageId(), stage);
        }

        /** Remove a completed stage from user
         *
         * @param stageId - stage to remove
         * @return returns true if user now has no stages, false otherwise
         */
        public synchronized boolean completeStage(int stageId) {
            if(activeStages.remove(stageId) == null) {
                System.out.println("######## ERROR: Stage " + stageId + " not found");
            }

            return activeStages.isEmpty();
        }
    }

    Pool rootPool;
    SparkContext sc;
    int totalCores;
    StageListener listener;

    // User fair scheduling variables
    ConcurrentHashMap<String, User> activeUsers = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, String> stageIdToUser = new ConcurrentHashMap<>();
    long startTime = System.currentTimeMillis();
    UserClusterFairScheduler(Pool rootPool, SparkContext sc) {
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

    protected double convertReadableTime(long time) {
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

        // ######## Update virtual fair scheduler #########

        // see if any stages have finished
        List<Tuple2<Integer, Long>> endTimes = listener.getStageEndTimes();

        long currentTime = System.currentTimeMillis();
        System.out.println("####### Current time: " + convertReadableTime(currentTime));

        // advance fair scheduler
        long previousEndTime = 0;
        for(Tuple2<Integer, Long> stageTuple : endTimes) {
            int stageId = stageTuple._1();
            long endTime = stageTuple._2();

            if( previousEndTime > endTime) {
                System.out.println("######## ERROR: end time skew detected: " +stageId);
                System.out.println("######## ERROR: previous: " + previousEndTime + " current: " + endTime);
            }
            if(!stageIdToUser.containsKey(stageId)) {
                // This should technically never occur
                System.out.println("######## ERROR: stage was already finished: " +stageId);
                continue;
            }

            // remove the stage from the map
            String userName = stageIdToUser.remove(stageId);
            System.out.println("######## Stage completed: " + stageId
                    + " with endtime: " + convertReadableTime(endTime)
                    + " for user: " + userName
            );

            // To ensure that multiple threads dont add/remove users
            synchronized(this) {
                if(!activeUsers.containsKey(userName)) {
                    // This should technically never occur
                    System.out.println("######## ERROR: user not found: " +stageId);
                    continue;
                }

                // remove the stage from the user
                User currentUser = activeUsers.get(userName);
                if(currentUser.completeStage(stageId)) {
                    activeUsers.remove(userName);
                    // Update shares for other users
                    if(!activeUsers.isEmpty()) {
                        double userShare = ((double) this.totalCores) / (activeUsers.size());
                        for(User u : activeUsers.values()) {
                            u.updateShare(userShare);
                        }
                    }

                }
            }

            previousEndTime = endTime;
        }

        // ######## Add stage and update deadlines #########
        int stageId = taskSetManager.stageId();

        // Calculate deadline of current stage
        long expectedRuntime = listener.getRuntimeEstimate(stageId);

        // Get current user
        String userName = properties.getProperty("user.name");
        if (userName == null) {
            userName = "DEFAULT";
        }

        synchronized(this) {
            if (!activeUsers.containsKey(userName)) {
                // create a new user in the fair scheduler
                double userShare = ((double) this.totalCores) / (activeUsers.size() + 1.0);
                User newUser = new User(userName, expectedRuntime);
                activeUsers.put(userName, newUser);

                newUser.addStage(currentTime, expectedRuntime, taskSetManager);

                // Update all shares, since a new user takes equal portion
                for(User user : activeUsers.values()) {
                    user.updateShare(userShare);
                }
            } else {
                // update only the current user
                User currentUser = activeUsers.get(userName);
                currentUser.addStage(currentTime, expectedRuntime, taskSetManager);

            }
            // update all deadlines to catch up to current time, and recalculate deadlines based on shares
            for(User user : activeUsers.values()) {
                user.updateDeadlines(currentTime);
            }
            // Keep track of who the stage belongs to
            stageIdToUser.put(stageId, userName);

        }

        System.out.println("######## INFO_CHECK: time taken for scheudling " + (System.currentTimeMillis() - currentTime));

    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        setPriority(manager, properties);

        rootPool.addSchedulable(manager);
    }
}
