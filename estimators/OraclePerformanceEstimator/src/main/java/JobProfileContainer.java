import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JobProfileContainer {
    public static final long DEFAULT_STAGE_RUNTIME = 1000L;
    public static final String ROOT_EXECUTION_ID = "spark.sql.execution.root.id";
    private static final String JOB_CLASS_PROPERTY = "job.class";
    private static final long DEFAULT_PARALLELIZE_TIME = 200L;


    private final Map<Integer, StageInfo> widowStages = new ConcurrentHashMap<>();
    private final Map<Integer, JobProfile> sqlIdToJobProfile;
    private final Map<Integer, JobProfile> stageIdToJobProfile;
    private final Map<Long, JobProfile> executionIdToJobProfile;
    private final Map<String, LinkedList<JobProfile>> jobClassToJobProfiles;

    private static final JobRuntime DEFAULT_JOB_RUNTIME = new JobRuntime(JobRuntime.JOB_INVALID_ID(), DEFAULT_STAGE_RUNTIME);

    JobProfileContainer() {
        sqlIdToJobProfile = new ConcurrentHashMap<>();
        stageIdToJobProfile = new ConcurrentHashMap<>();
        jobClassToJobProfiles = new ConcurrentHashMap<>();
        executionIdToJobProfile = new ConcurrentHashMap<>();
    }


    private void setupOracle() {
        // Add job runtimes
        jobClassToJobProfiles.put(
                "jobs.implementations.ShortOperation",
                new LinkedList<>(List.of(new JobProfile(
                        "jobs.implementations.ShortOperation",
                        20000L))));
        jobClassToJobProfiles.put("jobs.implementations.LongOperation",
                new LinkedList<>(List.of(new JobProfile(
                        "jobs.implementations.LongOperation",
                        235740L))));
        jobClassToJobProfiles.put("jobs.implementations.SuperShortOperation",
                new LinkedList<>(List.of(new JobProfile(
                        "jobs.implementations.SuperShortOperation",
                        4000L))));
    }





    public JobRuntime getJobRuntime(int stageId) {
        JobProfile jobProfile = stageIdToJobProfile.get(stageId);

        // no job profile for this stage
        if (jobProfile == null) {
            System.out.println("######### ERROR: getJobRuntime: No Job Profile found for stage " + stageId);
            return DEFAULT_JOB_RUNTIME;
        }

        long totalRuntime = 0L;
        long jobCount = 0L;
        for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(jobProfile.getJobClass(), key -> new LinkedList<>())) {
            if(historyProfile.isFinished()) {
                jobCount++;
                totalRuntime += historyProfile.getRuntime();
            }
        }

        if(jobCount == 0) {
            System.out.println("######### ERROR: getJobRuntime: No completed history profiles for " + stageId + " with jobclass " + jobProfile.getJobClass());
            return DEFAULT_JOB_RUNTIME;
        }
        long estimatedRuntime = totalRuntime / jobCount;
        //update job estimated runtime
        jobProfile.updateEstimatedRuntime(estimatedRuntime);
        return new JobRuntime(jobProfile.getExecutionId(), estimatedRuntime);

    }

    public long getStageRuntime(int stageId) {
        JobProfile jobProfile = stageIdToJobProfile.get(stageId);

        // no job profile for this stage
        if (jobProfile == null) {
            System.out.println("######### ERROR: getStageRuntime: No Job Profile found for stage " + stageId);
            return DEFAULT_STAGE_RUNTIME;
        }
        // get stage node that corresponds to this stageId
        StageNode stageNode = jobProfile.getStageNode(stageId);
        if (stageNode == null) {
            System.out.println("######### ERROR: getStageRuntime: No stage node found for stage " + stageId + " in job id " + jobProfile.getExecutionId());
            return DEFAULT_STAGE_RUNTIME;
        }

        Set<Integer> stageNodeIds = stageNode.getStageNodeIds();
        long totalRuntime = 0L;
        long jobCount = 0L;
        for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(jobProfile.getJobClass(), key -> new LinkedList<>())) {
            if(historyProfile.isFinished()) {
                StageNode historyStageNode = historyProfile.getStageNode(stageNodeIds);
                if(historyStageNode != null) {
                    jobCount++;
                    totalRuntime += historyStageNode.getRuntime();
                }
            }
        }

        if(jobCount == 0) {
            System.out.println("######### ERROR: getStageRuntime: No completed history profiles for " + stageId + " with jobclass " + jobProfile.getJobClass() + " with stage node id " + stageNodeIds.toString());
            return DEFAULT_STAGE_RUNTIME;
        }
        long estimatedRuntime = totalRuntime / jobCount;
        //update stage node estimated runtime
        stageNode.updateEstimatedRuntime(estimatedRuntime);
        return estimatedRuntime;
    }

    public long getSqlRuntime(int sqlId, long totalSize) {
        JobProfile jobProfile = sqlIdToJobProfile.get(sqlId);
        if (jobProfile == null) {
            System.out.println("######### ERROR: getStageExecutionRuntime: No Job Profile found for execution " + sqlId);
            return DEFAULT_STAGE_RUNTIME;
        }

        StageNode stageNode = jobProfile.getStageNodeWithSqlId(sqlId);
        if (stageNode == null) {
            System.out.println("######### ERROR: getStageExecutionRuntime: No stage node found for execution " + sqlId + " in job:" + jobProfile.getExecutionId());
            return DEFAULT_STAGE_RUNTIME;
        }

        Set<Integer> stageNodeIds = stageNode.getStageNodeIds();
        long totalRuntime = 0L;
        long jobCount = 0L;
        for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(jobProfile.getJobClass(), key -> new LinkedList<>())) {
            if(historyProfile.isFinished()) {
                StageNode historyStageNode = historyProfile.getStageNode(stageNodeIds);
                if(historyStageNode != null) {
                    jobCount++;
                    double ratio = historyStageNode.getTotalToInputRatio(totalSize);
                    totalRuntime += (long)(historyStageNode.getRuntime() * ratio) ;
                }
            }
        }

        if(jobCount == 0) {
            System.out.println("######### ERROR: getStageExecutionRuntime: No completed history profiles for " + sqlId + " with jobclass " + jobProfile.getJobClass() + " with stage node id " + stageNodeIds.toString());
            return DEFAULT_STAGE_RUNTIME;
        }
        long estimatedRuntime = totalRuntime / jobCount;
        //update stage node estimated runtime
        stageNode.updateEstimatedRuntime(estimatedRuntime);
        return estimatedRuntime;
    }



    public void handleSparkListenerStageSubmitted(SparkListenerStageSubmitted stageEvent) {
        StageInfo stageInfo = stageEvent.stageInfo();
        int stageId = stageInfo.stageId();
        System.out.println("####### stage submitted: " + stageId);

        String jobClass = stageEvent.properties().getProperty(JOB_CLASS_PROPERTY, "DEFAULT");
        StageTypeClassifier.Type stageType = StageTypeClassifier.getStageType(stageEvent);
        System.out.println("####### stage type: " + stageType);
        System.out.println("####### job class: " + jobClass);
        // Based on stage class, add mapping from the stage to its corresponding profile
        switch(stageType) {
            case PARALLELIZE -> {
                stageIdToJobProfile.computeIfAbsent(stageId, key -> {
                    System.out.println("####### adding parallelize job with stageId: " + key + " type: " + stageType);
                    return new JobProfile(jobClass, DEFAULT_PARALLELIZE_TIME);
                });
            }
            case SQL -> {
                String executionId = stageEvent.properties().getProperty(ROOT_EXECUTION_ID, null);
                // Get stage runtime based on jobClass and executionId
                JobProfile jobProfile = getAndUpdateJobProfile(jobClass, executionId, stageInfo);
                if(jobProfile != null) {
                    stageIdToJobProfile.computeIfAbsent(stageId, key -> {
                        System.out.println("####### adding SQL job with stageId: " + key + " type: " + stageType + " to job: " + jobProfile.getExecutionId() );
                        return jobProfile;
                    });
                } else {
                    // This stage is suppose to be added to a SQL job profile, but it is not there
                    // We store this tage in the widow list, and hope to find its job profile later
                    System.out.println("####### ERROR: Adding widow stage: " + stageId);
                    widowStages.put(stageId, stageInfo);
                }
            }
            case UNKNOWN -> {
                // If no executionId found, the stage is not a parallelize operation, nor associated with a query
                // hence we just return default for this case
                stageIdToJobProfile.computeIfAbsent(stageId, key -> {
                    System.out.println("####### ERROR: no sql profile found for stageId: " + key);
                    return new JobProfile(jobClass, DEFAULT_STAGE_RUNTIME);
                });
            }
        }
    }

    public void handleSparkListenerStageCompleted(SparkListenerStageCompleted stageEvent) {

        StageInfo stageInfo = stageEvent.stageInfo();
        int stageId = stageInfo.stageId();
        System.out.println("####### stage completed: " + stageId);

        JobProfile jobProfile = stageIdToJobProfile.get(stageId);

        if(jobProfile == null) {
            System.out.println("############ ERROR: Stage completed before being registered, this makes no sense honestly");
            return;
        }

        jobProfile.updateStageCompletion(stageId);

    }


    public void handleSparkListenerSQLExecutionStart(SparkListenerSQLExecutionStart sqlEvent) {
        JobProfile jobProfile = new JobProfile(sqlEvent);
        for(int sqlId : jobProfile.getSqlNodeIds()) {
            sqlIdToJobProfile.put(sqlId, jobProfile);
        }
        executionIdToJobProfile.put(jobProfile.getExecutionId(), jobProfile);
    }

    public void handleSparkListenerSQLAdaptiveExecutionUpdate(SparkListenerSQLAdaptiveExecutionUpdate sqlEvent) {
        JobProfile jobProfile = executionIdToJobProfile.get(sqlEvent.executionId());
        jobProfile.updatePlan(sqlEvent);

        for(int sqlId : jobProfile.getSqlNodeIds()) {
            sqlIdToJobProfile.put(sqlId, jobProfile);
        }
    }

    public void handleSparkListenerSQLExecutionEnd(SparkListenerSQLExecutionEnd sqlEvent) {
        JobProfile jobProfile = executionIdToJobProfile.get(sqlEvent.executionId());
        jobProfile.updateJobCompletion();
    }


    private JobProfile getAndUpdateJobProfile(String jobClass, String executionIdStr, StageInfo stageInfo) {
        // extract executionId as integer
        long executionId = 0;
        try {
            executionId = Long.parseLong(executionIdStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("ERROR: Execution id is not a number: " + executionIdStr);
        }
        JobProfile jobProfile = executionIdToJobProfile.getOrDefault(executionId, null);
        // check if the execution plan is registered
        if(jobProfile == null) {
            System.out.println("####### ERROR: no job profile found for executionId: " + executionId);
            return null;
        }

        // Add class to jobs
        jobProfile.setJobClass(jobClass);
        jobClassToJobProfiles.computeIfAbsent(jobClass, key -> new LinkedList<>()).add(jobProfile);
        // update job profile with stage info
        jobProfile.updateStageNodes(stageInfo);

        return jobProfile;
    }


}
