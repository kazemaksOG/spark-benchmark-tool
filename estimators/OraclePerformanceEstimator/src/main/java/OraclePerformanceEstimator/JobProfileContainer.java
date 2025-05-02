package OraclePerformanceEstimator;

import OraclePerformanceEstimator.Jobs.JobProfile;
import OraclePerformanceEstimator.Jobs.SQL.SqlJobProfile;
import OraclePerformanceEstimator.Jobs.SQL.StageNode;
import OraclePerformanceEstimator.Jobs.SingleStage.SingleStageJobProfile;
import OraclePerformanceEstimator.Util.StageTypeClassifier;
import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static OraclePerformanceEstimator.Jobs.JobProfile.DEFAULT_JOB_CLASS;
import static OraclePerformanceEstimator.Jobs.JobProfile.DEFAULT_STAGE_RUNTIME;

public class JobProfileContainer {

    private static AtomicLong nextJobGroupId = new AtomicLong(0);

    public static final String ROOT_EXECUTION_ID = "spark.sql.execution.root.id";
    public static final String JOB_CLASS_PROPERTY = "job.class";

    private static final String DEFAULT_JOB_GROUP = "DEFAULT";
    private static final String JOB_GROUP_PROPERTY = "spark.jobGroup.id";



    private final Map<Integer, StageInfo> widowStages = new ConcurrentHashMap<>();
    private final Map<Integer, SqlJobProfile> sqlIdToJobProfile;
    private final Map<Integer, JobProfile> stageIdToJobProfile;
    private final Map<Long, SqlJobProfile> executionIdToJobProfile;
    private final Map<String, Long> jobGroupToJobGroupId;
    private final Map<String, LinkedList<JobProfile>> jobClassToJobProfiles;

    private static final JobRuntime DEFAULT_JOB_RUNTIME = new JobRuntime(JobRuntime.JOB_INVALID_ID(), DEFAULT_STAGE_RUNTIME);

    public JobProfileContainer() {
        sqlIdToJobProfile = new ConcurrentHashMap<>();
        stageIdToJobProfile = new ConcurrentHashMap<>();
        jobClassToJobProfiles = new ConcurrentHashMap<>();
        executionIdToJobProfile = new ConcurrentHashMap<>();
        jobGroupToJobGroupId = new ConcurrentHashMap<>();
        setupOracle();
    }


    private void setupOracle() {
        // Add job runtimes
        jobClassToJobProfiles.put(
                "jobs.implementations.ShortOperation",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.ShortOperation",
                        15933L))));
        jobClassToJobProfiles.put(
                "jobs.implementations.LongOperation",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.LongOperation",
                        136486L))));
        jobClassToJobProfiles.put("jobs.implementations.SuperShortOperation",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.SuperShortOperation",
                        2671L))));


        jobClassToJobProfiles.put("jobs.implementations.udf.Loop1000",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.udf.Loop1000",
                        111115L))));
        jobClassToJobProfiles.put("jobs.implementations.udf.Loop500",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.udf.Loop500",
                        57115L))));

        jobClassToJobProfiles.put("jobs.implementations.udf.Loop100",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.udf.Loop100",
                        26926L))));

        jobClassToJobProfiles.put("jobs.implementations.udf.Loop20",
                new LinkedList<>(List.of(new SqlJobProfile(
                        JobRuntime.JOB_INVALID_ID(),
                        "jobs.implementations.udf.Loop20",
                        1523L))));

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
                System.out.println("##### INFO: using jobgroup:" + historyProfile.getJobGroupId());
                System.out.println("##### INFO: runtime:" + historyProfile.getRuntime());
                jobCount++;
                totalRuntime += historyProfile.getRuntime();
            }
        }

        if(jobCount == 0) {
            System.out.println("######### ERROR: getJobRuntime: No completed history profiles for " + stageId + " with jobclass " + jobProfile.getJobClass());
            System.out.println("######## jobClassToJobProfiles: " + jobClassToJobProfiles.get(jobProfile.getJobClass()).toString());
            return DEFAULT_JOB_RUNTIME;
        }
        long estimatedRuntime = totalRuntime / jobCount;
        System.out.println("###### INFO: estimatedRuntime: " + estimatedRuntime + " for job " + jobProfile.getJobId());
        //update job estimated runtime
        jobProfile.updateEstimatedRuntime(estimatedRuntime);
        return new JobRuntime(jobProfile.getJobGroupId(), estimatedRuntime);

    }

    public long getStageRuntime(int stageId) {
        JobProfile jobProfile = stageIdToJobProfile.get(stageId);

        // no job profile for this stage
        if (jobProfile == null) {
            System.out.println("######### ERROR: getStageRuntime: No Job Profile found for stage " + stageId);
            return DEFAULT_STAGE_RUNTIME;
        }

        if (jobProfile instanceof SqlJobProfile sqlJobProfile) {

            // get stage node that corresponds to this stageId
            StageNode stageNode = sqlJobProfile.getStageNode(stageId);
            if (stageNode == null) {
                System.out.println("######### ERROR: getStageRuntime: No stage node found for stage " + stageId + " in job id " + sqlJobProfile.getExecutionId());
                return DEFAULT_STAGE_RUNTIME;
            }

            Set<Integer> stageNodeIds = stageNode.getStageNodeIds();
            long totalRuntime = 0L;
            long jobCount = 0L;
            for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(sqlJobProfile.getJobClass(), key -> new LinkedList<>())) {
                if(historyProfile.isFinished()) {
                    if(historyProfile instanceof SqlJobProfile sqlHistoryProfile) {
                        StageNode historyStageNode = sqlHistoryProfile.getStageNode(stageNodeIds);
                        if(historyStageNode != null) {
                            jobCount++;
                            totalRuntime += historyStageNode.getRuntime();
                        }
                    }
                }
            }

            if(jobCount == 0) {
                System.out.println("######### ERROR: getStageRuntime: No completed history profiles for " + stageId + " with jobclass " + jobProfile.getJobClass() + " with stage node id " + stageNodeIds.toString());
                return jobProfile.getRuntime();
            }
            long estimatedRuntime = totalRuntime / jobCount;

            //update stage node estimated runtime
            stageNode.updateEstimatedRuntime(estimatedRuntime);
            return estimatedRuntime;

        } else if(jobProfile instanceof SingleStageJobProfile singleStageJobProfile) {

            long totalRuntime = 0L;
            long jobCount = 0L;
            for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(singleStageJobProfile.getJobClass(), key -> new LinkedList<>())) {
                if(historyProfile.isFinished()) {
                    if (historyProfile instanceof SingleStageJobProfile) {
                        jobCount++;
                        totalRuntime += historyProfile.getRuntime();
                    }
                }
            }
            if(jobCount == 0) {
                System.out.println("######### ERROR: getStageRuntime: No completed history profiles for " + stageId + " with jobclass " + jobProfile.getJobClass() + " jobid: " + singleStageJobProfile.getJobId());
                return DEFAULT_STAGE_RUNTIME;
            }
            return totalRuntime / jobCount;
        } else {
            throw new RuntimeException("Unknown Job Profile " + jobProfile.getJobClass());
        }
    }

    public long getSqlRuntime(int sqlId, long totalSize) {
        SqlJobProfile jobProfile = sqlIdToJobProfile.get(sqlId);
        if (jobProfile == null) {
            System.out.println("######### ERROR: getSqlRuntime: No Job Profile found for execution " + sqlId);
            return DEFAULT_STAGE_RUNTIME;
        }

        StageNode stageNode = jobProfile.getStageNodeWithSqlId(sqlId);
        if (stageNode == null) {
            System.out.println("######### ERROR: getSqlRuntime: No stage node found for execution " + sqlId + " in job:" + jobProfile.getExecutionId());
            return DEFAULT_STAGE_RUNTIME;
        }

        Set<Integer> stageNodeIds = stageNode.getStageNodeIds();
        long totalRuntime = 0L;
        long jobCount = 0L;


        for(JobProfile historyProfile : jobClassToJobProfiles.computeIfAbsent(jobProfile.getJobClass(), key -> new LinkedList<>())) {
            if(historyProfile.isFinished()) {
                if(historyProfile instanceof SqlJobProfile sqlHistoryProfile) {
                    StageNode historyStageNode = sqlHistoryProfile.getStageNode(stageNodeIds);
                    if(historyStageNode != null) {
                        jobCount++;
                        double ratio = historyStageNode.getTotalToInputRatio(totalSize);
                        totalRuntime += (long)(historyStageNode.getRuntime() * ratio) ;
                    }
                }
            }
        }


        if(jobCount == 0) {
            System.out.println("######### ERROR: getSqlRuntime: No completed history profiles for " + sqlId + " with jobclass " + jobProfile.getJobClass() + " with stage node id " + stageNodeIds.toString());
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

        String jobClass = stageEvent.properties().getProperty(JOB_CLASS_PROPERTY, DEFAULT_JOB_CLASS);
        String jobGroup = stageEvent.properties().getProperty(JOB_GROUP_PROPERTY, DEFAULT_JOB_GROUP);
        long jobGroupId = jobGroupToJobGroupId.computeIfAbsent(jobGroup, group -> nextJobGroupId.getAndIncrement());

        StageTypeClassifier.Type stageType = StageTypeClassifier.getStageType(stageEvent);
        System.out.println("####### stage type: " + stageType);
        System.out.println("####### job class: " + jobClass);
        System.out.println("####### jobGroupId: " + jobGroupId + " associated with: " + jobGroup);
        // Based on stage class, add mapping from the stage to its corresponding profile
        switch(stageType) {
            case SQL -> {
                String executionId = stageEvent.properties().getProperty(ROOT_EXECUTION_ID, null);
                // Get stage runtime based on jobClass and executionId
                SqlJobProfile jobProfile = getAndUpdateJobProfile(jobClass, executionId, stageInfo);
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
            default -> {
                // Assume this is a single stage job if no query associated
                JobProfile profile = new SingleStageJobProfile(stageInfo, stageType, jobClass, jobGroupId);
                stageIdToJobProfile.putIfAbsent(stageId, profile);
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
        String jobGroup = sqlEvent.sparkPlanInfo().properties().getProperty(JOB_GROUP_PROPERTY, DEFAULT_JOB_GROUP);
        long jobGroupId = jobGroupToJobGroupId.computeIfAbsent(jobGroup, group -> nextJobGroupId.getAndIncrement());

        SqlJobProfile jobProfile = new SqlJobProfile(sqlEvent, jobGroupId);
        for(int sqlId : jobProfile.getSqlNodeIds()) {
            sqlIdToJobProfile.put(sqlId, jobProfile);
        }
        executionIdToJobProfile.put(jobProfile.getExecutionId(), jobProfile);
        System.out.println("###### INFO: job update: " + jobProfile.getJobId() + " sqlIds: " + jobProfile.getSqlNodeIds().toString() );
    }

    public void handleSparkListenerSQLAdaptiveExecutionUpdate(SparkListenerSQLAdaptiveExecutionUpdate sqlEvent) {
        SqlJobProfile jobProfile = executionIdToJobProfile.get(sqlEvent.executionId());
        if(jobProfile == null) {
            System.out.println("############ ERROR: Spark plan updated before being registered, this makes no sense honestly");
            return;
        }
        jobProfile.updatePlan(sqlEvent);

        for(int sqlId : jobProfile.getSqlNodeIds()) {
            sqlIdToJobProfile.put(sqlId, jobProfile);
        }
        System.out.println("###### INFO: job update: " + jobProfile.getJobId() + " sqlIds: " + jobProfile.getSqlNodeIds().toString() );
    }

    public void handleSparkListenerSQLExecutionEnd(SparkListenerSQLExecutionEnd sqlEvent) {
        SqlJobProfile jobProfile = executionIdToJobProfile.get(sqlEvent.executionId());
        jobProfile.updateJobCompletion();
    }


    private SqlJobProfile getAndUpdateJobProfile(String jobClass, String executionIdStr, StageInfo stageInfo) {
        // extract executionId as integer
        long executionId = 0;
        try {
            executionId = Long.parseLong(executionIdStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("ERROR: Execution id is not a number: " + executionIdStr);
        }
        SqlJobProfile jobProfile = executionIdToJobProfile.getOrDefault(executionId, null);
        // check if the execution plan is registered
        if(jobProfile == null) {
            System.out.println("####### ERROR: no job profile found for executionId: " + executionId);
            return null;
        }

        // Add class to jobs
        jobProfile.setJobClass(jobClass);
        // update job profile with stage info
        jobProfile.updateStageNodes(stageInfo);

        return jobProfile;
    }


}
