package HistoricPerformanceEstimator.Jobs.SingleStage;

import HistoricPerformanceEstimator.Jobs.JobProfile;
import HistoricPerformanceEstimator.Util.StageTypeClassifier;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.StageInfo;


public class SingleStageJobProfile extends JobProfile {
    // we set it slightly higher, so that future jobs can take it and schedule it appropriately
    private static final long DEFAULT_PARALLELIZE_TIME = 5000L;
    private StageInfo stageInfo;

    public SingleStageJobProfile(StageInfo stageInfo, StageTypeClassifier.Type stageType, String jobClass, long jobGroupId) {
        // Single stage profiles do not have job they associate with, hence do not have a global identifier
        super(JobRuntime.JOB_INVALID_ID(), jobGroupId);
        super.setJobClass(jobClass);

        this.stageInfo = stageInfo;

        switch(stageType) {
            case PARALLELIZE -> {
                String parallelizeJobClass = stageType.toString();
                super.setJobClass(parallelizeJobClass);
                super.updateEstimatedRuntime(DEFAULT_PARALLELIZE_TIME);


                System.out.println("####### adding parallelize job with stageId: " + stageInfo.stageId() + " type: " + parallelizeJobClass);
            }

            case UNKNOWN -> {
                super.updateEstimatedRuntime(DEFAULT_STAGE_RUNTIME);
                System.out.println("####### ERROR: no sql profile found for stageId: " + stageInfo.stageId());
            }

            case SQL -> {
                super.updateEstimatedRuntime(DEFAULT_STAGE_RUNTIME);
                System.out.println("####### ERROR: SQL in single stage job, should not happen ever! id: " + stageInfo.stageId());
            }
        }

    }


    @Override
    public void updateStageCompletion(int stageId) {
        // Only update if not already completed
        if(this.isFinished()) {
            System.out.println("####### ERROR: job profile is finished:" + stageInfo.stageId());
            return;
        }

        if(stageInfo.stageId() != stageId) {
            System.out.println("######### ERROR: stageInfo stage Id " + stageInfo.stageId() + " does not match " + stageId);
            return;
        }

        TaskMetrics stageMetrics = stageInfo.taskMetrics();

        System.out.println("####### INFO: stage completed:" + stageInfo.stageId() + " task runtime metrics: " + stageMetrics.executorRunTime());

        this.setRealRuntime(stageMetrics.executorRunTime());
        this.setInputSize(stageMetrics.inputMetrics().bytesRead());
    }

    @Override
    public void updateJobCompletion() {
        this.updateStageCompletion(stageInfo.stageId());
    }
}
