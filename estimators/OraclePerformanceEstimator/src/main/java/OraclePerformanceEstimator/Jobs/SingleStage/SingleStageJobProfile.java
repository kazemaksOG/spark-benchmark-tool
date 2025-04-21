package OraclePerformanceEstimator.Jobs.SingleStage;

import OraclePerformanceEstimator.Jobs.JobProfile;
import OraclePerformanceEstimator.Jobs.SQL.StageNode;
import OraclePerformanceEstimator.Util.StageTypeClassifier;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.StageInfo;

import java.util.Optional;

import static OraclePerformanceEstimator.Util.StageTypeClassifier.Type.PARALLELIZE;

public class SingleStageJobProfile extends JobProfile {
    private static final long DEFAULT_PARALLELIZE_TIME = 200L;
    private StageInfo stageInfo;

    public SingleStageJobProfile(StageInfo stageInfo, StageTypeClassifier.Type stageType, String jobClass) {
        // Single stage profiles do not have job they associate with, hence do not have a global identifier
        super(JobRuntime.JOB_INVALID_ID());
        super.setJobClass(jobClass);

        this.stageInfo = stageInfo;

        switch(stageType) {
            case PARALLELIZE -> {
                String parallelizeJobClass = stageType.toString();
                super.setJobClass(parallelizeJobClass);
                super.setRealRuntime(DEFAULT_PARALLELIZE_TIME);


                System.out.println("####### adding parallelize job with stageId: " + stageInfo.stageId() + " type: " + parallelizeJobClass);
            }

            case UNKNOWN -> {
                super.setRealRuntime(DEFAULT_STAGE_RUNTIME);
                System.out.println("####### ERROR: no sql profile found for stageId: " + stageInfo.stageId());
            }

            case SQL -> {
                super.setRealRuntime(DEFAULT_STAGE_RUNTIME);
                System.out.println("####### ERROR: SQL in single stage job, should not happen ever! id: " + stageInfo.stageId());
            }
        }

    }


    @Override
    public void updateStageCompletion(int stageId) {
        // Only update if not already completed
        if(this.isFinished()) {
            return;
        }

        if(stageInfo.stageId() != stageId) {
            System.out.println("######### ERROR: stageInfo stage Id " + stageInfo.stageId() + " does not match " + stageId);
            return;
        }

        TaskMetrics stageMetrics = stageInfo.taskMetrics();

        this.setRealRuntime(stageMetrics.executorRunTime());
        this.setInputSize(stageMetrics.inputMetrics().bytesRead());
    }

    @Override
    public void updateJobCompletion() {
        this.updateStageCompletion(stageInfo.stageId());
    }
}
