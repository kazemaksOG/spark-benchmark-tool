package HistoricPerformanceEstimator.Jobs;

import java.util.Optional;


public abstract class JobProfile {
    public static final long DEFAULT_STAGE_RUNTIME = 1000L;
    public static final String DEFAULT_JOB_CLASS = "UNCLASSIFIED";

    private long jobId;
    private long jobGroupId;
    private String jobClass = DEFAULT_JOB_CLASS;
    private Optional<Long> realRuntime = Optional.empty();
    private Optional<Long> inputSize = Optional.empty();
    private long estimatedRuntime = DEFAULT_STAGE_RUNTIME;
    public JobProfile(long jobId, long jobGroupId) {
        this.jobId = jobId;
        this.jobGroupId = jobGroupId;
    }

    public long getRuntime() {
        return realRuntime.orElse(estimatedRuntime);
    }
    public long getJobId() {
        return jobId;
    }
    public long getJobGroupId() {
        return jobGroupId;
    }
    public String getJobClass() {
        return jobClass;
    }
    public boolean isFinished() {
        return realRuntime.isPresent();
    }


    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public void setRealRuntime(long realRuntime) {
        this.realRuntime = Optional.of(realRuntime);
    }

    public void setInputSize(long inputSize) {
        this.inputSize = Optional.of(inputSize);
    }

    public void updateEstimatedRuntime(long estimatedRuntime) {
        this.estimatedRuntime = estimatedRuntime;
    }

    abstract public void updateStageCompletion(int stageId);
    abstract public void updateJobCompletion();

    @Override
    public String toString() {
        return "JobProfile{" +
                "jobId=" + jobId +
                ", jobClass='" + jobClass + '\'' +
                ", realRuntime=" + realRuntime +
                ", inputSize=" + inputSize +
                ", estimatedRuntime=" + estimatedRuntime +
                '}';
    }
}
