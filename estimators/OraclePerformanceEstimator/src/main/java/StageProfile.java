public class StageProfile {
    String jobSectionId;
    int stageId;
    long estimate;

    StageProfile(String jobSecId, int stageId, long estimate) {
        this.jobSectionId = jobSecId;
        this.stageId = stageId;
        this.estimate = estimate;
    }

    long getEstimate() {
        return estimate;
    }

}