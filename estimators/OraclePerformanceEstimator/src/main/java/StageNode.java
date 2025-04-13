import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.SparkPlanInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

public class StageNode {
    Optional<Long> realRuntime = Optional.empty();
    Optional<Long> inputSize = Optional.empty();
    Optional<StageInfo> stageInfo = Optional.empty();

    TreeSet<Integer> wholeStageCodegenIds = new TreeSet<>();
    List<SQLNodeProfile> sqlNodes;
    long estimatedRuntime = JobProfileContainer.DEFAULT_STAGE_RUNTIME;
    StageNode(SQLNodeProfile sqlNodeProfile) {
        this.sqlNodes = new LinkedList<>();
        sqlNodes.add(sqlNodeProfile);

        addWholeStageCodegenId(sqlNodeProfile.planInfo);
    }

    private void addWholeStageCodegenId(SparkPlanInfo planInfo) {
        int wholeStageId = StageTypeClassifier.getWholeStageId(planInfo);
        if (wholeStageId != -1) {
            wholeStageCodegenIds.add(wholeStageId);
        }
    }

    public void addNode(SQLNodeProfile currentNode) {
        sqlNodes.add(currentNode);
        addWholeStageCodegenId(currentNode.planInfo);
    }

    public TreeSet<Integer> getStageNodeIds() {
        return wholeStageCodegenIds;
    }
    public boolean isCompleted() {
        return realRuntime.isPresent();
    }


    public Optional<StageInfo> getStageInfo() {
        return stageInfo;
    }

    public long getRuntime() {
        return realRuntime.orElse(estimatedRuntime);
    }

    public void updateEstimatedRuntime(long estimatedRuntime) {
        this.estimatedRuntime = estimatedRuntime;
    }

    /**
     *
     * @param totalSize
     * @return The ratio between given size and input size
     */
    public double getTotalToInputRatio(long totalSize) {
        if (totalSize > 0) {
            return this.inputSize.map(size -> totalSize / (double)size).orElse(1.0);
        } else {
            return 1.0;
        }
    }

    public void registerStageInfo(StageInfo stageInfo) {
        this.stageInfo = Optional.of(stageInfo);
    }

    public void updateStageNodeMetrics(int stageId) {
        if (stageInfo.isEmpty()) {
            System.out.println("######### ERROR: stageInfo is empty after stage has been completed: " + stageId);
            return;
        }

         if(stageInfo.get().stageId() != stageId) {
             System.out.println("######### ERROR: stageInfo stage Id " + stageInfo.get().stageId() + " does not match " + stageId);
             return;
         }

         TaskMetrics stageMetrics = stageInfo.get().taskMetrics();

         this.realRuntime = Optional.of(stageMetrics.executorRunTime());
         this.inputSize = Optional.of(stageMetrics.inputMetrics().bytesRead());

    }
}
