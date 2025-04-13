import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JobProfile {
    private Map<Integer, StageNode> stageIdToStageNode;
    private Map<Integer, StageNode> wholeStageIdToStageNode;
    private Map<Integer, StageNode> sqlIdToStageNode;
    private List<Integer> sqlNodeIds;

    private long executionId;
    private String jobClass = "UNKNOWN";
    private long estimatedRuntime = JobProfileContainer.DEFAULT_STAGE_RUNTIME;
    private Optional<Long> realRuntime = Optional.empty();

    JobProfile(SparkListenerSQLExecutionStart event) {
        stageIdToStageNode = new ConcurrentHashMap<>();
        wholeStageIdToStageNode = new ConcurrentHashMap<>();
        sqlIdToStageNode = new ConcurrentHashMap<>();

        executionId = event.executionId();

        createStageTree(event.sparkPlanInfo());
    }


    private void createStageTree(SparkPlanInfo planInfo) {
        // convert plan tree into a sqlTree, easier to iterate over it
        SQLNodeProfile rootNode = new SQLNodeProfile(planInfo);
        sqlNodeIds = new LinkedList<>();

        Stack<SQLNodeProfile> stack = new Stack<>();
        stack.push(rootNode);

        while(!stack.empty()) {
            SQLNodeProfile currentStage = stack.pop();
            sqlNodeIds.add(currentStage.sqlId);

            // convert and iterate over all children
            Seq<SparkPlanInfo> scalaSeq = currentStage.planInfo.children();
            List<SparkPlanInfo> javaList = JavaConverters.seqAsJavaList(scalaSeq);
            for(SparkPlanInfo child : javaList) {
                SQLNodeProfile childStage = new SQLNodeProfile(child);
                currentStage.addChild(childStage);
                stack.push(childStage);
            }
        }


        // Create a stageNode tree, which represents how stages convert SQL nodes into execution stages
        Stack<SQLNodeProfile> withinCurrentStage = new Stack<>();
        // The stages should be seperated at boundary points
        Stack<SQLNodeProfile> boundaryNodes = new Stack<>();
        boundaryNodes.push(rootNode);
        while(!boundaryNodes.empty()) {
            // create a new stage node on the boundary
            SQLNodeProfile currentSqlNode = boundaryNodes.pop();
            StageNode currentStageNode = new StageNode(currentSqlNode);

            // see if children are within current stage or boundary nodes
            for(SQLNodeProfile child : currentSqlNode.getChildren()) {
                if(child.isBoundaryPoint()) {
                    boundaryNodes.push(child);
                } else {
                    withinCurrentStage.push(child);
                }
            }

            // go over all children in current stage and add them
            while(!withinCurrentStage.empty()) {
                currentSqlNode = withinCurrentStage.pop();
                currentStageNode.addNode(currentSqlNode);
                // map sqlId to this stage node
                sqlIdToStageNode.put(currentSqlNode.sqlId, currentStageNode);
                for(SQLNodeProfile child : currentSqlNode.getChildren()) {
                    if(child.isBoundaryPoint()) {
                        boundaryNodes.push(child);
                    } else {
                        withinCurrentStage.push(child);
                    }
                }
            }
            // map all wholeStageCodegen ids to the respective stage node
            for(int wholeStageId : currentStageNode.getStageNodeIds()) {
                wholeStageIdToStageNode.put(wholeStageId, currentStageNode);
            }
        }
    }

    // For Oracle lookups
    JobProfile(String jobClass, long realRuntime) {
        stageIdToStageNode = new ConcurrentHashMap<>();
        wholeStageIdToStageNode = new ConcurrentHashMap<>();
        sqlIdToStageNode = new ConcurrentHashMap<>();

        this.executionId = -1;

        this.jobClass = jobClass;
        this.realRuntime = Optional.of(realRuntime);
    }



    public String getJobClass() {
        return jobClass;
    }
    public long getExecutionId() {
        return executionId;
    }

    public long getRuntime() {
        return realRuntime.orElse(estimatedRuntime);
    }

    public boolean isFinished() {
        return realRuntime.isPresent();
    }

    public void updateEstimatedRuntime(long estimatedRuntime) {
        this.estimatedRuntime = estimatedRuntime;
    }

    public StageNode getStageNode(int stageId) {
        return stageIdToStageNode.get(stageId);
    }

    public StageNode getStageNode(Set<Integer> stageNodeIds) {
        for(Integer stageNodeId : stageNodeIds) {
            StageNode stageNode = wholeStageIdToStageNode.get(stageNodeId);
            if(stageNode != null) {
                return stageNode;
            }
        }
        return null;
    }

    public StageNode getStageNodeWithSqlId(int executionId) {
        return sqlIdToStageNode.get(executionId);
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public void updateStageNodes(StageInfo stageInfo) {
        List<Integer> wholeStageIds = StageTypeClassifier.getWholeStageIds(stageInfo);
        if (wholeStageIds.isEmpty()) {
            System.out.println("#### ERROR: stage does not belong to a wholestagecodegen ");
            return;
        }
        for(Integer wholeStageId : wholeStageIds) {
            StageNode stageNode = wholeStageIdToStageNode.get(wholeStageId);
            if(stageNode != null) {
                stageNode.registerStageInfo(stageInfo);
                stageIdToStageNode.put(stageInfo.stageId(), stageNode);
            }
        }
    }

    public void updateStageCompletion(int stageId) {
        StageNode stageNode = stageIdToStageNode.get(stageId);
        if (stageNode != null) {
            stageNode.updateStageNodeMetrics(stageId);
        } else {
            System.out.println("########## ERROR: no stageNode found for stage id " + stageId);
        }

    }

    public List<Integer> getSqlNodeIds() {
        return sqlNodeIds;
    }

    public void updatePlan(SparkListenerSQLAdaptiveExecutionUpdate sqlEvent) {

        wholeStageIdToStageNode = new ConcurrentHashMap<>();
        sqlIdToStageNode = new ConcurrentHashMap<>();

        createStageTree(sqlEvent.sparkPlanInfo());

        // update stagesId to point to correct nodes
        Map<Integer, StageNode> previousStageIdToStageNode = stageIdToStageNode;
        stageIdToStageNode = new ConcurrentHashMap<>();
        for(Map.Entry<Integer, StageNode> entry : previousStageIdToStageNode.entrySet()) {
            int stageId = entry.getKey();
            StageNode oldStageNode = entry.getValue();
            Optional<StageInfo> info = oldStageNode.getStageInfo();
            StageNode newStageNode = null;
            for(int wholeStageId : oldStageNode.getStageNodeIds()) {
                StageNode check = wholeStageIdToStageNode.get(wholeStageId);
                if(check != null) {
                    newStageNode = check;
                    break;
                }
            }

            if(newStageNode == null) {
                System.out.println("#### ERROR: stage does not belong to a stagecodegen " + stageId + "wholestage ids" + oldStageNode.getStageNodeIds().toString());
                continue;
            }

            // update the stage
            info.ifPresent(newStageNode::registerStageInfo);
            stageIdToStageNode.put(stageId, newStageNode);
            if(oldStageNode.isCompleted()) {
                newStageNode.updateStageNodeMetrics(stageId);
            }
        }
    }

    public void updateJobCompletion() {
        long runtime = 0L;
        for(StageNode stageNode : stageIdToStageNode.values()) {
            runtime += stageNode.getRuntime();
        }
        this.realRuntime = Optional.of(runtime);
    }


}
