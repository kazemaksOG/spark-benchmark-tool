package OraclePerformanceEstimator.Jobs.SQL;

import OraclePerformanceEstimator.JobProfileContainer;
import OraclePerformanceEstimator.Jobs.JobProfile;
import OraclePerformanceEstimator.Util.StageTypeClassifier;
import org.apache.spark.scheduler.JobRuntime;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static OraclePerformanceEstimator.JobProfileContainer.JOB_CLASS_PROPERTY;

public class SqlJobProfile extends JobProfile {
    private Map<Integer, StageNode> stageIdToStageNode;
    private Map<Integer, StageNode> wholeStageIdToStageNode;
    private Map<Integer, StageNode> sqlIdToStageNode;
    private List<Integer> sqlNodeIds;


    public SqlJobProfile(SparkListenerSQLExecutionStart event, long jobGroupId) {
        super(event.executionId(), jobGroupId);

        // get jobclass from properties
        Properties prop = event.sparkPlanInfo().properties();
        String jobClass = prop.getProperty(JOB_CLASS_PROPERTY);
        if (jobClass != null) {
            super.setJobClass(jobClass);
        }


        stageIdToStageNode = new ConcurrentHashMap<>();
        wholeStageIdToStageNode = new ConcurrentHashMap<>();
        sqlIdToStageNode = new ConcurrentHashMap<>();
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

    // For Oracle Jobs
    public SqlJobProfile(long jobId, String jobClass, long realRuntime) {
        super(jobId, JobRuntime.JOB_INVALID_ID());
        super.setJobClass(jobClass);
        super.setRealRuntime(realRuntime);

        stageIdToStageNode = new ConcurrentHashMap<>();
        wholeStageIdToStageNode = new ConcurrentHashMap<>();
        sqlIdToStageNode = new ConcurrentHashMap<>();


        TreeSet<Integer> wholeStageCodegenIds = new TreeSet<>();
        wholeStageCodegenIds.add(1);
        wholeStageCodegenIds.add(2);
        wholeStageCodegenIds.add(3);
        wholeStageCodegenIds.add(4);
        wholeStageCodegenIds.add(5);
        StageNode stageNode = new StageNode(realRuntime, wholeStageCodegenIds);
        wholeStageIdToStageNode.put(1, stageNode);
        wholeStageIdToStageNode.put(2, stageNode);
        wholeStageIdToStageNode.put(3, stageNode);
        wholeStageIdToStageNode.put(4, stageNode);
        wholeStageIdToStageNode.put(5, stageNode);

    }


    public StageNode getStageNode(int stageId) {
        return stageIdToStageNode.get(stageId);
    }

    public long getExecutionId() {
        return this.getJobId();
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


    synchronized public void updateStageNodes(StageInfo stageInfo) {
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
                System.out.println("##### INFO: registering info for stage id " + stageInfo.stageId() + "to jobId: " + this.getJobId() + " in: " + wholeStageIds);
            }
        }
    }

    synchronized public void updateStageCompletion(int stageId) {
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

    synchronized public void updatePlan(SparkListenerSQLAdaptiveExecutionUpdate sqlEvent) {

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
            if(info.isPresent()) {
                System.out.println("##### INFO: registering info for stage id " + stageId + "to jobId: " + this.getJobId() + " in: " + newStageNode.wholeStageCodegenIds.toString());
            }
            info.ifPresent(newStageNode::registerStageInfo);
            stageIdToStageNode.put(stageId, newStageNode);
            if(oldStageNode.isCompleted()) {
                newStageNode.updateStageNodeMetrics(stageId);
            }
        }
    }

    synchronized public void updateJobCompletion() {
        long runtime = 0L;
        for(StageNode stageNode : stageIdToStageNode.values()) {
            runtime += stageNode.getRuntime();
        }
        super.setRealRuntime(runtime);
    }



}
