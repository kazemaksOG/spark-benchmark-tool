package HistoricPerformanceEstimator.Jobs.SQL;

import org.apache.spark.sql.execution.SparkPlanInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SQLNodeProfile {
    private static final Set<String> BOUNDARY_POINTS = new TreeSet<>(Set.of("Exchange"));
    int sqlId;
    int stageNodeId;
    String sqlName;
    long estimatedRuntime;
    SparkPlanInfo planInfo;
    List<SQLNodeProfile> children;

    SQLNodeProfile(SparkPlanInfo info) {
        sqlId = info.nodeId();
        sqlName = info.nodeName();
        planInfo = info;

        children = new LinkedList<>();

    }

    boolean isBoundaryPoint() {
        return BOUNDARY_POINTS.contains(sqlName);
    }

    void addChild(SQLNodeProfile child) {
        children.add(child);
    }
    List<SQLNodeProfile> getChildren() {
        return children;
    }
}