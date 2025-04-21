package OraclePerformanceEstimator.Util;

import OraclePerformanceEstimator.JobProfileContainer;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StageTypeClassifier {
    private static final String PARALLELIZE_SCOPE = "parallelize";

    public enum Type {
        PARALLELIZE, SQL, UNKNOWN
    }

    public static StageTypeClassifier.Type getStageType(SparkListenerStageSubmitted submitted) {
        StageTypeClassifier.Type type = StageTypeClassifier.Type.UNKNOWN;

        // Check if there is a query associated with this
        if(submitted.properties().containsKey(JobProfileContainer.ROOT_EXECUTION_ID)) {
            type = StageTypeClassifier.Type.SQL;
        }

        // determine type based on RDDs
        List<RDDInfo> rddInfos =  JavaConverters.seqAsJavaList(submitted.stageInfo().rddInfos());
        for (var rdd : rddInfos) {
            // check if scope is defined
            if(rdd.scope().isDefined()) {
                String name = rdd.scope().get().name();
                if (name.contains(PARALLELIZE_SCOPE)) {
                    type = StageTypeClassifier.Type.PARALLELIZE; break;
                }
            }
        }

        return type;
    }

    public static List<Integer> getWholeStageIds(StageInfo stageInfo) {
        ArrayList<Integer> ids = new ArrayList<>();

        Seq<RDDInfo> scalaSeq = stageInfo.rddInfos(); // Scala Seq<RDDInfo>
        List<RDDInfo> javaList = JavaConverters.seqAsJavaList(scalaSeq);
        for (RDDInfo info : javaList) {
            if(!info.scope().isEmpty()) {
                Integer id = extractCodegenId(info.scope().get().name());
                if(id != null) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    public static int getWholeStageId(SparkPlanInfo planInfo) {
        Integer id = extractCodegenId(planInfo.nodeName());
        return id == null ? -1 : id;
    }


    public static Integer extractCodegenId(String input) {
        Pattern pattern = Pattern.compile("WholeStageCodegen \\((\\d+)\\)");
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return null;
    }


}
