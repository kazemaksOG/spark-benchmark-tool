import org.apache.spark.scheduler.FairSchedulingAlgorithm;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulingAlgorithm;

import java.util.Objects;
import java.util.Random;


public class RandomSchedulingAlgorithm implements SchedulingAlgorithm {

    @Override
    public boolean comparator(Schedulable s1, Schedulable s2) {
        int s1Hash = Objects.hash(s1.name(), s1.minShare(), s1.stageId(), s1.runningTasks(), s1.priority());
        int s2Hash = Objects.hash(s2.name(), s2.minShare(), s2.stageId(), s2.runningTasks(), s2.priority());
        return s1Hash < s2Hash;
    }
}
