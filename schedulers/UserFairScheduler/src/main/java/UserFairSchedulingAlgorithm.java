import org.apache.spark.scheduler.FairSchedulingAlgorithm;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulingAlgorithm;

public class UserFairSchedulingAlgorithm implements SchedulingAlgorithm {
    FairSchedulingAlgorithm fairSchedulingAlgorithm = new FairSchedulingAlgorithm();
    @Override
    public boolean comparator(Schedulable s1, Schedulable s2) {
        return fairSchedulingAlgorithm.comparator(s1, s2);
    }
}
