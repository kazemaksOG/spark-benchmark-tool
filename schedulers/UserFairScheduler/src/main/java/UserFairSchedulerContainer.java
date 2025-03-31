import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulerContainer;
import org.apache.spark.scheduler.SchedulingAlgorithm;

public class UserFairSchedulerContainer implements SchedulerContainer {


    @Override
    public SchedulableBuilder getScheduler(Pool rootPool, SparkContext sparkContext) {
        return new UserFairScheduler(rootPool);
    }

    @Override
    public SchedulingAlgorithm getAlgorithm() {
        return new UserFairSchedulingAlgorithm();
    }
}
