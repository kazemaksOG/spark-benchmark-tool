import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulerContainer;
import org.apache.spark.scheduler.SchedulingAlgorithm;

public class RandomSchedulerContainer implements SchedulerContainer {


    @Override
    public SchedulableBuilder getScheduler(Pool rootPool) {
        return new RandomScheduler(rootPool);
    }

    @Override
    public SchedulingAlgorithm getAlgorithm() {
        return new RandomSchedulingAlgorithm();
    }
}
