import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulerContainer;
import org.apache.spark.scheduler.SchedulingAlgorithm;

public class TrueFifoSchedulerContainer implements SchedulerContainer {


    @Override
    public SchedulableBuilder getScheduler(Pool rootPool, SparkContext sc) {
        return new TrueFifoScheduler(rootPool, sc);
    }

    @Override
    public SchedulingAlgorithm getAlgorithm() {
        return new TrueFifoSchedulerAlgorithm();
    }
}
