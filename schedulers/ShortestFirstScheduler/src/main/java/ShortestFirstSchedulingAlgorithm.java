import org.apache.spark.scheduler.FIFOSchedulingAlgorithm;
import org.apache.spark.scheduler.FairSchedulingAlgorithm;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulingAlgorithm;

public class ShortestFirstSchedulingAlgorithm implements SchedulingAlgorithm {
    FIFOSchedulingAlgorithm fifoSchedulingAlgorithm = new FIFOSchedulingAlgorithm();
    @Override
    public boolean comparator(Schedulable s1, Schedulable s2) {
        return fifoSchedulingAlgorithm.comparator(s1, s2);
    }
}
