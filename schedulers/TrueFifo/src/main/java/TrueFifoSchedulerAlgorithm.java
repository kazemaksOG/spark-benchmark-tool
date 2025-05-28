import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulingAlgorithm;
import org.apache.spark.scheduler.TaskSetManager;


public class TrueFifoSchedulerAlgorithm implements SchedulingAlgorithm {

    @Override
    public boolean comparator(Schedulable s1, Schedulable s2) {
        if(!(s1 instanceof TaskSetManager taskSetManager1)) {
            return s1.name().compareTo(s2.name()) < 0;
        }
        if(!(s2 instanceof TaskSetManager taskSetManager2)) {
            return s1.name().compareTo(s2.name()) < 0;
        }
        // Lowest number will be scheduled first
        return taskSetManager1.priority() < taskSetManager2.priority();
    }
}
