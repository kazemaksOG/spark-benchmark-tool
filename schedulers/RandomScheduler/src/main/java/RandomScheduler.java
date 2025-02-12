import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulingMode;

import java.util.Properties;

public class RandomScheduler implements SchedulableBuilder {
    Pool rootPool;
    RandomScheduler(Pool rootPool) {
        this.rootPool = rootPool;
    }

    @Override
    public Pool rootPool() {
        return rootPool;
    }

    @Override
    public void buildPools() {

    }

    @Override
    public void addTaskSetManager(Schedulable manager, Properties properties) {
        rootPool.addSchedulable(manager);
    }
}
