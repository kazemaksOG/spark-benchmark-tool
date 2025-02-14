import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulingMode;

import java.util.HashMap;
import java.util.Properties;

public class ShortestFirstScheduler implements SchedulableBuilder {
    Pool rootPool;
    HashMap<String, Integer> jobWeight = new HashMap<>();

    ShortestFirstScheduler(Pool rootPool) {
        jobWeight.put("jobs.implementations.LongOperation", 10);
        jobWeight.put("jobs.implementations.ShortOperation", 3);
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
        String jobClass = properties.getProperty("job.class");
        Schedulable jobPool = this.rootPool.getSchedulableByName(jobClass);
        if (jobPool == null) {
            int jobPriority = jobWeight.get(jobClass);
            jobPool = new Pool(jobClass, SchedulingMode.FIFO(), 0, jobPriority, null);
            this.rootPool.addSchedulable(jobPool);
        }
        jobPool.addSchedulable(manager);

    }
}
