import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulingMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.spark.sql.functions.sum;

public class ShortestFirstScheduler implements SchedulableBuilder {
    Pool rootPool;
    HashMap<String, Integer> jobWeight = new HashMap<>();

    ShortestFirstScheduler(Pool rootPool) {
        jobWeight.put("jobs.implementations.LongOperation", 10);
        jobWeight.put("jobs.implementations.ShortOperation", 3);
        jobWeight.put("jobs.implementations.SuperShortOperation", 3);
        jobWeight.put("DefaultJob", 1);
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
        if (jobClass == null) {
            jobClass = "DefaultJob";
        }
        Schedulable jobPool = this.rootPool.getSchedulableByName(jobClass);
        if (jobPool == null) {
            int jobPriority = jobWeight.getOrDefault(jobClass, 1);
            jobPool = new Pool(jobClass, SchedulingMode.FIFO(), 0, jobPriority, null);
            this.rootPool.addSchedulable(jobPool);
        }
        jobPool.addSchedulable(manager);

    }
}
