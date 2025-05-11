import org.apache.spark.scheduler.Pool;
import org.apache.spark.scheduler.Schedulable;
import org.apache.spark.scheduler.SchedulableBuilder;
import org.apache.spark.scheduler.SchedulingMode;

import java.util.Properties;

public class UserFairScheduler implements SchedulableBuilder {
    Pool rootPool;
    UserFairScheduler(Pool rootPool) {
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
        String user = properties.getProperty("user.name");
        // If user not set, set it to default
        if (user == null) {
            user = "DEFAULT";
        }

        Schedulable userPool = rootPool.getSchedulableByName(user);
        if (userPool == null) {
            userPool = new Pool(user, SchedulingMode.FAIR(), 0, 1, null);
            rootPool.addSchedulable(userPool);
        }
        userPool.addSchedulable(manager);
    }
}
