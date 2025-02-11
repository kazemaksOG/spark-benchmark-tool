package config;

import jobs.Job;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public class User implements Runnable {
    private static final System.Logger LOGGER = System.getLogger(User.class.getName());

    private String user;
    private ArrayList<Workload> workloads;

    private transient SparkSession spark;

    public void resetBenchStartTime() {
        for(Workload workload : workloads) {
            workload.resetBenchStartTime();
        }
    }

    public void setSpark(SparkSession spark) {
        this.spark = spark;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public ArrayList<Workload> getWorkloadList() {
        return workloads;
    }

    public void setWorkloadList(ArrayList<Workload> workloadList) {
        this.workloads = workloadList;
    }

    public void getResults() {

    }

    @Override
    public String toString() {
        return "config.User{" +
                "user='" + user + '\'' +
                ", workloadList=" + workloads +
                '}';
    }

    @Override
    public void run() {
        if (spark == null) {
            LOGGER.log(ERROR, "No spark session defined for user {}", user);
            return;
        }

        ArrayList<Thread> runningWorkloads = new ArrayList<>();
        spark.sparkContext().setLocalProperty("user.name", user);
        // Run all workloads on parallel
        for (Workload workload : workloads) {
            workload.setSpark(spark);
            Thread workloadThread = new Thread(workload);
            workloadThread.start();
            runningWorkloads.add(workloadThread);
        }

        // wait for all workloads to finish
        for (Thread thread : runningWorkloads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        spark.sparkContext().setLocalProperty("user.name", null);

    }
}

